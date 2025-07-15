package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/sashabaranov/go-openai"
)

type MemorySummary struct {
	SummaryText string  // The OpenAI-generated summary
	ContextIDs  []int64 // IDs of contexts (messages) that went into this summary
	Time        int64   // When the summary was generated (unix seconds)
}

// Requests to the memory process
type MemoryRequest struct {
	ChannelID     string
	Message       ChatMessage
	ReplyChan     chan MemorySummary
	CharacterName string
}

var (
	// This channel is used for IPC between the Discord bot and memory process (pipe/socket/other process in prod!)
	MemoryChan = make(chan MemoryRequest)
)

func StartMemory() {
	LogToFile("memory.log")
	memoryDb, err := sql.Open("sqlite", "data/memory.db")
	if err != nil {
		log.Fatalf("failed to open memoryDb: %v", err)
	}
	initMemoryDB(memoryDb)

	postDb, err := sql.Open("sqlite", "data/docs.db")
	if err != nil {
		log.Fatalf("failed to open postDb: %v", err)
	}
	go memoryLoop(postDb, memoryDb, MemoryChan)
	// Do NOT block forever or defer memoryDb.Close() here
}

// DB schema setup
func initMemoryDB(memoryDb *sql.DB) {
	_, err := memoryDb.Exec(`
	CREATE TABLE IF NOT EXISTS contexts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		channel_id TEXT,
		author_id TEXT,
		username TEXT,
		content TEXT,
		time INTEGER,
		type TEXT DEFAULT 'message'
	);
	CREATE TABLE IF NOT EXISTS summaries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		channel_id TEXT,
		summary_text TEXT,
		context_ids TEXT,  -- JSON array of int64
		time INTEGER
	);
	`)
	if err != nil {
		log.Fatalf("failed to create tables: %v", err)
	}
}

// Memory loop: receives update/fetch requests and manages DB + OpenAI summarization
func memoryLoop(postDb, memoryDb *sql.DB, ch <-chan MemoryRequest) {
	for req := range ch {
		log.Printf("[memoryLoop] Received memory request for channel=%s character=%s", req.ChannelID, req.CharacterName)

		if req.ReplyChan != nil {
			log.Printf("[memoryLoop] ReplyChan detected: sending latest summary back.")
			summary, err := getLatestSummary(memoryDb, req.ChannelID)
			if err != nil {
				log.Printf("[memoryLoop] getLatestSummary error: %v", err)
				req.ReplyChan <- MemorySummary{}
			} else {
				req.ReplyChan <- summary
			}
			continue
		}

		// Always insert new context
		_, err := memoryDb.Exec(`INSERT INTO contexts (channel_id, author_id, username, content, time, type)
            VALUES (?, ?, ?, ?, ?, ?)`,
			req.ChannelID, req.Message.AuthorID, req.Message.Username, req.Message.Content, req.Message.Time, "message")
		if err != nil {
			log.Printf("[memoryLoop] Failed to insert user message context: %v", err)
		} else {
			log.Printf("[memoryLoop] Inserted user message into context table.")
		}

		if err := updateSummary(memoryDb, req.ChannelID); err != nil {
			log.Printf("[memoryLoop] updateSummary error: %v", err)
			continue
		}
		log.Printf("[memoryLoop] Updated memory summary for channel %s.", req.ChannelID)
	}
}

// Fetch N most recent unsummarized contexts, plus the last summary
func updateSummary(memoryDb *sql.DB, channelID string) error {
	log.Printf("[updateSummary] Attempting summary update for channel: %s", channelID)

	// Fetch the latest summary, if any
	lastSummary, err := getLatestSummary(memoryDb, channelID)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("[updateSummary] Error getting latest summary: %v", err)
		return err
	}

	var lastSeenID int64 = 0
	if len(lastSummary.ContextIDs) > 0 {
		lastSeenID = lastSummary.ContextIDs[len(lastSummary.ContextIDs)-1]
	}
	log.Printf("[updateSummary] Last seen context ID: %d", lastSeenID)

	// ALWAYS get the *most recent* 50 new contexts since lastSeenID
	const maxContexts = 50
	rows, err := memoryDb.Query(`
		SELECT id, author_id, username, content, time
		FROM contexts
		WHERE channel_id = ? AND id > ?
		ORDER BY id ASC
		LIMIT ?
	`, channelID, lastSeenID, maxContexts)
	if err != nil {
		log.Printf("[updateSummary] DB query error: %v", err)
		return err
	}
	defer rows.Close()

	var contexts []ChatMessage
	var contextIDs []int64
	for rows.Next() {
		var id, t int64
		var authorID, username, content string
		if err := rows.Scan(&id, &authorID, &username, &content, &t); err != nil {
			log.Printf("[updateSummary] DB row scan error: %v", err)
			return err
		}
		contextIDs = append(contextIDs, id)
		contexts = append(contexts, ChatMessage{
			AuthorID: authorID, Username: username, Content: content, Time: t,
		})
	}
	if err := rows.Err(); err != nil {
		log.Printf("[updateSummary] DB rows iteration error: %v", err)
		return err
	}
	log.Printf("[updateSummary] Fetched %d new context(s) to summarize.", len(contexts))

	if len(contexts) == 0 {
		log.Printf("[updateSummary] No new contexts to summarize for channel %s.", channelID)
		return nil // Nothing new
	}

	// Construct memory prompt for OpenAI
	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	var prompt string
	if lastSummary.SummaryText != "" {
		prompt = fmt.Sprintf(
			"Previous memory summary for this channel:\n%s\n\n"+
				"New chat context to update the memory:\n%s\n\n"+
				"Summarize these new messages and combine them with the old summary. "+
				"Output an updated memory summary that keeps important facts, events, and character relationships, in a concise and readable way.",
			lastSummary.SummaryText,
			messagesToString(contexts),
		)
	} else {
		prompt = fmt.Sprintf(
			"No prior summary.\nNew chat context to store in memory:\n%s\n\n"+
				"Summarize these messages for memory. Focus on important facts, events, and relationships.",
			messagesToString(contexts),
		)
	}
	log.Printf("[updateSummary] Memory prompt built, sending to OpenAI.")

	resp, err := client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
		Model:     "gpt-4.1-nano-2025-04-14",
		Messages:  []openai.ChatCompletionMessage{{Role: "user", Content: prompt}},
		MaxTokens: 1000,
	})
	if err != nil {
		log.Printf("[updateSummary] OpenAI API error: %v", err)
		return fmt.Errorf("OpenAI summary: %v", err)
	}
	summary := resp.Choices[0].Message.Content
	if summary == "" {
		log.Printf("[updateSummary] WARNING: OpenAI returned an empty summary. Skipping summary save.")
		return fmt.Errorf("OpenAI returned empty summary")
	}
	log.Printf("[updateSummary] Got summary from OpenAI (%d chars).", len(summary))

	// Combine context IDs from previous summary and just-summarized contexts
	combinedIDs := append([]int64(nil), lastSummary.ContextIDs...)
	combinedIDs = append(combinedIDs, contextIDs...)
	contextIDsJSON, err := json.Marshal(combinedIDs)
	if err != nil {
		log.Printf("[updateSummary] Error marshaling context IDs: %v", err)
		return err
	}

	// Insert new summary into the database
	_, err = memoryDb.Exec(`INSERT INTO summaries (channel_id, summary_text, context_ids, time) VALUES (?, ?, ?, ?)`,
		channelID, summary, string(contextIDsJSON), time.Now().Unix())
	if err != nil {
		log.Printf("[updateSummary] DB insert error: %v", err)
		return err
	}
	log.Printf("[updateSummary] Inserted new summary for channel %s with %d context(s).", channelID, len(contextIDs))

	return nil
}

// Fetch most recent summary for channelID
func getLatestSummary(memoryDb *sql.DB, channelID string) (MemorySummary, error) {
	row := memoryDb.QueryRow(`SELECT summary_text, context_ids, time FROM summaries WHERE channel_id = ? ORDER BY id DESC LIMIT 1`, channelID)
	var summaryText string
	var contextIDsJSON string
	var t int64
	err := row.Scan(&summaryText, &contextIDsJSON, &t)
	if err != nil {
		return MemorySummary{}, err
	}
	var contextIDs []int64
	json.Unmarshal([]byte(contextIDsJSON), &contextIDs)
	return MemorySummary{
		SummaryText: summaryText,
		ContextIDs:  contextIDs,
		Time:        t,
	}, nil
}

// Helper: Convert chat messages to readable string for LLM
func messagesToString(msgs []ChatMessage) string {
	s := ""
	for _, m := range msgs {
		tm := time.Unix(m.Time, 0).Format("2006-01-02 15:04")
		s += fmt.Sprintf("[%s] %s: %s\n", tm, m.Username, m.Content)
	}
	return s
}

func LogToFile(filename string) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		// If log file can't be opened, fall back to stderr and warn user.
		log.Printf("WARNING: could not open log file %s for writing: %v", filename, err)
		return
	}
	log.SetOutput(f)
}

func UpdateMemory(channelID, characterName, authorID, username, content string, timestamp int64) {
	MemoryChan <- MemoryRequest{
		ChannelID:     channelID,
		CharacterName: characterName,
		ReplyChan:     nil, // No reply expected for update
		Message: ChatMessage{
			AuthorID: authorID,
			Username: username,
			Content:  content,
			Time:     timestamp,
		},
	}
}

func GetMemorySummary(channelID, characterName string) MemorySummary {
	replyChan := make(chan MemorySummary)
	MemoryChan <- MemoryRequest{
		ChannelID:     channelID,
		CharacterName: characterName,
		ReplyChan:     replyChan, // Request summary
		// Message is ignored for fetch, so can be zero value
	}
	return <-replyChan
}
