package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/glebarez/go-sqlite"
	"github.com/sashabaranov/go-openai"
)

// --- Models (structs used only for mapping) ---
type ConversationSummary struct {
	ID         uint
	Username   string
	ThreadPath string
	Start      int64
	End        int64
	Summary    string
}

type ConversationTimelineContext struct {
	ID         uint
	Prompt     string
	Username   string
	ThreadPath string
	Start      int64
	End        int64
	ChunkIDs   string // IDs of the chunk summaries (comma or JSON)
}

// --- Table migration ---
func ensureTimelineTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS conversation_summaries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT,
			thread_path TEXT,
			start INTEGER,
			end INTEGER,
			summary TEXT
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS conversation_timeline_contexts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			prompt TEXT,
			username TEXT,
			thread_path TEXT,
			start INTEGER,
			end INTEGER,
			chunk_ids TEXT
		)
	`)
	return err
}

// --- Helpers ---
func GetUserPosts(db *sql.DB, username string) ([]ForumPost, error) {
	rows, err := db.Query(`SELECT post_id, user, user_num, timestamp, message, thread_path FROM forum_posts WHERE user = ? ORDER BY timestamp ASC`, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var posts []ForumPost
	for rows.Next() {
		var p ForumPost
		if err := rows.Scan(&p.PostID, &p.User, &p.UserNum, &p.Timestamp, &p.Message, &p.ThreadPath); err != nil {
			return nil, err
		}
		posts = append(posts, p)
	}
	return posts, nil
}

func GetThreadPostsBetween(db *sql.DB, threadPath string, start, end int64) ([]ForumPost, error) {
	var rows *sql.Rows
	var err error
	if end > 0 && end < (1<<63-1) {
		rows, err = db.Query(`SELECT post_id, user, user_num, timestamp, message, thread_path FROM forum_posts WHERE thread_path = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp ASC`, threadPath, start, end)
	} else {
		rows, err = db.Query(`SELECT post_id, user, user_num, timestamp, message, thread_path FROM forum_posts WHERE thread_path = ? AND timestamp >= ? ORDER BY timestamp ASC`, threadPath, start)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var posts []ForumPost
	for rows.Next() {
		var p ForumPost
		if err := rows.Scan(&p.PostID, &p.User, &p.UserNum, &p.Timestamp, &p.Message, &p.ThreadPath); err != nil {
			return nil, err
		}
		posts = append(posts, p)
	}
	return posts, nil
}

type Conversation struct {
	ThreadPath string
	Start      int64
	End        int64
	Posts      []ForumPost
}

func FindUserConversations(db *sql.DB, username string) ([]Conversation, error) {
	userPosts, err := GetUserPosts(db, username)
	if err != nil {
		return nil, err
	}
	if len(userPosts) == 0 {
		return nil, nil
	}

	var conversations []Conversation
	for i, post := range userPosts {
		thread := post.ThreadPath
		startTime := post.Timestamp

		// Find end time: time of next user post, *if* in a different thread
		endTime := int64(1<<63 - 1) // max int64
		if i+1 < len(userPosts) && userPosts[i+1].ThreadPath != thread {
			endTime = userPosts[i+1].Timestamp
		} else if i+1 < len(userPosts) && userPosts[i+1].ThreadPath == thread {
			continue // Next post is in the same thread, skip this conversation to avoid duplicate windows
		}

		threadPosts, err := GetThreadPostsBetween(db, thread, startTime, endTime)
		if err != nil {
			return nil, err
		}
		if len(threadPosts) > 0 {
			conversations = append(conversations, Conversation{
				ThreadPath: thread,
				Start:      startTime,
				End:        endTime,
				Posts:      threadPosts,
			})
		}
	}
	return conversations, nil
}

// --- Timeline Function ---
func Timeline(dryRun bool, username string) {
	db, err := sql.Open("sqlite", "data/docs.db")
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	defer db.Close()

	if err := ensureTimelineTables(db); err != nil {
		log.Fatalf("failed to migrate: %v", err)
	}

	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	maxChars := 100000 // safe for GPT-4o, adjust for your model

	convos, err := FindUserConversations(db, username)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d conversations for user %s\n", len(convos), username)

	for i, convo := range convos {
		fmt.Printf("\n--- Conversation %d (thread: %s, from %d to %d, %d posts) ---\n",
			i+1, convo.ThreadPath, convo.Start, convo.End, len(convo.Posts))

		chunks := ChunkPosts(convo.Posts, maxChars)
		var summaries []string
		for j, chunk := range chunks {
			fmt.Printf("Summarizing chunk %d/%d...\n", j+1, len(chunks))
			summary, err := SummarizeChunk(db, client, chunk, dryRun)
			if err != nil {
				log.Printf("Summarization failed: %v", err)
				continue
			}
			summaries = append(summaries, summary)
		}
		summary := strings.Join(summaries, "\n---\n")
		if dryRun {
			res, err := db.Exec(
				`INSERT INTO conversation_timeline_contexts (prompt, username, thread_path, start, end, chunk_ids) VALUES (?, ?, ?, ?, ?, ?)`,
				"You are a skilled fantasy forum summarizer. Your task is to combine multiple summaries into one concise but thorough summary for the entire conversation window.",
				username, convo.ThreadPath, convo.Start, convo.End, strings.Join(summaries, ","),
			)
			if err != nil {
				log.Printf("Failed to save dry run timeline context: %v", err)
				continue
			}
			id, _ := res.LastInsertId()
			fmt.Printf("Dry run timeline context saved with ID %d\n", id)
			fmt.Println("Chunk IDs:", strings.Join(summaries, ","))
			continue
		}
		// Save actual summary to ConversationSummary table
		_, err := db.Exec(
			`INSERT INTO conversation_summaries (username, thread_path, start, end, summary) VALUES (?, ?, ?, ?, ?)`,
			username, convo.ThreadPath, convo.Start, convo.End, summary,
		)
		if err != nil {
			log.Printf("Failed to save summary: %v", err)
		} else {
			fmt.Printf("Saved summary for Conversation %d\n", i+1)
			fmt.Println("Summary:", summary)
		}
	}
}
