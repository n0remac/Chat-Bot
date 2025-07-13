package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/glebarez/go-sqlite"
	"github.com/sashabaranov/go-openai"
)

type SummarizationContext struct {
	ID        uint
	Prompt    string
	ChunkText string
}

type SummarizedThreadContext struct {
	ID         uint
	Prompt     string
	ThreadPath string
	IDs        string
}

// --- Ensure tables exist ---
func ensureTables(db *sql.DB) error {
	// forum_posts table is assumed to exist already.
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS summarization_contexts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			prompt TEXT,
			chunk_text TEXT
		);
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS summarized_thread_contexts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			prompt TEXT,
			thread_path TEXT,
			ids TEXT
		);
	`)
	return err
}

// --- Query all posts in a thread, sorted by timestamp ---
func GetPostsByThread(db *sql.DB, threadPath string) ([]ForumPost, error) {
	rows, err := db.Query(`SELECT post_id, user, user_num, timestamp, message, thread_path FROM forum_posts WHERE thread_path = ? ORDER BY timestamp ASC`, threadPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var posts []ForumPost
	for rows.Next() {
		var p ForumPost
		err := rows.Scan(&p.PostID, &p.User, &p.UserNum, &p.Timestamp, &p.Message, &p.ThreadPath)
		if err != nil {
			return nil, err
		}
		posts = append(posts, p)
	}
	return posts, nil
}

// --- Split posts into text chunks that fit within a window ---
func ChunkPosts(posts []ForumPost, maxChars int) [][]ForumPost {
	var chunks [][]ForumPost
	var current []ForumPost
	currentLen := 0
	for _, post := range posts {
		msgLen := len(post.Message)
		if currentLen+msgLen+64 > maxChars && len(current) > 0 {
			chunks = append(chunks, current)
			current = nil
			currentLen = 0
		}
		current = append(current, post)
		currentLen += msgLen + 64
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

// --- Generate a summary for a chunk of posts ---
func SummarizeChunk(db *sql.DB, client *openai.Client, posts []ForumPost, dryRun bool) (string, error) {
	var builder strings.Builder
	for _, post := range posts {
		fmt.Fprintf(&builder, "%s:\n%s\n", post.User, post.Message)
	}
	chunkText := builder.String()

	systemPrompt := "You are a skilled fantasy forum summarizer."

	if dryRun {
		fmt.Println("Dry run mode: not sending to OpenAI")
		res, err := db.Exec(`INSERT INTO summarization_contexts (prompt, chunk_text) VALUES (?, ?)`, systemPrompt, chunkText)
		if err != nil {
			return "", fmt.Errorf("failed to save dry run context: %w", err)
		}
		id, _ := res.LastInsertId()
		fmt.Printf("Dry run context saved with ID %d\n", id)
		return fmt.Sprintf("%d", id), nil
	} else {
		prompt := fmt.Sprintf(
			"Summarize the following forum thread section as if you are explaining the key events. Keep the summaries close to the original tone and feel of the original posts.\n\nThread Section:\n%s", chunkText,
		)

		req := openai.ChatCompletionRequest{
			Model: "gpt-4.1-2025-04-14",
			Messages: []openai.ChatCompletionMessage{
				{Role: openai.ChatMessageRoleSystem, Content: systemPrompt},
				{Role: openai.ChatMessageRoleUser, Content: prompt},
			},
		}
		resp, err := client.CreateChatCompletion(context.Background(), req)
		if err != nil {
			return "", err
		}
		return resp.Choices[0].Message.Content, nil
	}
}

// --- Summarize a whole thread ---
func SummarizeThread(db *sql.DB, client *openai.Client, threadPath string, maxChars int, dryRun bool) (string, error) {
	posts, err := GetPostsByThread(db, threadPath)
	if err != nil {
		return "", err
	}
	if len(posts) == 0 {
		return "(No posts in thread)", nil
	}

	chunks := ChunkPosts(posts, maxChars)
	var summaries []string
	for idx, chunk := range chunks {
		fmt.Printf("Summarizing chunk %d/%d for thread: %s\n", idx+1, len(chunks), threadPath)
		summary, err := SummarizeChunk(db, client, chunk, dryRun)
		if err != nil {
			return "", err
		}
		summaries = append(summaries, summary)
	}

	systemPrompt := "You are a skilled fantasy forum summarizer. Your task is to combine multiple summaries into one concise but thorough summary for the entire thread."
	if dryRun {
		fmt.Println("Dry run mode: not sending final summary to OpenAI")
		ids := strings.Join(summaries, ",")
		res, err := db.Exec(`INSERT INTO summarized_thread_contexts (prompt, thread_path, ids) VALUES (?, ?, ?)`, systemPrompt, threadPath, ids)
		if err != nil {
			return "", fmt.Errorf("failed to save dry run context: %w", err)
		}
		id, _ := res.LastInsertId()
		fmt.Printf("Dry run context saved with ID %d\n", id)
		return fmt.Sprintf("%d", id), nil
	} else {
		if len(summaries) == 1 {
			return summaries[0], nil
		}
		finalPrompt := "Combine these thread section summaries into one concise but thorough summary for the entire thread:\n\n"
		for _, s := range summaries {
			finalPrompt += s + "\n"
		}
		req := openai.ChatCompletionRequest{
			Model: "gpt-4.1-2025-04-14",
			Messages: []openai.ChatCompletionMessage{
				{Role: openai.ChatMessageRoleSystem, Content: "You are a skilled fantasy forum summarizer."},
				{Role: openai.ChatMessageRoleUser, Content: finalPrompt},
			},
		}
		resp, err := client.CreateChatCompletion(context.Background(), req)
		if err != nil {
			return "", err
		}
		return resp.Choices[0].Message.Content, nil
	}
}

func Summarize(dryRun bool, threadPath string) {
	db, err := sql.Open("sqlite", "data/docs.db")
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	defer db.Close()

	if err := ensureTables(db); err != nil {
		log.Fatalf("failed to migrate: %v", err)
	}

	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	flag.Parse()
	maxChars := 10000000
	summary, err := SummarizeThread(db, client, threadPath, maxChars, dryRun)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n=== Thread Summary ===\n%s\n", summary)
}
