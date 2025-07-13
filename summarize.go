package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/sashabaranov/go-openai"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Query all posts in a thread, sorted by timestamp
func GetPostsByThread(db *gorm.DB, threadPath string) ([]ForumPost, error) {
	var posts []ForumPost
	err := db.Where("thread_path = ?", threadPath).
		Order("timestamp asc").Find(&posts).Error
	return posts, err
}

// Split posts into text chunks that fit within an approx token/window limit.
// NOTE: This is a simple version. Consider using a real token estimator!
func ChunkPosts(posts []ForumPost, maxChars int) [][]ForumPost {
	var chunks [][]ForumPost
	var current []ForumPost
	currentLen := 0
	for _, post := range posts {
		msgLen := len(post.Message)
		// (Add 64 chars as a guess for metadata/user)
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

type SummarizationContext struct {
	ID        uint `gorm:"primaryKey;autoIncrement"`
	Prompt    string
	ChunkText string // (optionally, store the formatted chunk)
}

type SummarizedThreadContext struct {
	ID         uint `gorm:"primaryKey;autoIncrement"`
	Prompt     string
	ThreadPath string
	IDs        string // (optionally, store the combined summaries)
}

// Generate a summary for a chunk of posts
func SummarizeChunk(db *gorm.DB, client *openai.Client, posts []ForumPost, dryRun bool) (string, error) {
	var builder strings.Builder
	for _, post := range posts {
		fmt.Fprintf(&builder, "%s:\n%s\n", post.User, post.Message)
	}
	chunkText := builder.String()

	systemPrompt := "You are a skilled fantasy forum summarizer."

	if dryRun {
		fmt.Println("Dry run mode: not sending to OpenAI")

		ctx := SummarizationContext{
			Prompt:    systemPrompt,
			ChunkText: chunkText,
		}

		if err := db.Create(&ctx).Error; err != nil {
			return "", fmt.Errorf("failed to save dry run context: %w", err)
		}
		fmt.Printf("Dry run context saved with ID %d\n", ctx.ID)

		return fmt.Sprintf("%d", ctx.ID), nil
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

// High-level: summarize a whole thread
func SummarizeThread(db *gorm.DB, client *openai.Client, threadPath string, maxChars int, dryRun bool) (string, error) {
	posts, err := GetPostsByThread(db, threadPath)
	if err != nil {
		return "", err
	}
	if len(posts) == 0 {
		return "(No posts in thread)", nil
	}

	// Split into chunks
	chunks := ChunkPosts(posts, maxChars)

	// Summarize each chunk
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

		ctx := SummarizedThreadContext{
			Prompt:     systemPrompt,
			ThreadPath: threadPath,
			IDs:        strings.Join(summaries, ","),
		}

		if err := db.Create(&ctx).Error; err != nil {
			return "", fmt.Errorf("failed to save dry run context: %w", err)
		}
		fmt.Printf("Dry run context saved with ID %d\n", ctx.ID)
		return fmt.Sprintf("%d", ctx.ID), nil
	} else {
		// If only one summary, return it.
		if len(summaries) == 1 {
			return summaries[0], nil
		}
		// Otherwise, combine summaries into a final prompt
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
	// Setup DB (change to postgres if needed)
	db, err := gorm.Open(sqlite.Open("data/docs.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}

	if err := db.AutoMigrate(&ForumPost{}, &SummarizationContext{}, &SummarizedThreadContext{}); err != nil {
		log.Fatalf("failed to migrate: %v", err)
	}

	// OpenAI client (put your API key here)
	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))

	flag.Parse()
	//"overworld/isran-empire/free-plains-isra/isra-free-city/threads/midnight-sun"
	maxChars := 10000000 // Be conservative; adjust for your model/context window
	summary, err := SummarizeThread(db, client, threadPath, maxChars, dryRun)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n=== Thread Summary ===\n%s\n", summary)
}
