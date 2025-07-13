package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/glebarez/sqlite"
	"github.com/sashabaranov/go-openai"
	"gorm.io/gorm"
)

// --- Models ---
type ConversationSummary struct {
	ID         uint `gorm:"primaryKey;autoIncrement"`
	Username   string
	ThreadPath string
	Start      int64
	End        int64
	Summary    string
}

type ConversationTimelineContext struct {
	ID         uint `gorm:"primaryKey;autoIncrement"`
	Prompt     string
	Username   string
	ThreadPath string
	Start      int64
	End        int64
	ChunkIDs   string // IDs of the chunk summaries (comma or JSON)
}

// --- Helpers ---
func GetUserPosts(db *gorm.DB, username string) ([]ForumPost, error) {
	var posts []ForumPost
	err := db.Where("user = ?", username).Order("timestamp asc").Find(&posts).Error
	return posts, err
}

func GetThreadPostsBetween(db *gorm.DB, threadPath string, start, end int64) ([]ForumPost, error) {
	var posts []ForumPost
	q := db.Where("thread_path = ? AND timestamp >= ?", threadPath, start)
	if end > 0 {
		q = q.Where("timestamp < ?", end)
	}
	err := q.Order("timestamp asc").Find(&posts).Error
	return posts, err
}

type Conversation struct {
	ThreadPath string
	Start      int64
	End        int64
	Posts      []ForumPost
}

func FindUserConversations(db *gorm.DB, username string) ([]Conversation, error) {
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
			// Next post is in the same thread, skip this conversation to avoid duplicate windows
			continue
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
	db, err := gorm.Open(sqlite.Open("data/docs.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}

	// AutoMigrate all models
	if err := db.AutoMigrate(&ConversationSummary{}, &SummarizationContext{}, &ConversationTimelineContext{}); err != nil {
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
		// Join chunk summaries, or join chunkIDs in dryRun mode
		summary := strings.Join(summaries, "\n---\n")
		if dryRun {
			ctx := ConversationTimelineContext{
				Prompt:     "You are a skilled fantasy forum summarizer. Your task is to combine multiple summaries into one concise but thorough summary for the entire conversation window.",
				Username:   username,
				ThreadPath: convo.ThreadPath,
				Start:      convo.Start,
				End:        convo.End,
				ChunkIDs:   strings.Join(summaries, ","),
			}
			if err := db.Create(&ctx).Error; err != nil {
				log.Printf("Failed to save dry run timeline context: %v", err)
				continue
			}
			fmt.Printf("Dry run timeline context saved with ID %d\n", ctx.ID)
			fmt.Println("Chunk IDs:", ctx.ChunkIDs)
			continue
		}
		// Save actual summary to ConversationSummary table
		convSum := ConversationSummary{
			Username:   username,
			ThreadPath: convo.ThreadPath,
			Start:      convo.Start,
			End:        convo.End,
			Summary:    summary,
		}
		if err := db.Create(&convSum).Error; err != nil {
			log.Printf("Failed to save summary: %v", err)
		}
		fmt.Printf("Saved summary for Conversation %d\n", i+1)
		fmt.Println("Summary:", summary)
	}
}
