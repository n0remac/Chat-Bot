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
	"github.com/qdrant/go-client/qdrant"
	"github.com/sashabaranov/go-openai"
)

// The result sent back by the recall process
type RecallResult struct {
	RecalledPosts []PostToEmbed // The posts the character "remembers"
	Time          int64         // When recall was generated (unix seconds)
}

// Requests to the recall process
type RecallRequest struct {
	ChannelID     string
	CharacterName string
	UserInput     string
	ReplyChan     chan RecallResult
}

var (
	RecallChan = make(chan RecallRequest)
)

// Start the recall process (runs in a goroutine)
func StartRecall() {
	LogToFile("recall.log")
	postDb, err := sql.Open("sqlite", "data/docs.db")
	if err != nil {
		log.Fatalf("failed to open postDb: %v", err)
	}
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host: qdrantHost, // e.g., "localhost"
		Port: qdrantPort, // e.g., 6334
	})
	if err != nil {
		log.Fatalf("failed to open qdrant client: %v", err)
	}
	go recallLoop(postDb, qdrantClient, RecallChan)
}

func recallLoop(postDb *sql.DB, qdrantClient *qdrant.Client, ch <-chan RecallRequest) {
	for req := range ch {
		log.Printf("[recallLoop] Received recall request for channel=%s character=%s", req.ChannelID, req.CharacterName)

		recalled, err := runRecall(postDb, qdrantClient, req.CharacterName, req.UserInput)
		if err != nil {
			log.Printf("[recallLoop] Recall error: %v", err)
			req.ReplyChan <- RecallResult{RecalledPosts: nil, Time: time.Now().Unix()}
		} else {
			req.ReplyChan <- RecallResult{RecalledPosts: recalled, Time: time.Now().Unix()}
		}
	}
}

// The main recall logic: embed user input, search Qdrant for relevant posts for the character
func runRecall(postDb *sql.DB, qdrantClient *qdrant.Client, characterName, userInput string) ([]PostToEmbed, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY not set")
	}
	openaiClient := openai.NewClient(apiKey)

	// Step 1: Embed the user input
	embResp, err := openaiClient.CreateEmbeddings(context.Background(), openai.EmbeddingRequest{
		Input: []string{userInput},
		Model: openai.LargeEmbedding3, // or AdaEmbeddingV2, but must match vectorSize
	})
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	if len(embResp.Data) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}
	queryVec := embResp.Data[0].Embedding

	// Step 2: Query Qdrant for top N relevant posts for this character
	const topK = 5

	queryPoints := &qdrant.QueryPoints{
		CollectionName: collectionName,
		Query:          qdrant.NewQuery(queryVec...),
		Limit:          ptrUint64(uint64(topK)),
		WithPayload:    qdrant.NewWithPayload(true),
		// Optional: Add a filter to only match posts from the character
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("user", characterName),
			},
		},
	}
	result, err := qdrantClient.Query(context.Background(), queryPoints)
	if err != nil {
		return nil, fmt.Errorf("qdrant query error: %w", err)
	}

	var recalled []PostToEmbed
	for _, pt := range result {
		// Map Qdrant payload back to your struct
		payload := pt.Payload
		post := PostToEmbed{
			PostID:    asString(payload["post_id"]),
			User:      asString(payload["user"]),
			Message:   asString(payload["message"]),
			ThreadID:  asString(payload["thread_id"]),
			Timestamp: asInt64(payload["timestamp"]),
		}
		recalled = append(recalled, post)
	}
	return recalled, nil
}

// ptrUint64 returns a pointer to the given uint64 value.
func ptrUint64(v uint64) *uint64 {
	return &v
}

// Helper functions to safely extract fields
func asString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
func asInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		var x int64
		json.Unmarshal([]byte(t), &x)
		return x
	default:
		return 0
	}
}

// Usage: send a recall request and get the response
func RecallRelevantPosts(channelID, characterName, userInput string) []PostToEmbed {
	replyChan := make(chan RecallResult)
	RecallChan <- RecallRequest{
		ChannelID:     channelID,
		CharacterName: characterName,
		UserInput:     userInput,
		ReplyChan:     replyChan,
	}
	result := <-replyChan
	return result.RecalledPosts
}
