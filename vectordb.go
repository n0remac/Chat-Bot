package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	// This is the SQLite driver
	_ "github.com/glebarez/go-sqlite"
	"github.com/qdrant/go-client/qdrant"
	"github.com/sashabaranov/go-openai"
)

type PostToEmbed struct {
	PostID    string `json:"post_id"`
	User      string `json:"user"`
	Message   string `json:"message"`
	ThreadID  string `json:"thread_id,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

const (
	// Path to your SQLite database file
	dbPath = "data/docs.db"
	// Qdrant server connection details
	qdrantHost = "localhost"
	qdrantPort = 6334
	// The name for our vector collection in Qdrant
	collectionName = "forum_posts"
	// OpenAI's text-embedding-ada-002 model produces vectors of this size.
	// It's crucial that this matches the model's output.
	vectorSize   = 3072
	maxBatchSize = 500
)

func submitEmbeddingsBatch(client *openai.Client, lines []openai.BatchLineItem) (string, error) {
	ctx := context.Background()

	batchReq := openai.CreateBatchWithUploadFileRequest{
		Endpoint:         openai.BatchEndpointEmbeddings,
		CompletionWindow: "24h",
		UploadBatchFileRequest: openai.UploadBatchFileRequest{
			FileName: "embeddings_batch.jsonl",
			Lines:    lines,
		},
	}
	batchResp, err := client.CreateBatchWithUploadFile(ctx, batchReq)
	if err != nil {
		return "", fmt.Errorf("failed to create batch: %w", err)
	}

	// batchResp.ID is your batch identifier for later polling
	return batchResp.ID, nil
}

func CreateVectorDBForTFS(dryMode bool) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Fatal: failed to open sqlite db at %s: %v", dbPath, err)
	}
	defer db.Close()

	if err := EnsureBatchTable(db); err != nil {
		log.Fatalf("Fatal: Failed to ensure batch_jobs table exists: %v", err)
	}

	posts, err := GetAllForumPosts(db)
	if err != nil {
		log.Fatalf("Fatal: Failed to get posts : %v", err)
	}

	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("Fatal: OPENAI_API_KEY environment variable is not set.")
	}
	openaiClient := openai.NewClient(apiKey)

	// Prepare all messages for embedding
	postsToEmbed := make([]PostToEmbed, len(posts))
	for i, post := range posts {
		postsToEmbed[i] = PostToEmbed{
			PostID:    post.PostID,
			User:      post.User,
			Message:   post.Message,
			ThreadID:  post.ThreadPath,
			Timestamp: post.Timestamp,
		}
	}

	// Batch processing
	numBatches := (len(postsToEmbed) + maxBatchSize - 1) / maxBatchSize
	for batchNum := 0; batchNum < numBatches; batchNum++ {
		start := batchNum * maxBatchSize
		end := start + maxBatchSize
		if end > len(postsToEmbed) {
			end = len(postsToEmbed)
		}
		batch := postsToEmbed[start:end]

		// Step 1: Build batch line items
		lines := make([]openai.BatchLineItem, len(batch))
		for i, post := range batch {
			lines[i] = openai.BatchEmbeddingRequest{
				CustomID: post.PostID,
				Body: openai.EmbeddingRequest{
					Input: post.Message,
					Model: openai.LargeEmbedding3,
				},
				Method: "POST",
				URL:    openai.BatchEndpointEmbeddings,
			}
		}

		if dryMode {
			// Save batch request to file
			fileName := fmt.Sprintf("embedding_batch_%d.json", batchNum+1)
			out, err := json.MarshalIndent(lines, "", "  ")
			if err != nil {
				log.Fatalf("Failed to marshal lines: %v", err)
			}
			err = os.WriteFile(fileName, out, 0644)
			if err != nil {
				log.Fatalf("Failed to write batch file: %v", err)
			}
			log.Printf("Dry mode: Batch %d saved to %s (%d items)", batchNum+1, fileName, len(batch))
		} else {
			// --- SUBMIT BATCH JOB FOR EMBEDDINGS ---
			batchID, err := submitEmbeddingsBatch(openaiClient, lines)
			if err != nil {
				log.Fatalf("Fatal: Failed to submit embedding batch: %v", err)
			}
			if err := SaveBatchID(db, batchID); err != nil {
				log.Printf("Warning: Failed to save batch ID %s: %v", batchID, err)
			}
			log.Println("-----")
			log.Printf("Submitted embedding batch job to OpenAI Batch API for %d posts.\n", len(batch))
			log.Printf("Batch ID: %s\n", batchID)
			log.Println("-----")
		}
	}
	if dryMode {
		log.Println("All batches saved as files. No API calls made.")
	} else {
		log.Println("All batches submitted to OpenAI Batch API.")
	}
}

func GetAllForumPosts(db *sql.DB) ([]ForumPost, error) {
	rows, err := db.Query("SELECT post_id, user, user_num, timestamp, message, thread_path FROM forum_posts")
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var posts []ForumPost
	for rows.Next() {
		var post ForumPost
		if err := rows.Scan(&post.PostID, &post.User, &post.UserNum, &post.Timestamp, &post.Message, &post.ThreadPath); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		posts = append(posts, post)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return posts, nil
}

func InsertBatchEmbeddings(qdrantClient *qdrant.Client, embeddings [][]float32, posts []PostToEmbed) error {
	if len(embeddings) != len(posts) {
		return fmt.Errorf("embeddings/posts length mismatch: %d vs %d", len(embeddings), len(posts))
	}
	points := make([]*qdrant.PointStruct, len(posts))
	for i, post := range posts {
		postID, err := strconv.ParseUint(post.PostID, 10, 64)
		if err != nil {
			postID = uint64(hashString(post.PostID))
		}
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(postID),
			Vectors: qdrant.NewVectors(embeddings[i]...),
			Payload: qdrant.NewValueMap(map[string]any{
				"user":      post.User,
				"thread_id": post.ThreadID,
				"timestamp": post.Timestamp,
				"message":   post.Message,
				"post_id":   post.PostID,
			}),
		}
	}

	_, err := qdrantClient.Upsert(context.Background(), &qdrant.UpsertPoints{
		CollectionName: collectionName,
		Points:         points,
		Wait:           func(b bool) *bool { return &b }(true),
	})
	return err
}

func hashString(s string) int {
	hash := 0
	for _, c := range s {
		hash = 31*hash + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

func EnsureBatchTable(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS batch_jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		batch_id TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		completed BOOLEAN DEFAULT 0
	);
	`)
	return err
}

func SaveBatchID(db *sql.DB, batchID string) error {
	_, err := db.Exec(`INSERT INTO batch_jobs (batch_id, completed) VALUES (?, 0)`, batchID)
	return err
}

func CompleteBatches() {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Fatal: failed to open sqlite db at %s: %v", dbPath, err)
	}
	if err := MarkAllBatchesCompleted(db); err != nil {
		log.Printf("Warning: Failed to mark all batches completed: %v", err)
	} else {
		log.Println("All batch jobs marked as completed.")
	}
}

func MarkAllBatchesCompleted(db *sql.DB) error {
	_, err := db.Exec(`UPDATE batch_jobs SET completed = 1 WHERE completed = 0`)
	return err
}

func ListBatches() {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Fatal: failed to open sqlite db at %s: %v", dbPath, err)
	}
	batches, err := GetUncompletedBatchIDs(db)
	if err != nil {
		log.Fatalf("Fatal: Failed to get uncompleted batch IDs: %v", err)
	}
	if len(batches) == 0 {
		log.Println("No uncompleted batches found.")
		return
	}
	log.Println("Uncompleted batch IDs:")
	for _, batchID := range batches {
		log.Println("  " + batchID)
	}
}

func GetUncompletedBatchIDs(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT batch_id FROM batch_jobs WHERE completed = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batchIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		batchIDs = append(batchIDs, id)
	}
	return batchIDs, nil
}

func SearchForumPosts(query string, topK int) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENAI_API_KEY not set")
	}
	openaiClient := openai.NewClient(apiKey)

	// 1. Get query embedding
	embResp, err := openaiClient.CreateEmbeddings(context.Background(), openai.EmbeddingRequest{
		Input: []string{query},
		Model: openai.LargeEmbedding3, // Or ada-002
	})
	if err != nil {
		return "", fmt.Errorf("embedding request failed: %w", err)
	}
	if len(embResp.Data) == 0 {
		return "", fmt.Errorf("no embedding returned for query")
	}
	queryVec := embResp.Data[0].Embedding
	if len(queryVec) != vectorSize {
		return "", fmt.Errorf("embedding size mismatch: got %d, want %d", len(queryVec), vectorSize)
	}

	// 2. Connect to Qdrant
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{Host: qdrantHost, Port: qdrantPort})
	if err != nil {
		return "", fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	// 3. Build Qdrant QueryPoints struct (returns top K)
	queryPoints := &qdrant.QueryPoints{
		CollectionName: collectionName,
		Query:          qdrant.NewQuery(queryVec...), // Unpack the vector
		Limit:          func(v uint64) *uint64 { return &v }(uint64(topK)),
		WithPayload:    qdrant.NewWithPayload(true), // Get payload data
	}
	result, err := qdrantClient.Query(context.Background(), queryPoints)
	if err != nil {
		return "", fmt.Errorf("Qdrant query error: %w", err)
	}
	if len(result) == 0 {
		fmt.Println("No results found.")
		return "No results found.", nil
	}

	fmt.Println("Top results:")
	strResults := ""
	for i, pt := range result {
		fmt.Printf("Rank %d, score: %.4f\n", i+1, pt.Score)
		if pt.Payload != nil {
			fmt.Printf("  user: %v\n", pt.Payload["user"])
			fmt.Printf("  message: %v\n", pt.Payload["message"])
			fmt.Printf("  thread_id: %v\n", pt.Payload["thread_id"])
			fmt.Printf("  timestamp: %v\n", pt.Payload["timestamp"])
			strResults += fmt.Sprintf("Username %s:\n%s\n", pt.Payload["user"], pt.Payload["message"])
		}
		fmt.Println()
	}
	return strResults, nil
}
