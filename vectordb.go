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
	vectorSize = 1536
)

func CreateVectorDBForCharacter(character string) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Fatal: failed to open sqlite db at %s: %v", dbPath, err)
	}
	defer db.Close()

	log.Printf("Fetching posts for character %q from SQLite database...", character)
	posts, err := GetAllUserPosts(db, character)
	if err != nil {
		log.Fatalf("Fatal: Failed to get posts for character %q: %v", character, err)
	}
	if len(posts) == 0 {
		log.Printf("No posts found for character %q. Exiting.", character)
		return
	}
	log.Printf("Successfully found %d posts to process for character %q.\n", len(posts), character)

	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("Fatal: OPENAI_API_KEY environment variable is not set.")
	}
	openaiClient := openai.NewClient(apiKey)

	log.Println("Connecting to Qdrant...")
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host: qdrantHost,
		Port: qdrantPort,
	})
	if err != nil {
		log.Fatalf("Fatal: Could not create qdrant client: %v", err)
	}

	log.Printf("Ensuring Qdrant collection '%s' exists...\n", collectionName)
	err = qdrantClient.CreateCollection(context.Background(), &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     vectorSize,
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		log.Printf("Note: Could not create collection (it likely already exists): %v\n", err)
	}

	batchSize := 100
	for i := 0; i < len(posts); i += batchSize {
		end := i + batchSize
		if end > len(posts) {
			end = len(posts)
		}
		batch := posts[i:end]
		log.Printf("Processing batch %d-%d of %d for %q...\n", i+1, end, len(posts), character)

		textsToEmbed := make([]string, len(batch))
		for j, post := range batch {
			textsToEmbed[j] = post.Message
		}

		embeddings, err := getEmbeddings(openaiClient, textsToEmbed)
		if err != nil {
			log.Fatalf("Fatal: Failed to get embeddings for batch %d-%d: %v", i+1, end, err)
		}

		points := make([]*qdrant.PointStruct, len(batch))
		for j, post := range batch {
			postIDUint, err := strconv.ParseUint(post.PostID, 10, 64)
			if err != nil {
				log.Fatalf("Fatal: Failed to convert post_id '%s' to uint64: %v", post.PostID, err)
			}
			points[j] = &qdrant.PointStruct{
				Id:      qdrant.NewIDNum(postIDUint),
				Vectors: qdrant.NewVectors(embeddings[j]...),
				Payload: qdrant.NewValueMap(map[string]any{
					"user":        post.User,
					"timestamp":   post.Timestamp,
					"thread_path": post.ThreadPath,
					"message":     post.Message,
				}),
			}
		}
		_, err = qdrantClient.Upsert(context.Background(), &qdrant.UpsertPoints{
			CollectionName: collectionName,
			Points:         points,
			Wait:           func(b bool) *bool { return &b }(true),
		})
		if err != nil {
			log.Fatalf("Fatal: Failed to upsert batch %d-%d to Qdrant: %v", i+1, end, err)
		}
	}

	log.Println("---")
	log.Printf("Successfully indexed all posts for character %q into Qdrant.\n", character)
	log.Printf("Collection '%s' is now ready for searching.\n", collectionName)
}

// getEmbeddings takes a slice of texts and returns their vector embeddings
// from the OpenAI API.
func getEmbeddings(client *openai.Client, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	// Create the request for the OpenAI API
	req := openai.EmbeddingRequest{
		Input: texts,
		Model: openai.LargeEmbedding3,
	}

	// Call the API
	resp, err := client.CreateEmbeddings(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("failed to create embeddings from openai: %w", err)
	}

	// Extract the float vectors from the response
	embeddings := make([][]float32, len(resp.Data))
	for i, d := range resp.Data {
		embeddings[i] = d.Embedding
	}

	return embeddings, nil
}

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

const maxBatchSize = 50000

func CreateVectorDBForTFS(dryMode bool) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Fatal: failed to open sqlite db at %s: %v", dbPath, err)
	}
	defer db.Close()

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