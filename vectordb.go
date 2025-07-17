package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

const maxBatchSize = 500

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

func DownloadBatch(fileID string) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		fmt.Println("Set OPENAI_API_KEY env variable.")
		return
	}
	url := fmt.Sprintf("https://api.openai.com/v1/files/%s/content", fileID)
	outfile := "embeddings_output.jsonl"

	// Get content length
	req, _ := http.NewRequest("HEAD", url, nil)
	req.Header.Add("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error fetching headers:", err)
		return
	}
	lengthStr := resp.Header.Get("Content-Length")
	if lengthStr == "" {
		fmt.Println("No Content-Length header found")
		return
	}
	length, _ := strconv.Atoi(lengthStr)
	fmt.Println("Total file size:", length, "bytes")

	chunkSize := 50 * 1024 * 1024 // 50 MB
	start := 0
	part := 1

	f, err := os.Create(outfile)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer f.Close()

	for start < length {
		end := start + chunkSize - 1
		if end >= length {
			end = length - 1
		}
		fmt.Printf("Downloading bytes %d-%d...\n", start, end)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Add("Authorization", "Bearer "+apiKey)
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("Error on chunk %d: %v\n", part, err)
			return
		}
		if resp.StatusCode != 206 && resp.StatusCode != 200 {
			fmt.Printf("Server returned status %d on chunk %d\n", resp.StatusCode, part)
			return
		}

		_, err = io.Copy(f, resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("Error writing chunk %d: %v\n", part, err)
			return
		}
		start = end + 1
		part++
	}
	fmt.Println("Download complete!")
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

func DownloadCompletedBatches() error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	defer db.Close()

	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("OPENAI_API_KEY not set")
	}
	openaiClient := openai.NewClient(apiKey)

	batchIDs, err := GetUncompletedBatchIDs(db)
	if err != nil {
		return fmt.Errorf("failed to fetch batch IDs: %w", err)
	}

	for _, batchID := range batchIDs {
		// --- Poll/check completion ---
		batch, err := openaiClient.RetrieveBatch(context.Background(), batchID)
		if err != nil {
			log.Printf("Failed to get batch %s: %v (will retry later)", batchID, err)
			continue
		}
		if batch.Status != "completed" {
			log.Printf("Batch %s not complete (status: %s), skipping.", batchID, batch.Status)
			continue
		}
		// --- Download results file ---
		fileID := batch.OutputFileID // Should be batch.OutputFileID (verify with your actual struct)
		if *fileID == "" {
			log.Printf("No output file ID for batch %s, skipping.", batchID)
			continue
		}
		log.Printf("Downloading completed batch %s (file id: %s)...", batchID, fileID)
		DownloadBatch(*fileID)
		// --- Mark as completed ---
		_, err = db.Exec(`UPDATE batch_jobs SET completed = 1 WHERE batch_id = ?`, batchID)
		if err != nil {
			log.Printf("Warning: Could not mark batch %s as completed: %v", batchID, err)
			continue
		}
		log.Printf("Batch %s marked as completed.", batchID)
	}
	return nil
}
