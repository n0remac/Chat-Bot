package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/qdrant/go-client/qdrant"
)

// Structs for parsing the batches API response
type BatchesResponse struct {
	Object  string  `json:"object"`
	Data    []Batch `json:"data"`
	HasMore bool    `json:"has_more"`
	LastID  string  `json:"last_id"`
}

type Batch struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	OutputFileID string `json:"output_file_id"`
}

type BatchLine struct {
	CustomID string `json:"custom_id"`
	Response struct {
		Body struct {
			Data []struct {
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		} `json:"body"`
	} `json:"response"`
}

const (
	OUTDIR        = "openai_batches"
	COMBINED_FILE = "combined.jsonl"
)

func AllBatches() {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("OPENAI_API_KEY not set in environment")
	}
	os.MkdirAll(OUTDIR, 0755)

	// Step 1: Fetch all batches
	var allBatches []Batch

	after := ""
	for {
		// Build URL with optional after parameter
		url := "https://api.openai.com/v1/batches"
		if after != "" {
			url += "?after=" + after
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			log.Fatalf("Failed to fetch batches: %s\n%s", resp.Status, string(body))
		}

		var batchesResp BatchesResponse
		if err := json.NewDecoder(resp.Body).Decode(&batchesResp); err != nil {
			log.Fatal("Failed to decode JSON:", err)
		}

		allBatches = append(allBatches, batchesResp.Data...)

		if batchesResp.HasMore {
			after = batchesResp.LastID // Use the last_id for pagination
		} else {
			break
		}
	}

	// Step 2: Download all output files for completed batches
	var downloadedFiles []string
	for _, batch := range allBatches {
		if batch.Status != "completed" || batch.OutputFileID == "" {
			continue
		}
		outfile := filepath.Join(OUTDIR, batch.ID+".jsonl")
		if fileExistsAndNotEmpty(outfile) {
			fmt.Printf("File %s exists, skipping.\n", outfile)
			downloadedFiles = append(downloadedFiles, outfile)
			continue
		}
		fmt.Printf("Downloading %s (file id: %s)...\n", outfile, batch.OutputFileID)
		fileURL := fmt.Sprintf("https://api.openai.com/v1/files/%s/content", batch.OutputFileID)
		req, err := http.NewRequest("GET", fileURL, nil)
		if err != nil {
			log.Println("  Request error:", err)
			continue
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("  Download error:", err)
			continue
		}
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("  Failed to download file: %s\n%s", resp.Status, string(body))
			continue
		}
		f, err := os.Create(outfile)
		if err != nil {
			log.Println("  File create error:", err)
			resp.Body.Close()
			continue
		}
		_, err = io.Copy(f, resp.Body)
		resp.Body.Close()
		f.Close()
		if err != nil {
			log.Println("  File write error:", err)
			continue
		}
		downloadedFiles = append(downloadedFiles, outfile)
	}

	// Step 3: Combine all downloaded files into one
	combinedPath := filepath.Join(OUTDIR, COMBINED_FILE)
	fmt.Println("Combining files into", combinedPath)
	combined, err := os.Create(combinedPath)
	if err != nil {
		log.Fatal("Failed to create combined file:", err)
	}
	defer combined.Close()
	for _, fname := range downloadedFiles {
		f, err := os.Open(fname)
		if err != nil {
			log.Println("  Skipping file:", fname, err)
			continue
		}
		_, err = io.Copy(combined, f)
		f.Close()
		if err != nil {
			log.Println("  Error combining file:", fname, err)
			continue
		}
	}
	fmt.Println("Combined file is", combinedPath)

	// Step 4: remove individual files
	for _, fname := range downloadedFiles {
		if err := os.Remove(fname); err != nil {
			log.Println("  Error removing file:", fname, err)
		} else {
			fmt.Println("Removed individual file:", fname)
		}
	}
	fmt.Println("All done! Combined file is ready for processing.")
}

func CheckBatchStatuses() {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("OPENAI_API_KEY not set in environment")
	}

	// Read batches.txt
	batchIDs := make(map[string]struct{})
	file, err := os.Open("batches.txt")
	if err != nil {
		log.Fatal("Failed to open batches.txt:", err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			batchIDs[line] = struct{}{}
		}
	}
	file.Close()
	if len(batchIDs) == 0 {
		log.Fatal("No batch IDs found in batches.txt")
	}

	// Fetch all batches metadata (pagination)
	allBatches := make(map[string]Batch)
	after := ""
	for {
		url := "https://api.openai.com/v1/batches"
		if after != "" {
			url += "?after=" + after
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			log.Fatalf("Failed to fetch batches: %s\n%s", resp.Status, string(body))
		}

		var batchesResp BatchesResponse
		if err := json.NewDecoder(resp.Body).Decode(&batchesResp); err != nil {
			log.Fatal("Failed to decode JSON:", err)
		}

		for _, batch := range batchesResp.Data {
			allBatches[batch.ID] = batch
		}

		if batchesResp.HasMore {
			after = batchesResp.LastID
		} else {
			break
		}
	}

	// Print status for each batch in batches.txt
	fmt.Println("Batch Statuses:")
	for id := range batchIDs {
		if batch, found := allBatches[id]; found {
			fmt.Printf("%s: %s\n", id, batch.Status)
		} else {
			fmt.Printf("%s: not found\n", id)
		}
	}
}

func BatchesFromFile() {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("OPENAI_API_KEY not set in environment")
	}
	os.MkdirAll(OUTDIR, 0755)

	// Step 1: Read batches.txt (one batch ID per line)
	batchIDs := make(map[string]struct{})
	file, err := os.Open("batches.txt")
	if err != nil {
		log.Fatal("Failed to open batches.txt:", err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			batchIDs[line] = struct{}{}
		}
	}
	file.Close()
	if len(batchIDs) == 0 {
		log.Fatal("No batch IDs found in batches.txt")
	}

	// Step 2: Fetch all batches metadata from OpenAI
	var allBatches []Batch
	after := ""
	for {
		url := "https://api.openai.com/v1/batches"
		if after != "" {
			url += "?after=" + after
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			log.Fatalf("Failed to fetch batches: %s\n%s", resp.Status, string(body))
		}

		var batchesResp BatchesResponse
		if err := json.NewDecoder(resp.Body).Decode(&batchesResp); err != nil {
			log.Fatal("Failed to decode JSON:", err)
		}

		allBatches = append(allBatches, batchesResp.Data...)
		if batchesResp.HasMore {
			after = batchesResp.LastID
		} else {
			break
		}
	}

	// Step 3: Download output files for batches listed in batches.txt
	var downloadedFiles []string
	for _, batch := range allBatches {
		if _, found := batchIDs[batch.ID]; !found {
			continue // skip batches not in the list
		}
		if batch.Status != "completed" || batch.OutputFileID == "" {
			fmt.Printf("Batch %s not ready for download (status: %s)\n", batch.ID, batch.Status)
			continue
		}
		outfile := filepath.Join(OUTDIR, batch.ID+".jsonl")
		if fileExistsAndNotEmpty(outfile) {
			fmt.Printf("File %s exists, skipping.\n", outfile)
			downloadedFiles = append(downloadedFiles, outfile)
			continue
		}
		fmt.Printf("Downloading %s (file id: %s)...\n", outfile, batch.OutputFileID)
		fileURL := fmt.Sprintf("https://api.openai.com/v1/files/%s/content", batch.OutputFileID)
		req, err := http.NewRequest("GET", fileURL, nil)
		if err != nil {
			log.Println("  Request error:", err)
			continue
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("  Download error:", err)
			continue
		}
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("  Failed to download file: %s\n%s", resp.Status, string(body))
			continue
		}
		f, err := os.Create(outfile)
		if err != nil {
			log.Println("  File create error:", err)
			resp.Body.Close()
			continue
		}
		_, err = io.Copy(f, resp.Body)
		resp.Body.Close()
		f.Close()
		if err != nil {
			log.Println("  File write error:", err)
			continue
		}
		downloadedFiles = append(downloadedFiles, outfile)
	}

	// Step 4: Combine all downloaded files into one
	combinedPath := filepath.Join(OUTDIR, COMBINED_FILE)
	fmt.Println("Combining files into", combinedPath)
	combined, err := os.Create(combinedPath)
	if err != nil {
		log.Fatal("Failed to create combined file:", err)
	}
	defer combined.Close()
	for _, fname := range downloadedFiles {
		f, err := os.Open(fname)
		if err != nil {
			log.Println("  Skipping file:", fname, err)
			continue
		}
		_, err = io.Copy(combined, f)
		f.Close()
		if err != nil {
			log.Println("  Error combining file:", fname, err)
			continue
		}
	}
	fmt.Println("Combined file is", combinedPath)

	// Step 5: remove individual files
	for _, fname := range downloadedFiles {
		if err := os.Remove(fname); err != nil {
			log.Println("  Error removing file:", fname, err)
		} else {
			fmt.Println("Removed individual file:", fname)
		}
	}
	fmt.Println("All done! Combined file is ready for processing.")
}

func fileExistsAndNotEmpty(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Size() > 0
}

func ImportEmbeddingsFromJSONL(jsonlPath string, db *sql.DB, qdrantClient *qdrant.Client) error {
	file, err := os.Open(jsonlPath)
	if err != nil {
		return fmt.Errorf("failed to open JSONL file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var toInsert []struct {
		Post      PostToEmbed
		Embedding []float32
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		var entry BatchLine
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("Skipping line (unmarshal error): %v", err)
			continue
		}
		if entry.CustomID == "" || len(entry.Response.Body.Data) == 0 {
			log.Printf("Skipping line (missing custom_id or embedding): %s", string(line))
			continue
		}

		// Lookup original post
		var post PostToEmbed
		err := db.QueryRow(`
			SELECT post_id, user, message, thread_path, timestamp
			FROM forum_posts WHERE post_id = ?
		`, entry.CustomID).Scan(&post.PostID, &post.User, &post.Message, &post.ThreadID, &post.Timestamp)
		if err != nil {
			log.Printf("Skipping embedding with custom_id %s (not found in db): %v", entry.CustomID, err)
			continue
		}

		toInsert = append(toInsert, struct {
			Post      PostToEmbed
			Embedding []float32
		}{Post: post, Embedding: entry.Response.Body.Data[0].Embedding})

		// Optional: Insert in batches for efficiency
		if len(toInsert) >= 100 {
			if err := batchInsertQdrant(toInsert, qdrantClient); err != nil {
				log.Printf("Batch insert error: %v", err)
			}
			toInsert = nil
		}
	}
	// Insert any leftovers
	if len(toInsert) > 0 {
		if err := batchInsertQdrant(toInsert, qdrantClient); err != nil {
			log.Printf("Final batch insert error: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}
	return nil
}

// Batch insert into Qdrant (using your InsertBatchEmbeddings logic)
func batchInsertQdrant(batch []struct {
	Post      PostToEmbed
	Embedding []float32
}, qdrantClient *qdrant.Client) error {
	points := make([]*qdrant.PointStruct, len(batch))
	for i, item := range batch {
		postID, err := strconv.ParseUint(item.Post.PostID, 10, 64)
		if err != nil {
			postID = uint64(hashString(item.Post.PostID))
		}
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(postID),
			Vectors: qdrant.NewVectors(item.Embedding...),
			Payload: qdrant.NewValueMap(map[string]any{
				"user":      item.Post.User,
				"thread_id": item.Post.ThreadID,
				"timestamp": item.Post.Timestamp,
				"message":   item.Post.Message,
				"post_id":   item.Post.PostID,
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

func LoadEmbeddings() {
	db, _ := sql.Open("sqlite", dbPath)
	defer db.Close()

	qdrantClient, _ := qdrant.NewClient(&qdrant.Config{Host: qdrantHost, Port: qdrantPort})

	if err := EnsureQdrantCollection(qdrantClient, collectionName, vectorSize); err != nil {
		log.Fatalf("Failed to ensure Qdrant collection: %v", err)
	}

	if err := ImportEmbeddingsFromJSONL("openai_batches/combined.jsonl", db, qdrantClient); err != nil {
		log.Fatal("Import error:", err)
	}
}

func EnsureQdrantCollection(qdrantClient *qdrant.Client, collectionName string, vectorSize int) error {
	// Try to create the collection (it will fail if it already exists, but that's fine)
	err := qdrantClient.CreateCollection(context.Background(), &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(vectorSize),
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		// Qdrant will error if it already exists, but it's not fatal
		log.Printf("Note: Could not create collection (it may already exist): %v", err)
	}
	return nil
}
