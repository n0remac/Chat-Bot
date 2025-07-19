package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/glebarez/go-sqlite"
)

// ---- ForumPost Struct ----
type ForumPost struct {
	PostID     string
	User       string
	UserNum    int
	Timestamp  int64
	Message    string
	ThreadPath string
}

func CountLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}
	fmt.Println(lines)
	return lines, scanner.Err()
}

// ---- Find All `posts` Files ----
func FindPostsFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() == "posts" {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// ---- Parse a Single Posts File ----
func ParsePostsFile(path string, threadPath string) ([]ForumPost, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw map[string]map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("error parsing %s: %w", path, err)
	}

	var posts []ForumPost
	for postID, post := range raw {
		user, _ := post["user"].(string)
		var userNum int
		if val, ok := post["user_num"].(float64); ok {
			userNum = int(val)
		}
		var timestamp int64
		switch v := post["timestamp"].(type) {
		case float64:
			timestamp = int64(v)
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				timestamp = parsed
			} else {
				fmt.Println("Failed to parse timestamp string:", v)
			}
		default:
			fmt.Println("timestamp field missing or unknown type:", post["timestamp"])
		}
		message, _ := post["message"].(string)
		posts = append(posts, ForumPost{
			PostID:     postID,
			User:       user,
			UserNum:    userNum,
			Timestamp:  timestamp,
			Message:    message,
			ThreadPath: threadPath,
		})
	}
	return posts, nil
}

// ---- Scrape and Batch Insert into DB ----
func ScrapeAndInsertPosts(db *sql.DB, basePath string) error {
	const batchSize = 1000 // Tune this for your system

	files, err := FindPostsFiles(basePath)
	if err != nil {
		return err
	}

	// Prepare statement for inserting posts
	stmt, err := db.Prepare(`
		INSERT OR REPLACE INTO forum_posts
		(post_id, user, user_num, timestamp, message, thread_path)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	for _, postsPath := range files {
		fmt.Printf("Processing %s...\n", postsPath)
		relPath, _ := filepath.Rel(basePath, postsPath)
		threadPath := strings.TrimSuffix(relPath, "/posts")
		posts, err := ParsePostsFile(postsPath, threadPath)
		if err != nil {
			fmt.Printf("Error parsing %s: %v\n", postsPath, err)
			continue
		}

		// Batch insert
		for i := 0; i < len(posts); i += batchSize {
			end := i + batchSize
			if end > len(posts) {
				end = len(posts)
			}
			batch := posts[i:end]
			tx, err := db.Begin()
			if err != nil {
				fmt.Printf("Failed to begin transaction: %v\n", err)
				continue
			}
			for _, p := range batch {
				_, err := tx.Stmt(stmt).Exec(p.PostID, p.User, p.UserNum, p.Timestamp, p.Message, p.ThreadPath)
				if err != nil {
					fmt.Printf("Insert error (post_id=%s): %v\n", p.PostID, err)
				}
			}
			if err := tx.Commit(); err != nil {
				fmt.Printf("Commit error: %v\n", err)
			}
			fmt.Print(".")
		}
	}
	return nil
}

func ensureForumPostsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS forum_posts (
			post_id TEXT PRIMARY KEY,
			user TEXT,
			user_num INTEGER,
			timestamp INTEGER,
			message TEXT,
			thread_path TEXT
		)
	`)
	return err
}

func Scrape() {
	db, err := sql.Open("sqlite", "data/docs.db")
	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}
	defer db.Close()
	if err := ensureForumPostsTable(db); err != nil {
		panic(fmt.Sprintf("failed to create table: %v", err))
	}

	basePath := "data/tfs/forum/"
	if err := ScrapeAndInsertPosts(db, basePath); err != nil {
		fmt.Println("Error:", err)
	}
}

// ---- Main Entrypoint ----
func main() {
	mode := flag.String("mode", "", "Mode to run: scrape, summarize, timeline, character, chat, or best")
	dryRun := flag.Bool("dry-run", false, "Run without making changes (for testing)")
	threadPath := flag.String("thread", "", "Thread path to summarize (e.g. overworld/isran-empire/free-plains-isra/isra-free-city/threads/midnight-sun)")
	username := flag.String("username", "Empress Naoki", "Username for timeline generation")

	csPath := flag.String("cs", "data/tfs/characters/naoki.json", "Path to character sheet JSON")
	writingPath := flag.String("writing", "data/tfs/writing/empress-naoki-posts.txt", "Path to original writing sample")
	userMessage := flag.String("message", "Hello, how are you?", "User message for chat")
	num := flag.Int("num", 5, "Number of results")
	flag.Parse()

	switch *mode {
	case "scrape":
		Scrape()
	case "summarize":
		Summarize(*dryRun, *threadPath)
	case "timeline":
		Timeline(*dryRun, *username)
	case "character":
		Charactar(*username, *dryRun)
	case "chat":
		msg, err := Chat(*csPath, *writingPath, *userMessage)
		if err != nil {
			fmt.Println("Chat error:", err)
			return
		}
		fmt.Println(msg)
	case "best":
		BestPosts(*username, *dryRun)
	case "discord":
		StartDiscordBot()
	case "vector":
		CreateVectorDBForTFS(*dryRun)
	case "complete-batches":
		CompleteBatches()
	case "list-batches":
		ListBatches()
	case "download-batches":
		Batches()
	case "load-embeddings":
		LoadEmbeddings()
	case "search":
		SearchForumPosts(*userMessage, *num)
	case "count-lines":
		CountLines(*csPath)
	default:
		fmt.Println("Please specify a mode: scrape, summarize, or timeline")
	}
}
