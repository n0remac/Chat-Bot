package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/glebarez/sqlite"
	"github.com/sashabaranov/go-openai"
	"gorm.io/gorm"
)

type CharacterSheet struct {
	Name                   string              `json:"name"`
	PersonalityTraits      []string            `json:"personality_traits"`
	Likes                  []string            `json:"likes"`
	Dislikes               []string            `json:"dislikes"`
	Fears                  []string            `json:"fears"`
	Catchphrases           []string            `json:"catchphrases"`
	Skills                 []string            `json:"skills"`
	Goals                  []string            `json:"goals"`
	Affiliations           []string            `json:"affiliations"`
	ImportantRelationships []map[string]string `json:"important_relationships"` // [{"name": "...", "type": "..."}]
}

// Define function JSON schema for OpenAI
var characterSheetFunction = openai.FunctionDefinition{
	Name:        "extract_character_sheet",
	Description: "Extract character sheet details about a forum roleplaying character.",
	Parameters: map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name":               map[string]string{"type": "string", "description": "The character's name"},
			"personality_traits": map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"likes":              map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"dislikes":           map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"fears":              map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"catchphrases":       map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"skills":             map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"goals":              map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"affiliations":       map[string]interface{}{"type": "array", "items": map[string]string{"type": "string"}},
			"important_relationships": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"name": map[string]string{"type": "string"},
						"type": map[string]string{"type": "string"},
					},
					"required": []string{"name", "type"},
				},
			},
		},
		"required": []string{"name"},
	},
}

func ExtractCharacterSheet(client *openai.Client, chunk string, charName string, dryRun bool) (*CharacterSheet, error) {
	if dryRun {
		return &CharacterSheet{
			Name:              charName,
			PersonalityTraits: []string{"brave"},
			// Fill others with dummy data
		}, nil
	}

	ctx := context.Background()

	functions := []openai.FunctionDefinition{characterSheetFunction}
	msgs := []openai.ChatCompletionMessage{
		{
			Role:    "system",
			Content: "You are an expert at extracting detailed character sheets from fantasy roleplay forum posts.",
		},
		{
			Role:    "user",
			Content: fmt.Sprintf("Extract as much character sheet information as possible for the character '%s' from the following posts. Only consider what can be reasonably inferred from these posts:\n\n%s", charName, chunk),
		},
	}

	resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:     openai.GPT4o,
		Messages:  msgs,
		Functions: functions,
		FunctionCall: openai.FunctionCall{
			Name: "extract_character_sheet",
		},
	})
	if err != nil {
		return nil, err
	}

	// Extract the function response
	var cs CharacterSheet
	for _, choice := range resp.Choices {
		if choice.Message.FunctionCall != nil && choice.Message.FunctionCall.Arguments != "" {
			err := json.Unmarshal([]byte(choice.Message.FunctionCall.Arguments), &cs)
			if err != nil {
				return nil, err
			}
			return &cs, nil
		}
	}
	return nil, fmt.Errorf("No function response in completion")
}

func SynthesizeMasterSheet(client *openai.Client, username string, sheets []*CharacterSheet, dryRun bool) (*CharacterSheet, error) {
	if dryRun {
		return sheets[0], nil
	}
	jsons := make([]string, len(sheets))
	for i, s := range sheets {
		b, _ := json.Marshal(s)
		jsons[i] = string(b)
	}
	prompt := fmt.Sprintf(
		"You are an expert at synthesizing character sheets for roleplaying characters. "+
			"Merge the following JSON character sheets for '%s' into a single master sheet, "+
			"deduplicating and combining information where possible. "+
			"Only output the master sheet as JSON, with the same structure as the input. Do not include any explanation or commentary, just the JSON object.\n\n%s",
		username, strings.Join(jsons, "\n\n"))

	ctx := context.Background()
	msgs := []openai.ChatCompletionMessage{
		{
			Role:    "system",
			Content: "You are an expert at merging structured character sheets for roleplaying characters.",
		},
		{
			Role:    "user",
			Content: prompt,
		},
	}

	resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:    openai.GPT4o,
		Messages: msgs,
	})
	if err != nil {
		return nil, err
	}
	var cs CharacterSheet
	for _, choice := range resp.Choices {
		if choice.Message.Content != "" {
			jsonStr, err := extractFirstJSON(choice.Message.Content)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal([]byte(jsonStr), &cs)
			if err != nil {
				return nil, err
			}
			return &cs, nil
		}
	}
	return nil, fmt.Errorf("No content in completion")
}

func extractFirstJSON(s string) (string, error) {
	// Remove code fences
	re := regexp.MustCompile("(?s)```(?:json)?(.*?)```")
	m := re.FindStringSubmatch(s)
	if len(m) > 1 {
		return strings.TrimSpace(m[1]), nil
	}
	// Fallback: find first { ... }
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start >= 0 && end > start {
		return s[start : end+1], nil
	}
	return "", errors.New("no JSON object found")
}

func GetAllUserPosts(db *gorm.DB, username string) ([]ForumPost, error) {
	var posts []ForumPost
	err := db.Where("user = ?", username).Order("timestamp asc").Find(&posts).Error
	return posts, err
}

func ConcatenatePosts(posts []ForumPost) string {
	var builder strings.Builder
	for _, post := range posts {
		// Include thread or timestamp if you want context
		builder.WriteString(fmt.Sprintf("[Thread: %s, Time: %d]\n%s\n\n", post.ThreadPath, post.Timestamp, post.Message))
	}
	return builder.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Charactar(username string, dryRun bool) {
	maxChars := 500_000

	db, err := gorm.Open(sqlite.Open("data/docs.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}

	posts, err := GetAllUserPosts(db, username)
	if err != nil || len(posts) == 0 {
		log.Fatalf("failed to get posts: %v", err)
	}
	fmt.Printf("Found %d posts for %s\n", len(posts), username)

	chunks := ChunkPosts(posts, maxChars)
	fmt.Printf("Split into %d chunks.\n", len(chunks))

	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	sheets := make([]*CharacterSheet, 0, len(chunks))

	for i, chunk := range chunks {
		fmt.Printf("Extracting character sheet from chunk %d/%d...\n", i+1, len(chunks))
		cs, err := ExtractCharacterSheet(client, ConcatenatePosts(chunk), username, dryRun)
		if err != nil {
			log.Printf("Extraction failed: %v", err)
			continue
		}
		out, _ := json.MarshalIndent(cs, "", "  ")
		fmt.Printf("Chunk %d character sheet:\n%s\n", i+1, out)
		sheets = append(sheets, cs)
		// Optionally: collect for merging later
	}
	fmt.Printf("------------------------------------")
	masterSheet, err := SynthesizeMasterSheet(client, username, sheets, dryRun)
	if err != nil {
		log.Fatalf("Failed to synthesize master sheet: %v", err)
	}
	out, _ := json.MarshalIndent(masterSheet, "", "  ")
	fmt.Printf("Master character sheet for %s:\n%s\n", username, out)
	if !dryRun {
		outputPath := fmt.Sprintf("data/tfs/characters/%s.json", strings.ToLower(strings.ReplaceAll(username, " ", "-")))
		if err := os.WriteFile(outputPath, out, 0644); err != nil {
			log.Fatalf("Failed to write master sheet to %s: %v", outputPath, err)
		}
		fmt.Printf("Master character sheet saved to %s\n", outputPath)
	}
}

var bestPostsFunction = openai.FunctionDefinition{
	Name:        "select_best_posts",
	Description: "Select the five most representative or impressive in-character posts for the given character.",
	Parameters: map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"best_posts": map[string]interface{}{
				"type":        "array",
				"items":       map[string]string{"type": "string"},
				"description": "The five best or most representative posts.",
			},
		},
		"required": []string{"best_posts"},
	},
}

func SelectBestPosts(client *openai.Client, posts []ForumPost, charName string, dryRun bool) ([]string, error) {
	if dryRun {
		n := min(len(posts), 5)
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, posts[i].Message)
		}
		return out, nil
	}

	ctx := context.Background()

	// Concatenate posts with minimal context for the LLM
	var sb strings.Builder
	for i, post := range posts {
		sb.WriteString(fmt.Sprintf("Post %d:\n%s\n\n", i+1, post.Message))
	}

	systemPrompt := fmt.Sprintf(
		"You are an expert at analyzing in-character writing for a fantasy roleplaying forum. "+
			"Your task is to select the five best posts for the character '%s', showcasing their unique personality, voice, and most impressive or representative writing.",
		charName,
	)

	msgs := []openai.ChatCompletionMessage{
		{
			Role:    "system",
			Content: systemPrompt,
		},
		{
			Role:    "user",
			Content: fmt.Sprintf("Here are %d posts:\n\n%s\n\nSelect the five best or most representative in-character posts, returning only the exact full text for each.", len(posts), sb.String()),
		},
	}

	resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:     openai.GPT4o, // or your preferred model
		Messages:  msgs,
		Functions: []openai.FunctionDefinition{bestPostsFunction},
		FunctionCall: openai.FunctionCall{
			Name: "select_best_posts",
		},
	})
	if err != nil {
		return nil, err
	}

	type BestPosts struct {
		BestPosts []string `json:"best_posts"`
	}

	for _, choice := range resp.Choices {
		if choice.Message.FunctionCall != nil && choice.Message.FunctionCall.Arguments != "" {
			var result BestPosts
			if err := json.Unmarshal([]byte(choice.Message.FunctionCall.Arguments), &result); err != nil {
				return nil, err
			}
			return result.BestPosts, nil
		}
	}
	return nil, fmt.Errorf("No function response in completion")
}

func BestPosts(username string, dryRun bool) {
	maxChars := 500_000

	db, err := gorm.Open(sqlite.Open("data/docs.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}

	posts, err := GetAllUserPosts(db, username)
	if err != nil || len(posts) == 0 {
		log.Fatalf("failed to get posts: %v", err)
	}
	fmt.Printf("Found %d posts for %s\n", len(posts), username)

	chunks := ChunkPosts(posts, maxChars)
	fmt.Printf("Split into %d chunks.\n", len(chunks))

	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	bestPosts := make([]string, 0, len(chunks))

	for i, chunk := range chunks {
		fmt.Printf("Selecting best posts from chunk %d/%d...\n", i+1, len(chunks))
		selectedPosts, err := SelectBestPosts(client, chunk, username, dryRun)
		if err != nil {
			log.Printf("Selection failed: %v", err)
			continue
		}
		fmt.Printf("Chunk %d selected posts:\n%s\n", i+1, strings.Join(selectedPosts, "\n---\n"))
		bestPosts = append(bestPosts, selectedPosts...)
	}

	fmt.Printf("------------------------------------")
	fmt.Printf("Best posts for %s:\n%s\n", username, strings.Join(bestPosts, "\n---\n"))
	// save to /data/tfs/writing/<username>-best-posts.txt
	if !dryRun {
		outputPath := fmt.Sprintf("data/tfs/writing/%s-best-posts.txt", strings.ToLower(strings.ReplaceAll(username, " ", "-")))
		if err := os.WriteFile(outputPath, []byte(strings.Join(bestPosts, "\n---\n")), 0644); err != nil {
			log.Fatalf("Failed to write best posts to %s: %v", outputPath, err)
		}
		fmt.Printf("Best posts saved to %s\n", outputPath)
	}
}
