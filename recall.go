package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/sashabaranov/go-openai"
)

// Function definitions for OpenAI function-calling

var relevanceFunction = openai.FunctionDefinition{
	Name:        "is_relevant",
	Description: "Check if any of the following posts are relevant to the user's message.",
	Parameters: map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"relevant": map[string]interface{}{"type": "boolean"},
			"reasons":  map[string]interface{}{"type": "string"},
			"post_indexes": map[string]interface{}{
				"type":        "array",
				"items":       map[string]string{"type": "integer"},
				"description": "Indexes of relevant posts in the chunk",
			},
		},
		"required": []string{"relevant"},
	},
}

var extractInfoFunction = openai.FunctionDefinition{
	Name:        "extract_relevant_info",
	Description: "Summarize the important information from these posts as it pertains to the user's message.",
	Parameters: map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"summary": map[string]interface{}{"type": "string"},
		},
		"required": []string{"summary"},
	},
}

// Main recall function: returns a string summary, or "" if nothing found
func RecallRelevantMemory(db *sql.DB, client *openai.Client, characterName, userMessage string, dryRun bool) (string, error) {
	log.Println("Recalling relevant memory for character:", characterName, "User message:", userMessage)
	// 1. Get ALL posts for the character (or a lot of them, to keep cost down)
	posts, err := GetAllUserPosts(db, characterName)
	if err != nil {
		return "", err
	}
	if len(posts) == 0 {
		return "", nil
	}

	chunkSize := 1000000 // Tune for token/cost
	log.Printf("Total posts found: %d, chunking into size %d", len(posts), chunkSize)
	chunks := ChunkPosts(posts, chunkSize)
	log.Printf("Total chunks created: %d", len(chunks))

	var allRelevantPosts []ForumPost
	for ci, chunk := range chunks {
		log.Printf("Processing chunk %d/%d with %d posts", ci+1, len(chunks), len(chunk))
		// 2. Function call 1: Relevance check
		if dryRun {
			// For dryRun, simulate every other chunk as relevant
			if ci%2 == 0 {
				allRelevantPosts = append(allRelevantPosts, chunk...)
			}
			continue
		}

		postsText := postsToString(chunk)
		prompt := fmt.Sprintf(
			"User message:\n%s\n\nPosts:\n%s\n\n"+
				"Are any of these posts relevant to answering the user's message? "+
				"If so, return true and the indexes of relevant posts in the chunk (starting from 0).",
			userMessage, postsText)

		resp, err := client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
			Model:     "gpt-4.1-nano-2025-04-14",
			Messages:  []openai.ChatCompletionMessage{{Role: "user", Content: prompt}},
			Functions: []openai.FunctionDefinition{relevanceFunction},
			FunctionCall: openai.FunctionCall{
				Name: "is_relevant",
			},
			MaxTokens: 512,
		})
		if err != nil {
			return "", fmt.Errorf("OpenAI relevance check: %v", err)
		}

		type RelevantResp struct {
			Relevant    bool   `json:"relevant"`
			Reasons     string `json:"reasons"`
			PostIndexes []int  `json:"post_indexes"`
		}
		found := false
		for _, choice := range resp.Choices {
			if choice.Message.FunctionCall != nil && choice.Message.FunctionCall.Arguments != "" {
				var relResp RelevantResp
				if err := json.Unmarshal([]byte(choice.Message.FunctionCall.Arguments), &relResp); err != nil {
					continue
				}
				if relResp.Relevant && len(relResp.PostIndexes) > 0 {
					for _, idx := range relResp.PostIndexes {
						if idx >= 0 && idx < len(chunk) {
							allRelevantPosts = append(allRelevantPosts, chunk[idx])
						}
					}
					found = true
				}
			}
		}
		if !found {
			continue // no relevant posts in this chunk
		}
	}

	if len(allRelevantPosts) == 0 {
		return "I do not have any relevant memories about that.", nil
	}

	// 3. Function call 2: Summarization
	if dryRun {
		return fmt.Sprintf("[Memory: %d relevant posts found, summarized here...]", len(allRelevantPosts)), nil
	}

	relevantText := postsToString(allRelevantPosts)
	prompt := fmt.Sprintf(
		"User message:\n%s\n\nRelevant posts from my memory:\n%s\n\n"+
			"Summarize the important information from these posts as it pertains to the user's message. "+
			"If none of them are directly relevant, say so clearly.",
		userMessage, relevantText,
	)
	resp, err := client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
		Model:     "gpt-4.1-nano-2025-04-14",
		Messages:  []openai.ChatCompletionMessage{{Role: "user", Content: prompt}},
		Functions: []openai.FunctionDefinition{extractInfoFunction},
		FunctionCall: openai.FunctionCall{
			Name: "extract_relevant_info",
		},
		MaxTokens: 1024,
	})
	if err != nil {
		return "", fmt.Errorf("OpenAI info extraction: %v", err)
	}

	type InfoResp struct {
		Summary string `json:"summary"`
	}
	for _, choice := range resp.Choices {
		if choice.Message.FunctionCall != nil && choice.Message.FunctionCall.Arguments != "" {
			var info InfoResp
			if err := json.Unmarshal([]byte(choice.Message.FunctionCall.Arguments), &info); err == nil {
				return info.Summary, nil
			}
		}
	}

	return "", fmt.Errorf("No summary found in completion")
}

// Utility to pretty-print forum posts for LLM input
func postsToString(posts []ForumPost) string {
	var sb strings.Builder
	for i, post := range posts {
		sb.WriteString(fmt.Sprintf("Post %d by %s at %d:\n%s\n\n", i, post.User, post.Timestamp, post.Message))
	}
	return sb.String()
}

func Recall(postDb, memoryDb *sql.DB, req MemoryRequest) {
	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	// Recall relevant memory for the character and latest message
	specificMemory, err := RecallRelevantMemory(postDb, client, req.CharacterName, req.Message.Content, false)
	if err != nil {
		log.Printf("[memoryLoop] RecallRelevantMemory error: %v", err)
	}
	log.Printf("[memoryLoop] Recalled memory for character=%s: %.200s", req.CharacterName, specificMemory)

	// Insert recalled memory as a "memory" context (unless empty)
	if s := strings.TrimSpace(specificMemory); s != "" && s != "I do not have any relevant memories about that." {
		_, err := memoryDb.Exec(`INSERT INTO contexts (channel_id, author_id, username, content, time, type)
				VALUES (?, ?, ?, ?, ?, ?)`,
			req.ChannelID, "bot", "Memory", specificMemory, req.Message.Time, "memory")
		if err != nil {
			log.Printf("[memoryLoop] Failed to insert memory context: %v", err)
			// continue to still store message!
		} else {
			log.Printf("[memoryLoop] Inserted recalled memory into context table.")
		}
	}

	// Insert user's actual message as a "message" context
	_, err = memoryDb.Exec(`INSERT INTO contexts (channel_id, author_id, username, content, time, type)
			VALUES (?, ?, ?, ?, ?, ?)`,
		req.ChannelID, req.Message.AuthorID, req.Message.Username, req.Message.Content, req.Message.Time, "message")
	if err != nil {
		log.Printf("[memoryLoop] Failed to insert user message context: %v", err)
	}
	log.Printf("[memoryLoop] Inserted user message into context table.")
}
