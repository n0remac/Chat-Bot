package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/sashabaranov/go-openai"
)

func LoadCharacterSheet(path string) (*CharacterSheet, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cs CharacterSheet
	if err := json.Unmarshal(data, &cs); err != nil {
		return nil, err
	}
	return &cs, nil
}

func LoadOriginalWriting(path string) (string, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func buildSystemPrompt(cs *CharacterSheet, sampleWriting string) string {
	return fmt.Sprintf(
		"You are roleplaying as the following character in a fantasy setting.\n\n"+
			"Character Sheet:\n%s\n\n"+
			"Example Writing by This Character:\n%s\n\n"+
			"Respond *in character*, using their unique voice, style, and worldview. If you use catchphrases, mannerisms, or recurring themes, base them on the sheet and examples.",
		formatCharacterSheet(cs),
		truncate(sampleWriting, 3000), // truncate if huge
	)
}

func formatCharacterSheet(cs *CharacterSheet) string {
	out, _ := json.MarshalIndent(cs, "", "  ")
	return string(out)
}

func truncate(s string, max int) string {
	rs := []rune(s)
	if len(rs) > max {
		return string(rs[:max]) + "\n...[truncated]..."
	}
	return s
}

func ChatWith(cs *CharacterSheet, writing, userMessage string) (string, error) {
	systemPrompt := buildSystemPrompt(cs, writing)

	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	ctx := context.Background()

	messages := []openai.ChatCompletionMessage{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: userMessage},
	}

	resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:     "gpt-4.1-nano-2025-04-14",
		Messages:  messages,
		MaxTokens: 10000, // tune as desired
	})
	if err != nil {
		log.Fatalf("OpenAI request failed: %v", err)
	}

	return strings.TrimSpace(resp.Choices[0].Message.Content), nil
}

func Chat(csPath, writingPath, userMessage string) (string, error) {
	cs, err := LoadCharacterSheet(csPath)
	if err != nil {
		return "", fmt.Errorf("failed to load character sheet: %w", err)
	}

	writing, err := LoadOriginalWriting(writingPath)
	if err != nil {
		return "", fmt.Errorf("failed to load original writing: %w", err)
	}

	response, err := ChatWith(cs, writing, userMessage)
	if err != nil {
		return "", fmt.Errorf("chat failed: %w", err)
	}

	return response, nil

}


func ChatWithHistory(cs *CharacterSheet, writing, chatContext, userMessage string) (string, error) {
	systemPrompt := buildSystemPrompt(cs, writing)
	// Prepend chatContext to user message for context
	userPrompt := chatContext + userMessage

	client := openai.NewClient(os.Getenv("OPENAI_API_KEY"))
	ctx := context.Background()

	messages := []openai.ChatCompletionMessage{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: userPrompt},
	}

	resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:     "gpt-4.1-nano-2025-04-14",
		Messages:  messages,
		MaxTokens: 10000,
	})
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(resp.Choices[0].Message.Content), nil
}
