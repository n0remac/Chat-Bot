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

func buildSystemPrompt(cs *CharacterSheet, sampleWriting, mode string) string {
	var prompt string
	switch mode {
	case "chat":
		prompt =
			"You are the following fantasy character.\n\n" +
				"Character Sheet:\n%s\n\n" +
				"Example Writing by This Character:\n%s\n\n" +
				"Respond *in character*, using their unique voice, style, and worldview. " +
				"Use catchphrases sparingly, only when appropriate.\n\n" +
				"You are responding as a Discord bot in chat mode. Most of your responses should be concise, clever, and directly relevant to the user's message. " +
				"If the user sends a long or emotionally deep message, you may respond with a few more sentences, but avoid long paragraphs unless absolutely necessary. " +
				"Keep things snappy and avoid long monologues. Use emotes or actions (e.g. *shrugs*) sparingly, as fits Discord chat."
	case "roleplay":
		prompt =
			"You are the following fantasy character.\n\n" +
				"Character Sheet:\n%s\n\n" +
				"Example Writing by This Character:\n%s\n\n" +
				"Respond *in character*, using their unique voice, style, and worldview. " +
				"Use catchphrases sparingly, only when appropriate.\n\n" +
				"You are roleplaying in a fantasy setting. Your responses can be more verbose and immersive, painting a scene or showing your character's thoughts and emotions. " +
				"Feel free to use descriptive language, actions, and internal monologue. " +
				"Longer messages are welcome if they contribute to the story or the character's development, but avoid purple prose unless it fits the character. " +
				"Stay in-character at all times, and interact with the user as if they are a part of the same world."
	default:
		// fallback to chat mode if unknown
		prompt =
			"You are the following fantasy character.\n\n" +
				"Character Sheet:\n%s\n\n" +
				"Example Writing by This Character:\n%s\n\n" +
				"Respond *in character*, using their unique voice, style, and worldview. " +
				"Use catchphrases sparingly, only when appropriate.\n\n" +
				"You are responding as a Discord bot, so keep responses concise and relevant to the user's message. " +
				"If the user sends a longer message you can respond in a longer way, but if they send a short message, keep your response short too."
	}

	return fmt.Sprintf(
		prompt,
		formatCharacterSheet(cs),
		truncate(sampleWriting, 3000), // truncate if huge
	)
}

func ChatWith(cs *CharacterSheet, writing, userMessage string, userId string) (string, error) {
	systemPrompt := buildSystemPrompt(cs, writing, userModes[userId])

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

	userModes["test"] = "chat" // Default mode for testing

	response, err := ChatWith(cs, writing, userMessage, "test")
	if err != nil {
		return "", fmt.Errorf("chat failed: %w", err)
	}

	return response, nil

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
