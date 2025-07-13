package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bwmarrin/discordgo"
)

// Set these to your files (or make configurable)
// var characterSheetPath = "data/tfs/characters/naoki.json"
// var sampleWritingPath = "data/tfs/writing/empress-naoki-best-posts.txt"

var characterSheetPath = "data/tfs/characters/puck.json"
var sampleWritingPath = "data/tfs/writing/puck-best-posts.txt"

// Set your Discord bot token here or via environment
var discordToken = os.Getenv("DISCORD_BOT_TOKEN")

func StartDiscordBot() {
	if discordToken == "" {
		log.Fatalf("DISCORD_BOT_TOKEN not set")
	}

	dg, err := discordgo.New("Bot " + discordToken)
	if err != nil {
		log.Fatalf("error creating Discord session: %v", err)
	}

	dg.AddHandler(messageCreate)
	dg.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentsDirectMessages

	err = dg.Open()
	if err != nil {
		log.Fatalf("error opening Discord session: %v", err)
	}
	log.Println("Bot is now running. Press CTRL-C to exit.")
	select {} // Block forever
}

func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Ignore bot's own messages
	if m.Author.ID == s.State.User.ID {
		return
	}

	// You can change the prefix/trigger as desired
	const prefix = "!"

	isCommand := strings.HasPrefix(m.Content, prefix)
	isDM := m.GuildID == "" // direct message

	if isCommand || isDM {
		// Remove prefix if present
		userMsg := m.Content
		if isCommand {
			userMsg = strings.TrimSpace(m.Content[len(prefix):])
		}
		if userMsg == "" {
			s.ChannelMessageSend(m.ChannelID, "Please provide a message to chat with.")
			return
		}

		s.ChannelTyping(m.ChannelID) // Show "typing..."

		// Call your Chat logic
		resp, err := Chat(characterSheetPath, sampleWritingPath, userMsg)
		if err != nil {
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Error: %v", err))
			return
		}

		// Discord has a 2000 char/message limit
		for len(resp) > 0 {
			chunk := resp
			if len(chunk) > 1900 {
				chunk = chunk[:1900]
			}
			s.ChannelMessageSend(m.ChannelID, chunk)
			resp = resp[len(chunk):]
		}
	}
}

// func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
// 	if m.Author.ID == s.State.User.ID {
// 		return
// 	}

// 	const prefix = "!"
// 	isCommand := strings.HasPrefix(m.Content, prefix)
// 	isDM := m.GuildID == ""

// 	if isCommand || isDM {
// 		userMsg := m.Content
// 		if isCommand {
// 			userMsg = strings.TrimSpace(m.Content[len(prefix):])
// 		}
// 		if userMsg == "" {
// 			s.ChannelMessageSend(m.ChannelID, "Please provide a message to chat with.")
// 			return
// 		}

// 		s.ChannelTyping(m.ChannelID)

// 		// ---- Fetch last 10 hours of messages ----
// 		now := time.Now()
// 		tenHoursAgo := now.Add(-10 * time.Hour)

// 		// We'll fetch up to 100 messages (Discord API limit per call)
// 		messages, err := s.ChannelMessages(m.ChannelID, 100, "", "", "")
// 		if err != nil {
// 			s.ChannelMessageSend(m.ChannelID, "Error reading channel history.")
// 			return
// 		}

// 		var history []string
// 		for i := len(messages) - 1; i >= 0; i-- { // oldest to newest
// 			msg := messages[i]

// 			if msg.Timestamp.Before(tenHoursAgo) {
// 				continue
// 			}
// 			// Optionally skip bot messages
// 			if msg.Author.Bot {
// 				continue
// 			}
// 			username := msg.Author.Username
// 			content := msg.Content
// 			history = append(history, fmt.Sprintf("%s: %s", username, content))
// 		}

// 		// Format chat history as context
// 		chatContext := ""
// 		if len(history) > 0 {
// 			chatContext = "Recent channel conversation:\n" + strings.Join(history, "\n") + "\n\n"
// 		}

// 		cs, err := LoadCharacterSheet(characterSheetPath)
// 		if err != nil {
// 			fmt.Println("Error loading character sheet:", err)
// 			return
// 		}
// 		writing, err := LoadOriginalWriting(sampleWritingPath)
// 		if err != nil {
// 			fmt.Println("Error loading original writing:", err)
// 			return
// 		}

// 		resp, err := ChatWithHistory(cs, writing, chatContext, userMsg)
// 		if err != nil {
// 			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Error: %v", err))
// 			return
// 		}

// 		for len(resp) > 0 {
// 			chunk := resp
// 			if len(chunk) > 1900 {
// 				chunk = chunk[:1900]
// 			}
// 			s.ChannelMessageSend(m.ChannelID, chunk)
// 			resp = resp[len(chunk):]
// 		}
// 	}
// }
