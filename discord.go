package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

// Set these to your files (or make configurable)
// var characterSheetPath = "data/tfs/characters/naoki.json"
// var sampleWritingPath = "data/tfs/writing/empress-naoki-best-posts.txt"

var characterSheetPath = "data/tfs/characters/puck.json"
var sampleWritingPath = "data/tfs/writing/puck-best-posts.txt"

// Set your Discord bot token here or via environment
var discordToken = os.Getenv("DISCORD_BOT_TOKEN")

var (
	// Map of username to CharacterSheet and sample writing
	loadedCharacters = make(map[string]*CharacterSheet)
	loadedWritings   = make(map[string]string)
	// Per-user currently selected character
	userCharacter = make(map[string]string)
	userModes     = make(map[string]string)
)

func StartDiscordBot() {
	StartMemory()
	StartRecall()
	LoadAllCharacters()
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
	if m.Author.ID == s.State.User.ID {
		return
	}
	const prefix = "!"

	UpdateMemory(m.ChannelID, userCharacter[m.Author.ID], m.Author.ID, m.Author.Username, m.Content, time.Now().Unix())

	isCommand := strings.HasPrefix(m.Content, prefix)
	isDM := m.GuildID == ""

	if !isCommand && !isDM {
		return
	}

	// Remove prefix if present
	userMsg := m.Content
	if isCommand {
		userMsg = strings.TrimSpace(m.Content[len(prefix):])
	}

	// Command parsing
	fields := strings.Fields(userMsg)
	if len(fields) == 0 {
		s.ChannelMessageSend(m.ChannelID, "Please provide a command or message.")
		return
	}

	// Otherwise, treat as a chat message
	username, ok := userCharacter[m.Author.ID]
	if !ok {
		username = "Empress Naoki"
		userCharacter[m.Author.ID] = username
	}
	mode := userModes[m.Author.ID]
	if mode == "" {
		mode = "chat" // Default mode if not set
	}
	cs := loadedCharacters[username]

	// Handle mode switching
	if fields[0] == "mode" && len(fields) > 1 {
		mode := strings.Join(fields[1:], " ")
		userModes[m.Author.ID] = mode
		s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Switched mode to '%s'.", mode))
		return
	}

	// Handle "!create <username>"
	if fields[0] == "create" && len(fields) > 1 {
		username := strings.Join(fields[1:], " ")
		fmt.Println(username)
		go func() { // Run in background to avoid blocking
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Creating character sheet and best posts for %s...", username))
			err := Charactar(username, false) // This writes to file
			if err != nil {
				s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Failed to create character: %v", err))
				return
			}
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Selecting posts for %s...", username))
			BestPosts(username, false) // This writes to file
			// Load the results
			csPath := fmt.Sprintf("data/tfs/characters/%s.json", strings.ToLower(strings.ReplaceAll(username, " ", "-")))
			writingPath := fmt.Sprintf("data/tfs/writing/%s-best-posts.txt", strings.ToLower(strings.ReplaceAll(username, " ", "-")))
			cs, err1 := LoadCharacterSheet(csPath)
			writing, err2 := LoadOriginalWriting(writingPath)
			if err1 != nil || err2 != nil {
				s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Failed to load character: %v %v", err1, err2))
				return
			}
			loadedCharacters[username] = cs
			loadedWritings[username] = writing
			userCharacter[m.Author.ID] = username // Set as current
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Character '%s' loaded and set as active!", username))
		}()
		return
	}

	// Handle "!switch <username>"
	if fields[0] == "switch" && len(fields) > 1 {
		username := strings.Join(fields[1:], " ")
		if _, ok := loadedCharacters[username]; !ok {
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Character '%s' not loaded. Use !create %s first.", username, username))
			return
		}
		userCharacter[m.Author.ID] = username
		s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Switched to character '%s'.", username))
		return
	}

	postDb, err := sql.Open("sqlite", "data/docs.db")
	if err != nil {
		log.Fatalf("failed to open postDb: %v", err)
	}

	if fields[0] == "posts" {
		username := userCharacter[m.Author.ID]
		posts, err := GetAllUserPosts(postDb, username)
		if err != nil {
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Error fetching posts: %v", err))
			return
		}
		s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Found %d posts for character '%s'.", len(posts), username))
		return
	}
	// Handle "!list" to show loaded characters
	if fields[0] == "list" {
		var names []string
		for name := range loadedCharacters {
			names = append(names, name)
		}
		if len(names) == 0 {
			s.ChannelMessageSend(m.ChannelID, "No characters loaded yet.")
		} else {
			s.ChannelMessageSend(m.ChannelID, "Loaded characters: "+strings.Join(names, ", "))
		}
		return
	}

	if fields[0] == "search" {
		query := strings.Join(fields[1:], " ")
		if query == "" {
			s.ChannelMessageSend(m.ChannelID, "Please provide a search query.")
			return
		}
		topK := 1 // Default number of results
		results, err := SearchForumPosts(query, topK)
		if err != nil {
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Search error: %v", err))
			return
		}
		if results == "" {
			s.ChannelMessageSend(m.ChannelID, "No results found.")
		} else {
			fmt.Println(len(results))
			s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Search results:\n%s", results))
		}
		return
	}

	// If the user sends just a character name (shortcut to switch)
	if len(fields) == 1 && loadedCharacters[fields[0]] != nil {
		userCharacter[m.Author.ID] = fields[0]
		s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Switched to character '%s'.", fields[0]))
		return
	}

	// take message from memoryReq.ReplyChan
	history := GetMemorySummary(m.ChannelID, username)

	// fmt.Println("History summary:", history.SummaryText)

	s.ChannelTyping(m.ChannelID)
	posts := RecallRelevantPosts(m.ChannelID, username, userMsg)
	strPosts := ""
	for _, post := range posts {
		strPosts += fmt.Sprintf("%s\n", post.Message)
	}
	resp, err := ChatWith(cs, strPosts, userMsg, m.ChannelID, history.SummaryText)
	if err != nil {
		s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Error: %v", err))
		return
	}

	// Discord 2000 char limit
	for len(resp) > 0 {
		chunk := resp
		if len(chunk) > 1900 {
			chunk = chunk[:1900]
		}
		fmt.Println("Sending chunk:", chunk)
		s.ChannelMessageSend(m.ChannelID, chunk)
		resp = resp[len(chunk):]
	}
}

func LoadAllCharacters() {
	files, err := filepath.Glob("data/tfs/characters/*.json")
	if err != nil {
		log.Printf("Error loading character files: %v", err)
		return
	}
	count := 0
	for _, csPath := range files {
		cs, err := LoadCharacterSheet(csPath)
		if err != nil {
			log.Printf("Failed to load character from %s: %v", csPath, err)
			continue
		}
		base := strings.TrimSuffix(filepath.Base(csPath), ".json")
		writingPath := filepath.Join("data/tfs/writing", base+"-best-posts.txt")
		writing, err := LoadOriginalWriting(writingPath)
		if err != nil {
			log.Printf("No writing found for %s: %v", base, err)
			writing = ""
		}
		// Use cs.Name as the key if it's unique, or base filename otherwise
		key := cs.Name
		if key == "" {
			key = base
		}
		loadedCharacters[key] = cs
		loadedWritings[key] = writing
		count++
	}
	log.Printf("Loaded %d character sheets.", count)
}
