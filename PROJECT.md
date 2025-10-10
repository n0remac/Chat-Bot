# Chat-Bot Project

## Purpose

Chat-Bot is a Discord bot that enables users to interact with AI-powered fantasy roleplay characters derived from forum posts. The bot scrapes forum data, automatically generates detailed character sheets, and creates immersive in-character conversations using OpenAI's language models. Characters maintain long-term memory across conversations and recall relevant past interactions using semantic search.

## Architecture

### System Design

The application follows a concurrent, microservices-inspired architecture using Go goroutines for parallel processing:

```
┌─────────────────┐
│  Discord Bot    │
│  (discord.go)   │
└────────┬────────┘
         │
    ┌────┴────┬────────────┬────────────┐
    │         │            │            │
┌───▼───┐ ┌──▼────┐ ┌─────▼─────┐ ┌───▼────┐
│ Chat  │ │Memory │ │  Recall   │ │ Axis   │
│System │ │Process│ │  Process  │ │Evaluator│
└───┬───┘ └───┬───┘ └─────┬─────┘ └────────┘
    │         │            │
    │    ┌────▼────────────▼─────┐
    │    │   SQLite (memory.db)  │
    │    └───────────────────────┘
    │
    ├──────────┬────────────┐
    │          │            │
┌───▼───┐ ┌───▼────┐ ┌─────▼─────┐
│OpenAI │ │SQLite  │ │  Qdrant   │
│  API  │ │docs.db │ │Vector DB  │
└───────┘ └────────┘ └───────────┘
```

### Data Flow

1. **Forum Scraping**: Recursively walks forum directory structure, parses JSON post files, and bulk inserts into SQLite
2. **Character Generation**: Extracts character traits from posts using OpenAI function calling, merges into master character sheet
3. **Vector Embeddings**: Batch processes posts through OpenAI Embedding API, stores vectors in Qdrant for semantic search
4. **Chat Interaction**:
   - User sends message via Discord
   - Memory system logs message and updates conversation summary
   - Recall system performs semantic search for relevant character memories
   - Axis evaluators run in parallel to assess context
   - Chat system generates in-character response using character sheet, writing samples, and recalled memories
   - Response sent back to Discord

## Technical Stack

### Core Technologies

- **Language**: Go 1.24.2
- **Discord Integration**: discordgo v0.29.0
- **AI/ML**:
  - OpenAI GPT-4o (character generation, chat)
  - OpenAI GPT-4.1-nano (memory summarization)
  - text-embedding-3-large (semantic embeddings)
- **Vector Database**: Qdrant (via go-client v1.14.1)
- **Database**: SQLite 3 (via glebarez/go-sqlite and glebarez/sqlite)
- **Full-Text Search**: Bleve v1.0.14
- **JSON Processing**: tidwall/gjson v1.18.0

### Key Dependencies

```
github.com/bwmarrin/discordgo       - Discord API client
github.com/sashabaranov/go-openai   - OpenAI API client
github.com/qdrant/go-client         - Qdrant vector database client
github.com/glebarez/sqlite          - SQLite driver (CGO-free)
github.com/blevesearch/bleve        - Full-text search engine
```

## Major Components

### 1. Forum Scraper (`main.go`)

Recursively processes forum directory structure, parsing nested `posts` files containing JSON post data. Implements batch insertion with 1000-record transactions for optimal performance.

**Key Features**:
- Walks directory tree to find all post files
- Parses JSON with flexible timestamp handling
- Batch inserts with transaction management
- Thread path tracking for context

### 2. Character Generation System (`character.go`)

Automatically extracts character personality, traits, relationships, and voice from forum posts using OpenAI function calling.

**Process**:
1. Fetch all posts for a given username
2. Chunk posts (max 500K chars per chunk)
3. Extract character sheet from each chunk using `extract_character_sheet` function
4. Synthesize multiple character sheets into single master sheet
5. Select 5 best representative posts for writing samples

**Character Sheet Schema**:
```go
type CharacterSheet struct {
    Name                   string
    PersonalityTraits      []string
    Likes                  []string
    Dislikes               []string
    Fears                  []string
    Catchphrases           []string
    Skills                 []string
    Goals                  []string
    Affiliations           []string
    ImportantRelationships []map[string]string
}
```

### 3. Discord Bot (`discord.go`)

Handles Discord integration with command parsing and multi-character support.

**Commands**:
- `!create <username>` - Generate character sheet and writing samples
- `!switch <username>` - Switch to different character
- `!list` - Show all loaded characters
- `!posts` - Show post count for current character
- `!search <query>` - Semantic search across forum posts
- `!mode <chat|roleplay>` - Switch between response modes
- Direct messages or `!` prefix trigger bot interaction

### 4. Chat System (`chat.go`)

Generates in-character responses using character sheets, writing samples, and conversation history.

**Modes**:
- **Chat Mode**: Concise, clever responses suited for Discord chat (default)
- **Roleplay Mode**: Verbose, immersive responses with descriptive language

**System Prompt Components**:
- Character sheet (personality, traits, relationships)
- Example writing samples (up to 3000 chars)
- Long-term memory summary
- Recalled relevant posts
- Mode-specific instructions

### 5. Memory System (`memory.go`)

Maintains long-term conversation memory using automatic summarization.

**Architecture**:
- Asynchronous goroutine-based processor
- SQLite backend with `contexts` and `summaries` tables
- Incremental summarization (combines new messages with previous summary)
- Tracks up to 50 most recent messages per channel

**Flow**:
1. Messages logged to `contexts` table
2. Every update triggers summarization check
3. OpenAI generates updated summary combining old + new contexts
4. Summary stored with context IDs for traceability

### 6. Recall System (`recall.go`)

Performs semantic search to retrieve character-relevant memories based on user input.

**Process**:
1. Embed user input using text-embedding-3-large (3072 dimensions)
2. Query Qdrant vector database with character name filter
3. Return top 5 most semantically similar posts
4. Posts incorporated into chat context

**Advantages**:
- Character-specific memory filtering
- Semantic similarity vs keyword matching
- Fast vector search (<100ms typical)

### 7. Vector Database Management (`vectordb.go`)

Manages embedding generation and Qdrant collection lifecycle.

**Features**:
- Batch embedding via OpenAI Batch API (cheaper, async)
- Configurable batch sizes (default 500 posts)
- Batch job tracking in SQLite
- Point upsert to Qdrant with full payload
- Semantic search interface

**Payload Structure**:
```json
{
  "user": "username",
  "thread_id": "path/to/thread",
  "timestamp": 1234567890,
  "message": "post content",
  "post_id": "unique_id"
}
```

### 8. Axis Evaluation System (`axis.go`)

Modular framework for parallel evaluation of contextual factors influencing responses.

**Design Pattern**:
```go
type Axis interface {
    Run(ctx context.Context, input AxisInput) AxisOutput
    Name() string
}
```

**Current Implementations**:
- **RecallAxis**: Evaluates relevance of recalled memories

**Architecture**:
- Immediate axes run synchronously via goroutines
- Background processor supports async/scheduled evaluation
- Results can influence prompt construction or response selection

### 9. Batch Processing (`batches.go`)

Manages OpenAI Batch API integration for cost-effective embedding generation.

**Features**:
- Submit embedding batches (up to 24hr completion window)
- Track batch job status in SQLite
- List incomplete batches
- Load completed embeddings into Qdrant
- Dry-run mode for testing

### 10. Timeline & Summarization (`timeline.go`, `summarize.go`)

Generates chronological summaries and character timelines from forum posts.

**Use Cases**:
- Thread summarization for quick catch-up
- Character arc analysis
- Post history exploration

## Unique Features

### 1. Automatic Character Extraction

Unlike traditional chatbots with manually-written character definitions, this system automatically derives personality, voice, and traits from forum posts using AI analysis. This enables rapid character creation from existing content.

### 2. Dual Memory Architecture

Combines two complementary memory systems:
- **Long-term Memory**: Conversation summaries maintained per channel
- **Semantic Recall**: Vector search retrieves relevant character posts based on conversation context

This mimics human memory with both episodic (specific events) and semantic (general knowledge) components.

### 3. Best Posts Curation

Automatically selects most representative writing samples using AI evaluation, ensuring the character's unique voice is preserved in responses.

### 4. Mode-Based Response Styles

Adapts verbosity and tone based on context:
- **Chat Mode**: Discord-optimized short responses
- **Roleplay Mode**: Immersive narrative-style responses

### 5. Multi-Character Hot-Swapping

Users can dynamically switch between characters mid-conversation, with each character maintaining separate memory contexts.

### 6. Axis-Based Context Evaluation

Modular evaluation framework allows parallel assessment of multiple contextual factors (emotion, engagement, recall) that can influence response generation.

### 7. Cost-Optimized Batch Embeddings

Uses OpenAI Batch API for embedding generation (50% cost reduction) with job tracking and retry capabilities.

### 8. Forum-Aware Context

Preserves thread paths and timestamps, enabling context-aware responses that understand conversation lineage and temporal relationships.

## Usage

### Scrape Forum Data
```bash
./Chat-Bot -mode=scrape
```

### Generate Character
```bash
./Chat-Bot -mode=character -username="Character Name"
./Chat-Bot -mode=best -username="Character Name"
```

### Create Vector Embeddings
```bash
# Dry run (saves batches to files)
./Chat-Bot -mode=vector -dry-run

# Submit to OpenAI Batch API
./Chat-Bot -mode=vector

# Check batch status
./Chat-Bot -mode=batch-status

# Load completed embeddings
./Chat-Bot -mode=load-embeddings
```

### Run Discord Bot
```bash
export DISCORD_BOT_TOKEN="your-token"
export OPENAI_API_KEY="your-key"
./Chat-Bot -mode=discord
```

### Search Forum Posts
```bash
./Chat-Bot -mode=search -message="search query" -num=5
```

## Configuration

### Environment Variables
- `DISCORD_BOT_TOKEN` - Discord bot authentication token
- `OPENAI_API_KEY` - OpenAI API key

### Database Files
- `data/docs.db` - Forum posts and batch jobs
- `data/memory.db` - Conversation contexts and summaries

### Character Files
- `data/tfs/characters/*.json` - Character sheets
- `data/tfs/writing/*-best-posts.txt` - Writing samples

### Qdrant Configuration
- **Host**: localhost
- **Port**: 6334
- **Collection**: forum_posts
- **Vector Dimensions**: 3072 (text-embedding-3-large)

## Future Enhancements

- **Emotion/Engagement Axes**: Additional evaluators for emotional state and conversation engagement
- **Multi-Model Support**: Support for Anthropic Claude, local models
- **Web Interface**: Web-based character management dashboard
- **Advanced Memory**: Hierarchical summarization for very long conversations
- **Character Relationships**: Track and model inter-character dynamics
- **Voice Integration**: Text-to-speech for immersive audio roleplay
