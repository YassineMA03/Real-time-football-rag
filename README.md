# ⚽ Real-Time Football Match RAG System

A real-time football match streaming system with AI-powered chat using Kafka and Mistral AI.

## 🌐 Live Demo

**[Try it here!](https://frontend-production-6b8f.up.railway.app/)**

_Note: This demo link may expire. If it's not working, you can run the project locally following the setup instructions below._

## 🎯 What It Does

Stream football matches in real-time and chat with an AI assistant that knows what's happening in the game. Ask questions like "What's the score?" or "Who scored?" and get instant, context-aware answers.

## 🏗️ How It Works

```
Match Data (JSON files)
    ↓
Kafka Producer (replay_streamer.py) - Streams match events
    ↓
Kafka Topics (comments, events, scores)
    ↓
Kafka Consumer (kafka_context_store.py) - Tracks last 30 events
    ↓
RAG Engine (rag_engine_simple.py) - Reads context + asks Mistral AI
    ↓
You get smart answers! 🤖
```

## ✨ Features

- ⚡ **Real-time streaming** - Watch matches unfold minute by minute
- 🤖 **AI chatbot** - Ask questions about the match in natural language
- 🎮 **Multiple matches** - Stream several games at once
- ⏰ **Start anywhere** - Begin at any minute (e.g., 45'+2, 60')
- 📊 **Smart context** - AI knows the last 30 events (10 goals + 10 events + 10 comments)

## 🚀 Quick Start

### 1. Install Kafka

**macOS:**

```bash
brew install kafka
brew services start kafka
```

**Linux:**

```bash
# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka
bin/kafka-server-start.sh config/server.properties &
```

**Windows:**

```powershell
# Option 1: Use WSL2 (recommended)
# Install WSL2, then follow Linux instructions

# Option 2: Native Windows
# Download from https://kafka.apache.org/downloads
# Extract to C:\kafka
# Run from C:\kafka:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
# In new terminal:
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### 2. Setup Backend

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set Mistral API key (get yours at https://console.mistral.ai/)
export MISTRAL_API_KEY="your_key_here"  # On Windows: set MISTRAL_API_KEY=your_key_here

# Start backend
uvicorn backend.app:app --reload
```

Backend runs on **http://localhost:8000**

### 3. Setup Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs on **http://localhost:3000**

### 4. Use the App

1. Open **http://localhost:3000**
2. Select a match from "Match history"
3. Click **"Stream selected games"**
4. Click the match in "Currently streaming"
5. Ask questions in the chatbot!

## 💬 Example Questions

- "What's the score?"
- "Who scored?"
- "Any yellow cards?"
- "What happened in the last 5 minutes?"
- "Tell me about the goals"

## 📁 Project Structure

```
project/
├── backend/
│   ├── app.py                    # FastAPI server
│   ├── replay_streamer.py        # Kafka producer (streams events)
│   ├── kafka_context_store.py   # Kafka consumer (tracks context)
│   ├── rag_engine_simple.py     # AI chatbot with Mistral
│   └── runtime/                 # Generated context files
│       └── {game_id}/
│           └── rag_recap.txt    # Last 30 events (used by AI)
├── data/
│   └── games/
│       └── {game_id}/
│           ├── meta.json        # Match info
│           ├── events.json      # Goals, cards, subs
│           └── comments.json    # Commentary
├── frontend/
│   └── app/
│       └── page.tsx             # React UI
└── requirements.txt
```

## 🔧 Configuration

### Environment Variables

```bash
# Required
export MISTRAL_API_KEY="your_mistral_key"

# Optional (defaults shown)
export MISTRAL_MODEL="mistral-small-latest"
export KAFKA_BOOTSTRAP="localhost:9092"
```

## 📊 How the AI Works

### Context Window

The AI sees the **last 30 events**:

- **10 goals** with scorer info
- **10 events** (cards, subs, VAR)
- **10 comments** (commentary)

### Why Not Vector Search?

We use **simple file-based RAG** instead of vector databases because:

✅ **Recent context is what matters** - You care about the last 10 events, not searching through 500  
✅ **Simpler** - No embeddings, no Qdrant, no complexity  
✅ **Faster** - Direct file read (~1ms) vs vector search  
✅ **Cheaper** - No embedding API calls  
✅ **Perfect for live matches** - Chronological order, not semantic similarity

## 🎮 Advanced Usage

### Start at Specific Time

```bash
# Start at minute 45, extra time +2
{
  "games": [{
    "game_id": "chelsea_aston_villa_2025-12-27",
    "startAtMinute": 45,
    "startAtExtra": 2
  }]
}
```

### Stream Multiple Matches

```bash
# Stream 3 matches at once
{
  "games": [
    {"game_id": "match1", "startAtMinute": 0, "startAtExtra": 0},
    {"game_id": "match2", "startAtMinute": 45, "startAtExtra": 0},
    {"game_id": "match3", "startAtMinute": 60, "startAtExtra": 0}
  ]
}
```

## 🐛 Troubleshooting

### "Failed to load games"

**Check backend is running:**

```bash
curl http://localhost:8000/api/games
# Should return JSON with game list
```

**Check game data exists:**

```bash
ls -la data/games/
# Should show game folders with meta.json, events.json, comments.json
```

### "No context available"

**Wait for streaming to start** - Context builds as events stream

**Check Kafka is running:**

```bash
# Should show active brokers
kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### "Mistral API error"

**Check API key is set:**

```bash
echo $MISTRAL_API_KEY  # Should show your key
```

**Verify key is valid** at https://console.mistral.ai/

## 📝 API Reference

### `GET /api/games`

List available matches

### `POST /api/replay/start`

Start streaming match(es)

```json
{
  "games": [
    {
      "game_id": "match_id",
      "startAtMinute": 0,
      "startAtExtra": 0
    }
  ]
}
```

### `GET /api/replay/progress`

Get current match status

### `POST /api/chat`

Ask AI about match

```json
{
  "game_id": "match_id",
  "message": "What's the score?"
}
```

### `POST /api/replay/reset`

Stop all streams

## 🚢 Deploy to Railway

### Backend

1. Create new Railway project
2. Connect your GitHub repo
3. Set environment variables:
   - `MISTRAL_API_KEY` = your_key
   - `KAFKA_BOOTSTRAP` = (Railway provides this)
4. Deploy!

### Frontend

1. Create new Railway project
2. Connect your GitHub repo
3. Set environment variable:
   - `NEXT_PUBLIC_API_URL` = https://your-backend.up.railway.app
4. Deploy!

**Important:** Railway provides managed Kafka - you don't need to run it separately!

## 📦 Adding Match Data

Create a folder in `data/games/` with these files:

**meta.json:**

```json
{
  "game_id": "team1_team2_2026-01-01",
  "team_home": "Team 1",
  "team_away": "Team 2",
  "competition": "League Name",
  "date": "2026-01-01"
}
```

**events.json:**

```json
{
  "response": [
    {
      "time": { "elapsed": 23, "extra": null },
      "team": { "name": "Team 1" },
      "player": { "name": "John Doe" },
      "type": "Goal",
      "detail": "Normal Goal"
    }
  ]
}
```

**comments.json:**

```json
[
  {
    "minute": 1,
    "extra": 0,
    "type": "Commentary",
    "text": "Match kicks off!"
  }
]
```

## 🎓 Tech Stack

- **Backend:** FastAPI, Kafka, Mistral AI
- **Frontend:** Next.js, React, TypeScript
- **Streaming:** Apache Kafka
- **AI:** Mistral (mistral-small-latest)

## 🤝 Contributing

Contributions welcome! Ideas:

- [ ] Add player statistics
- [ ] Support more data sources
- [ ] Add match highlights
- [ ] Multi-language support
- [ ] Voice input/output
- [ ] Live score notifications

## 🙏 Credits

Built with:

- [Apache Kafka](https://kafka.apache.org/) - Event streaming
- [Mistral AI](https://mistral.ai/) - Language model
- [FastAPI](https://fastapi.tiangolo.com/) - Backend framework
- [Next.js](https://nextjs.org/) - Frontend framework

---

**Made for football fans**

Questions? Open an issue on GitHub!
