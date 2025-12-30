# ⚽ Football Match RAG System

A real-time football match assistant that streams live match data through Kafka and uses RAG (Retrieval-Augmented Generation) with Mistral AI to answer questions about ongoing matches.

## 🎯 Overview

This system simulates a live football streaming API and provides an intelligent chatbot that can answer questions about matches in real-time. It uses:

- **Kafka** for event streaming
- **Simple file-based RAG** for context retrieval
- **Mistral AI** for natural language responses
- **FastAPI** backend
- **Next.js** frontend

## 🏗️ Architecture

```
Match Data (JSON files)
    ↓
replay_streamer.py (Kafka Producer)
    ↓
Kafka Topics
    ├─> games.comments
    ├─> games.events
    └─> games.scores
    ↓
kafka_context_store.py (Kafka Consumer)
    ↓
Context Files (Last 10 of each type = 30 items total)
    ├─> rag_comments.txt
    ├─> rag_events.txt
    ├─> rag_scores.txt
    └─> rag_recap.txt (combined)
    ↓
rag_engine_simple.py (RAG)
    ↓
Mistral AI LLM
    ↓
User gets intelligent answers! 🤖
```

## 🔥 Kafka Architecture (Core Component)

Kafka is the **heart of this system** - it enables real-time streaming, scalability, and reliability.

### 📊 **System Components**

**Kafka Cluster:**
- Manages three topics: games.comments, games.events, games.scores
- Each topic has one partition for ordered message delivery
- Messages persist for 7 days (default retention)
- Coordinated by Zookeeper for metadata management

**Producer (replay_streamer.py):**
- Reads match data from JSON files
- Publishes messages to appropriate Kafka topics
- Each message contains: run_id, game_id, minute, extra, and event data
- Simulates real-time API by streaming chronologically

**Consumer (kafka_context_store.py):**
- Subscribes to all three Kafka topics
- Maintains rolling buffers (last 10 items per type)
- Automatically discards old messages when buffer is full
- Writes context to files in backend/runtime/

**Topics Structure:**
- **games.comments**: Match commentary and announcements
- **games.events**: Goals, cards, substitutions, VAR decisions
- **games.scores**: Score changes with goal details and scorers

### 🔄 **Message Flow**

**Step 1:** Producer reads match data and publishes to Kafka topics based on event type

**Step 2:** Kafka stores messages in topics with sequential offsets for ordering

**Step 3:** Consumer reads messages from all topics in order

**Step 4:** Consumer maintains three rolling buffers (deques with maxlen=10)

**Step 5:** Consumer writes four files per match:
- rag_comments.txt (10 latest comments)
- rag_events.txt (10 latest events)
- rag_scores.txt (10 latest goals)
- rag_recap.txt (all 30 combined - used by RAG)

**Step 6:** RAG engine reads rag_recap.txt when user asks a question

### ⚙️ **Key Configurations**

**Producer Settings:**
- Bootstrap server: localhost:9092
- Message serialization: JSON
- Acknowledgment level: Wait for leader
- Retry attempts: 3

**Consumer Settings:**
- Consumer group: rag-group
- Offset reset: earliest (reads from beginning)
- Auto-commit: enabled
- Session timeout: 30 seconds
- Max poll records: 500

**Context Store:**
- Buffer size: 10 items per type (comments, events, scores)
- Total context: 30 items maximum
- File updates: After each new message
- Directory structure: backend/runtime/{game_id}/

### 📈 **Architecture Benefits**

| Feature | Benefit |
|---------|---------|
| **Decoupling** | Producer and consumer run independently |
| **Scalability** | Can add multiple consumers for parallel processing |
| **Reliability** | Messages persist; can replay from any point in time |
| **Ordering** | Messages within partition maintain chronological order |
| **Multiple Consumers** | Easy to add analytics, logging, or monitoring |
| **Time Travel** | Can start streaming from any minute (e.g., 45' +2') |
| **Fault Tolerance** | If consumer crashes, it resumes from last committed offset |

### 🎯 **Why Kafka?**

**Event Streaming:** Perfect for sports data where events happen sequentially

**Message Persistence:** Can replay entire match from any point

**Consumer Groups:** Multiple services can read same data independently

**Offset Management:** Track where each consumer is in the stream

**Scalability:** Handle thousands of events per second across multiple matches

### 🔍 **Monitoring**

You can monitor Kafka health with built-in tools:

**Check active topics:** See what topics exist in the cluster

**View messages:** Read messages from any topic to verify data flow

**Monitor consumer groups:** Check if consumers are processing messages

**Check consumer lag:** See how far behind consumers are from latest messages

This helps debug issues like:
- Messages not being consumed
- Old run_id causing rejections
- Consumer offset reset problems
- Topic retention issues

## 📁 Project Structure

```
project/
├── backend/
│   ├── app.py                    # FastAPI application
│   ├── replay_streamer.py        # Kafka producer (simulates live API)
│   ├── kafka_context_store.py   # Kafka consumer (maintains context)
│   ├── rag_engine_simple.py     # RAG engine with Mistral AI
│   ├── runtime/                 # Generated context files
│   │   └── {game_id}/
│   │        ├── rag_comments.txt
│   │        ├── rag_events.txt
│   │        ├── rag_scores.txt
│   │        └── rag_recap.txt    # Combined (used by RAG)
├── data/
│   │   ├── events.json          # Match events
│   │   └── comments.json        # Match commentary
├── frontend/
│   └── app/
│       └── page.tsx             # Next.js UI
└── README.md
```

## ✨ Features

### Backend
- ✅ **Real-time streaming** simulation with Kafka
- ✅ **Tuple-based time ordering** (45' +1' < 45' +3' < 46')
- ✅ **Contextual LLM responses** with Mistral AI
- ✅ **Multiple match support** (stream multiple games simultaneously)
- ✅ **Start at any time** (e.g., 60')
- ✅ **Automatic context management** (last 30 items: 10 scores + 10 events + 10 comments)

### Frontend
- ✅ **Match selection** with custom start time (minute + extra)
- ✅ **Live match status** (score, time, status)
- ✅ **Per-match chatbot** with scrollable history
- ✅ **Auto-hide started matches** from history
- ✅ **Real-time updates** every second

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+
- Kafka
- Mistral API key

### 1. Install Kafka

**macOS (Homebrew):**
```bash
brew install kafka
```

**Linux (Manual):**
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0
```
**Windows:**
```powershell
# Download Kafka from https://kafka.apache.org/downloads
# Extract to C:\kafka

# Or use Chocolatey:
choco install kafka

# Or use WSL2 (recommended):
# Install WSL2 with Ubuntu, then follow Linux instructions above
```

### 2. Setup Backend

```bash
# Install Python dependencies
pip install fastapi uvicorn kafka-python mistralai python-multipart

# Set environment variables
export MISTRAL_API_KEY="your_mistral_api_key_here"
export MISTRAL_MODEL="mistral-small-latest"

# Or create .env file
echo "MISTRAL_API_KEY=your_mistral_api_key_here" > .env
echo "MISTRAL_MODEL=mistral-small-latest" >> .env
```

**Get Mistral API Key:** https://console.mistral.ai/

### 3. Setup Frontend

```bash
cd frontend
npm install
```

### 4. Start Services


**Terminal 1 - Kafka:**
```bash
kafka-server-start /usr/local/etc/kafka/server.properties
```

**Terminal 2 - Backend:**
```bash
uvicorn backend.app:app --reload
# Backend running on http://localhost:8000
```

**Terminal 3 - Frontend:**
```bash
cd frontend
npm run dev
# Frontend running on http://localhost:3000
```

### 5. Use the App

1. Open http://localhost:3000
2. Select a match from **Match history**
3. Set **Start minute** and **Extra** (e.g., 60)
4. Click **"Stream selected games"**
5. Match appears in **Currently streaming**
6. Click the match to activate chatbot
7. Ask questions like:
   - "What's the score?"
   - "Who scored?"
   - "Any yellow cards?"
   - "Tell me about the goals"

## 📊 How It Works


### Context Management

The system maintains a **rolling window** of the last 10 items of each type:

| Type | Count | Description |
|------|-------|-------------|
| Comments | max 10 | Latest commentary |
| Events |  max 10 | Recent match events (goals, cards, subs) |
| Scores | max 10 | Goal details with scorers |

**Total context: 30 items** (combined in `rag_recap.txt`)

### RAG Process

1. **User asks question** → "What's the score?"
2. **Read context** → `backend/runtime/{game_id}/rag_recap.txt`
3. **Build prompt** → Combine question + context
4. **Call Mistral** → Generate answer
5. **Return response** → "Nigeria is leading 2-0..."

## 🔧 Configuration

### Backend (`backend/app.py`)

```python
KAFKA_BOOTSTRAP = "localhost:9092"  # Kafka server
TICK_SEC_DEFAULT = 60               # Stream tick interval (seconds)
```

### Kafka Topics

```python
TOPICS = {
    "games.comments": "Match commentary",
    "games.events": "Match events (goals, cards, etc.)",
    "games.scores": "Score changes with goal details"
}
```

### Context Store (`backend/kafka_context_store.py`)

```python
top_k = 10  # Keep last 10 items per type
```

### RAG Engine (`backend/rag_engine_simple.py`)

```python
MISTRAL_MODEL = "mistral-small-latest"  # LLM model
max_tokens = 300                        # Response length
temperature = 0.2                       # Response creativity
```

## 🎮 Usage Examples

### Example 1: Start from Beginning

1. Select **Chelsea vs Aston Villa**
2. Leave **Start minute** = 0, **Extra** = 0
3. Click **Stream selected games**
4. Ask: "What happened so far?"

### Example 2: Start from Added Time

1. Select **Nigeria vs Tunisia**
2. Set **Start minute** = 45, **Extra** = 2
3. Click **Stream selected games**
4. Stream starts at 45' +2' (end of 1st half)

### Example 3: Multiple Matches

1. Select **multiple games**
2. Set different start times for each
3. Click **Stream selected games**
4. Switch between matches in chatbot

## 📝 API Endpoints

### `GET /api/games`
List available matches
```json
[
  {
    "game_id": "chelsea_aston_villa_2025-12-27",
    "team_home": "Chelsea",
    "team_away": "Aston Villa",
    "final_score": "3-0"
  }
]
```

### `POST /api/replay/start`
Start streaming match(es)
```json
{
  "games": [
    {
      "game_id": "chelsea_aston_villa_2025-12-27",
      "startAtMinute": 45,
      "startAtExtra": 1
    }
  ]
}
```

### `GET /api/replay/progress`
Get current match status
```json
{
  "run_id": "run_1735600000",
  "active_game_ids": ["chelsea_aston_villa_2025-12-27"],
  "progress": {
    "chelsea_aston_villa_2025-12-27": {
      "status": "streaming",
      "score_str": "2-0",
      "known_minute": 46,
      "known_extra": 0
    }
  }
}
```

### `POST /api/chat`
Ask question about match
```json
{
  "game_id": "chelsea_aston_villa_2025-12-27",
  "message": "What's the score?"
}
```

Response:
```json
{
  "answer": "Chelsea is currently leading 2-0 against Aston Villa...",
  "contexts": {
    "rag_recap": "[GAME 37' | 1st Half] SCORE: 1-0 | ⚽ Joao Pedro..."
  }
}
```

### `POST /api/replay/reset`
Stop all streams and reset

## 🔬 Technical Details

### Why No Vector Database?

This system uses **file-based RAG** instead of vector search because:

1. ✅ **Recent context is sufficient** - Users care about the last 10 events, not searching 500+
2. ✅ **Simpler architecture** - No Qdrant, no embeddings, no extra complexity
3. ✅ **Faster** - Direct file read vs vector similarity search
4. ✅ **Cheaper** - No embedding API calls
5. ✅ **Perfect for live matches** - Chronological context, not semantic search

### Why Kafka?

1. ✅ **Event streaming** - Natural fit for live sports data
2. ✅ **Scalability** - Can handle multiple matches, multiple consumers
3. ✅ **Reliability** - Messages persist, can replay from any point
4. ✅ **Decoupling** - Producer and consumers independent

### Context Window Strategy

Instead of vector search, we use a **rolling window**:

```python
# Keep last 10 of each type in deque
comments_buffer = deque(maxlen=10)  # Auto-discards old items
events_buffer = deque(maxlen=10)
scores_buffer = deque(maxlen=10)

# Combine all 30 items
combined = scores + events + comments
```

This gives the LLM **comprehensive recent context** without complexity.

## 📈 Performance

- **Context retrieval**: ~1ms (file read)
- **LLM response**: ~500-1000ms (Mistral API)
- **Total latency**: ~1 second per chat message
- **Memory usage**: Minimal (only stores last 30 items per match)
- **Kafka throughput**: Can handle 1000+ events/sec

## 🛡️ Security Notes

- **API Keys**: Never commit `.env` to git
- **CORS**: Configured for `localhost:3000` only
- **Validation**: All inputs validated with Pydantic
- **Rate Limiting**: Consider adding for production

## 🚢 Deployment

For production deployment:

1. **Use managed Kafka** (Confluent Cloud, AWS MSK)
2. **Add authentication** (JWT tokens)
3. **Enable HTTPS** (SSL certificates)
4. **Add monitoring** (Prometheus, Grafana)
5. **Scale horizontally** (multiple consumers)
6. **Add rate limiting** (prevent API abuse)
7. **Use production LLM** (Mistral API with higher limits)

## 📤 Git Best Practices

### **Before First Push**

**Create .gitignore file** in project root to exclude:
- Python cache files (__pycache__, *.pyc)
- Virtual environments (venv/, env/)
- Environment variables (.env, *.env)
- Runtime data (backend/runtime/)
- Kafka logs (/tmp/kafka-logs/)
- Frontend dependencies (node_modules/)
- IDE files (.vscode/, .idea/)
- API keys and credentials

**Critical files to NEVER commit:**
- .env files containing API keys
- backend/runtime/ directory
- Kafka data directories
- node_modules/
- Any file containing "api_key" in the name

**Always use environment variables** for sensitive data like API keys - never hardcode them in source files.

### **Initial Setup**

**1. Initialize repository:**
- Create .gitignore first
- Initialize git in project directory
- Add all files (respecting .gitignore)
- Make initial commit with descriptive message

**2. Create GitHub repository:**
- Use GitHub web interface or CLI
- Choose public or private
- Don't initialize with README (you have one)

**3. Connect and push:**
- Add GitHub as remote origin
- Push your main branch

### **Working with Git**

**Feature development workflow:**
- Create feature branch from main
- Make changes and commit regularly
- Push branch to GitHub
- Create Pull Request
- Merge after review
- Delete branch after merge

**Commit message format:**
- Use clear, descriptive messages
- Start with type: feat, fix, docs, style, refactor, test, chore
- Keep first line under 50 characters
- Add details in body if needed

**Good commit examples:**
- "feat: Add support for multiple simultaneous matches"
- "fix: Resolve Kafka consumer offset reset issue"
- "docs: Update README with Kafka architecture"

**Bad commit examples:**
- "updates"
- "fixed stuff"
- "changes"

### **Environment Variables**

**Create .env.example template** (safe to commit):
- Contains variable names with placeholder values
- Shows users what variables they need
- Never contains real API keys

**Users copy to .env and fill in real values:**
- Copy .env.example to .env
- Replace placeholders with actual keys
- .env stays local (never committed)

### **Security Checklist**

**Before every commit:**
- [ ] Run git status to see what will be committed
- [ ] Verify no .env file is staged
- [ ] Verify no API keys in code
- [ ] Check no runtime/ or node_modules/
- [ ] Ensure commit message is descriptive

**If you accidentally commit secrets:**
- Remove file from git history immediately
- Rotate the exposed API key
- Generate new key in Mistral Console
- Update local .env with new key

### **Repository Structure**

**Commit these:**
- Source code files (.py, .tsx)
- Configuration files (without secrets)
- Documentation (README.md)
- Dependencies (requirements.txt, package.json)
- .gitignore file
- .env.example template

**Never commit these:**
- .env files
- Runtime data
- Log files
- Cache files
- Dependencies (node_modules/)
- Build artifacts
- API keys or credentials

### **Large Files**

**For match data files over 50MB:**
- Use Git LFS (Large File Storage)
- Track large JSON files with LFS
- Prevents repository bloat

**Alternative for very large datasets:**
- Store externally (S3, Google Cloud)
- Document how to download in README
- Keep repository lightweight

### **Clean Repository**

**Before pushing:**
- Remove all runtime files
- Remove Kafka log directories
- Remove node_modules (users run npm install)
- Ensure .gitignore is working

**Maintainability:**
- Keep commits small and focused
- Write descriptive commit messages
- Use branches for features
- Review changes before committing
- Document major changes in README

### **Emergency: Exposed Secrets**

**If API key is committed:**

**Immediate actions:**
1. Go to Mistral Console immediately
2. Revoke the exposed API key
3. Generate new API key
4. Update local .env file

**Clean git history:**
1. Remove file from all commits (advanced Git operation)
2. Force push to rewrite history
3. Warn collaborators about force push

**Prevention:**
- Always check git status before committing
- Use .gitignore from the start
- Never hardcode API keys
- Use environment variables only

### **Collaboration Tips**

**Working with team:**
- Pull latest changes before starting work
- Use descriptive branch names (feature/add-stats)
- Keep commits atomic (one logical change)
- Write clear commit messages for teammates
- Review pull requests thoroughly

**Code review checklist:**
- No hardcoded secrets
- .gitignore is comprehensive
- Code is documented
- Tests pass
- No debugging code left in

**Branch naming:**
- feature/ - New features
- fix/ - Bug fixes
- docs/ - Documentation
- refactor/ - Code improvements
- test/ - Adding tests

This keeps your repository clean, secure, and professional! 🔒

## 🎓 Learning Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [FastAPI Tutorial](https://fastapi.tiangolo.com/tutorial/)
- [Mistral AI Docs](https://docs.mistral.ai/)
- [RAG Basics](https://www.pinecone.io/learn/retrieval-augmented-generation/)

## 🤝 Contributing

Contributions welcome! Areas for improvement:

- [ ] Add more match data sources
- [ ] Implement real-time score updates
- [ ] Add match statistics visualization
- [ ] Support multiple languages
- [ ] Add voice input/output
- [ ] Implement user authentication
- [ ] Add match highlights detection

## 🙏 Acknowledgments

- **Mistral AI** for the LLM API
- **Apache Kafka** for event streaming
- **FastAPI** for the backend framework
- **Next.js** for the frontend framework

---

**Built with ❤️ for football fans and AI enthusiasts**

For questions or issues, please open a GitHub issue.