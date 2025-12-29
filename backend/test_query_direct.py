# test_query_direct.py
import os
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "play_by_play_collection")
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "all-MiniLM-L6-v2")

print("🧪 Testing direct query for chelsea_aston_villa_2025-12-27\n")

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
embedder = SentenceTransformer(EMBED_MODEL_NAME)

# Query: "what is the score"
test_query = "what is the score"
match_id = "chelsea_aston_villa_2025-12-27"

qvec = embedder.encode(test_query).tolist()

print(f"Query: '{test_query}'")
print(f"Match ID: {match_id}\n")

# Test 1: Without run_id filter
print("=" * 70)
print("TEST 1: Query WITHOUT run_id filter")
print("=" * 70)

try:
    results = client.query_points(
        collection_name=QDRANT_COLLECTION,
        query=qvec,
        limit=10,
        with_payload=True,
        query_filter=Filter(must=[
            FieldCondition(key="match_id", match=MatchValue(value=match_id))
        ]),
    ).points
except AttributeError:
    results = client.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=qvec,
        limit=10,
        with_payload=True,
        query_filter=Filter(must=[
            FieldCondition(key="match_id", match=MatchValue(value=match_id))
        ]),
    )

print(f"\n✓ Found {len(results)} results\n")

if results:
    for i, hit in enumerate(results[:5], 1):
        p = hit.payload
        print(f"Result {i}:")
        print(f"  run_id: {p.get('run_id')}")
        print(f"  minute: {p.get('minute')}")
        print(f"  source: {p.get('source')}")
        print(f"  text: {p.get('text', '')[:80]}...")
        print()
else:
    print("❌ No results found!")
    print("\nThis means:")
    print("  - Either no data exists for this match_id")
    print("  - Or the data hasn't been ingested yet")

# Test 2: With a specific run_id from the data
print("\n" + "=" * 70)
print("TEST 2: Query WITH run_id filter")
print("=" * 70)

# Get the run_id from the first result
if results:
    test_run_id = results[0].payload.get('run_id')
    print(f"\nUsing run_id from data: {test_run_id}\n")
    
    try:
        results_with_run = client.query_points(
            collection_name=QDRANT_COLLECTION,
            query=qvec,
            limit=10,
            with_payload=True,
            query_filter=Filter(must=[
                FieldCondition(key="match_id", match=MatchValue(value=match_id)),
                FieldCondition(key="run_id", match=MatchValue(value=test_run_id)),
            ]),
        ).points
    except AttributeError:
        results_with_run = client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=qvec,
            limit=10,
            with_payload=True,
            query_filter=Filter(must=[
                FieldCondition(key="match_id", match=MatchValue(value=match_id)),
                FieldCondition(key="run_id", match=MatchValue(value=test_run_id)),
            ]),
        )
    
    print(f"✓ Found {len(results_with_run)} results with run_id filter\n")