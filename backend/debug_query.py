# debug_query.py
import os
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
from qdrant_client.models import Filter, FieldCondition, MatchValue

QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "play_by_play_collection")
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "all-MiniLM-L6-v2")

print("🔍 Debugging RAG Query\n")

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
embedder = SentenceTransformer(EMBED_MODEL_NAME)

# First, let's see what match_ids and run_ids exist in the collection
print("📋 Checking existing data in Qdrant...\n")

try:
    scroll_result = client.scroll(
        collection_name=QDRANT_COLLECTION,
        limit=100,  # Get more points to see all match_ids
        with_payload=True,
    )
    
    match_ids = set()
    run_ids = set()
    
    print("Sample data points:")
    for i, point in enumerate(scroll_result[0][:10], 1):  # Show first 10
        payload = point.payload or {}
        match_id = payload.get('match_id')
        run_id = payload.get('run_id')
        match_ids.add(match_id)
        run_ids.add(run_id)
        
        print(f"\n  Point {i}:")
        print(f"    match_id: {match_id}")
        print(f"    run_id: {run_id}")
        print(f"    minute: {payload.get('minute')}")
        print(f"    source: {payload.get('source')}")
        print(f"    text: {payload.get('text', '')[:60]}...")
    
    print(f"\n\n📊 Summary of all data in collection:")
    print(f"  Unique match_ids: {match_ids}")
    print(f"  Unique run_ids: {run_ids}")
    
    # Now test a query with NO filters
    print(f"\n\n🔍 Testing query WITHOUT filters...")
    test_query = "what is the score"
    qvec = embedder.encode(test_query).tolist()
    
    try:
        results = client.query_points(
            collection_name=QDRANT_COLLECTION,
            query=qvec,
            limit=5,
            with_payload=True,
        ).points
    except AttributeError:
        results = client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=qvec,
            limit=5,
            with_payload=True,
        )
    
    print(f"  Found {len(results)} results without filters")
    
    # Now test with a filter for an existing match_id
    if match_ids:
        test_match_id = list(match_ids)[0]
        print(f"\n\n🔍 Testing query WITH filter (match_id={test_match_id})...")
        
        try:
            results_filtered = client.query_points(
                collection_name=QDRANT_COLLECTION,
                query=qvec,
                limit=5,
                with_payload=True,
                query_filter=Filter(must=[
                    FieldCondition(key="match_id", match=MatchValue(value=test_match_id))
                ]),
            ).points
        except AttributeError:
            results_filtered = client.search(
                collection_name=QDRANT_COLLECTION,
                query_vector=qvec,
                limit=5,
                with_payload=True,
                query_filter=Filter(must=[
                    FieldCondition(key="match_id", match=MatchValue(value=test_match_id))
                ]),
            )
        
        print(f"  Found {len(results_filtered)} results with match_id filter")
        
        if results_filtered:
            print(f"\n  Sample filtered result:")
            p = results_filtered[0].payload
            print(f"    match_id: {p.get('match_id')}")
            print(f"    minute: {p.get('minute')}")
            print(f"    text: {p.get('text', '')[:80]}...")
    
    print(f"\n\n💡 What to check:")
    print(f"  1. Make sure your frontend is using one of these match_ids: {match_ids}")
    print(f"  2. Check if your replay is streaming with the correct run_id")
    print(f"  3. Verify the /api/replay/progress endpoint returns the correct match_id and run_id")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()