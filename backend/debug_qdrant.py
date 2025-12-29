# debug_qdrant.py
import os
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

QDRANT_URL = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "play_by_play_collection")

print(f"Connecting to Qdrant at: {QDRANT_URL}")
print(f"Collection name: {QDRANT_COLLECTION}")

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

# Check if collection exists
try:
    collections = client.get_collections().collections
    collection_names = [c.name for c in collections]
    print(f"\n✓ Available collections: {collection_names}")
    
    if QDRANT_COLLECTION in collection_names:
        print(f"✓ Collection '{QDRANT_COLLECTION}' exists")
        
        # Get collection info
        collection_info = client.get_collection(QDRANT_COLLECTION)
        print(f"\n📊 Collection Info:")
        print(f"  - Points count: {collection_info.points_count}")
        print(f"  - Vector size: {collection_info.config.params.vectors.size}")
        
        if collection_info.points_count == 0:
            print("\n⚠️  WARNING: Collection exists but has 0 points!")
            print("   The data hasn't been ingested yet.")
            print("   Make sure to:")
            print("   1. Start the replay streaming")
            print("   2. Wait for data to be ingested into Qdrant")
        else:
            # Try to retrieve some sample points
            print(f"\n📝 Sample points:")
            try:
                # Scroll through first few points
                scroll_result = client.scroll(
                    collection_name=QDRANT_COLLECTION,
                    limit=5,
                    with_payload=True,
                )
                
                for i, point in enumerate(scroll_result[0], 1):
                    payload = point.payload or {}
                    print(f"\n  Point {i}:")
                    print(f"    - match_id: {payload.get('match_id')}")
                    print(f"    - run_id: {payload.get('run_id')}")
                    print(f"    - minute: {payload.get('minute')}")
                    print(f"    - source: {payload.get('source')}")
                    print(f"    - text: {payload.get('text', '')[:50]}...")
                    
            except Exception as e:
                print(f"\n❌ Error retrieving sample points: {e}")
                
    else:
        print(f"\n❌ Collection '{QDRANT_COLLECTION}' does NOT exist!")
        print(f"   You need to create it first.")
        print(f"\n   To create the collection, run:")
        print(f"   ```python")
        print(f"   from qdrant_client.models import Distance, VectorParams")
        print(f"   client.create_collection(")
        print(f"       collection_name='{QDRANT_COLLECTION}',")
        print(f"       vectors_config=VectorParams(size=384, distance=Distance.COSINE)")
        print(f"   )")
        print(f"   ```")
        
except Exception as e:
    print(f"\n❌ Error connecting to Qdrant: {e}")
    import traceback
    traceback.print_exc()