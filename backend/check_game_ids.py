# check_game_ids.py
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "games"

print("🔍 Checking Game ID Conventions\n")
print("=" * 70)

if not DATA_DIR.exists():
    print(f"❌ Data directory not found: {DATA_DIR}")
    exit(1)

game_folders = sorted([f for f in DATA_DIR.iterdir() if f.is_dir()])

if not game_folders:
    print(f"❌ No game folders found in {DATA_DIR}")
    exit(1)

print(f"\n📁 Found {len(game_folders)} game folders:\n")

issues = []

for folder in game_folders:
    folder_name = folder.name
    meta_path = folder / "meta.json"
    
    print(f"Folder: {folder_name}")
    
    if not meta_path.exists():
        print(f"  ⚠️  No meta.json found")
        issues.append(f"{folder_name}: missing meta.json")
        continue
    
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        game_id_in_meta = meta.get("game_id", "NOT_FOUND")
        
        print(f"  meta.json game_id: {game_id_in_meta}")
        
        # Check if they match
        if game_id_in_meta != folder_name:
            print(f"  ❌ MISMATCH! Folder name ≠ game_id in meta.json")
            issues.append(f"{folder_name}: game_id in meta.json is '{game_id_in_meta}'")
        else:
            print(f"  ✓ Match!")
        
        # Check what files exist
        has_comments = (folder / "comments.json").exists() or (folder / "comments.jsonl").exists()
        has_events = (folder / "events.json").exists() or (folder / "events.jsonl").exists()
        
        print(f"  Files: comments={'✓' if has_comments else '✗'}, events={'✓' if has_events else '✗'}")
        
    except Exception as e:
        print(f"  ❌ Error reading meta.json: {e}")
        issues.append(f"{folder_name}: error reading meta.json - {e}")
    
    print()

print("=" * 70)

if issues:
    print(f"\n⚠️  Found {len(issues)} issues:\n")
    for issue in issues:
        print(f"  • {issue}")
    print("\n💡 Fix these by ensuring:")
    print("  1. Every game folder has a meta.json")
    print("  2. The 'game_id' in meta.json matches the folder name")
else:
    print("\n✓ All game IDs are consistent!")

print("\n" + "=" * 70)
print("\n📋 Summary of game_ids that should be used:")
print("\nIn your frontend, use one of these game_id values:")
for folder in game_folders:
    meta_path = folder / "meta.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            game_id = meta.get("game_id", folder.name)
            team_home = meta.get("team_home", "?")
            team_away = meta.get("team_away", "?")
            print(f"  • '{game_id}' ({team_home} vs {team_away})")
        except:
            print(f"  • '{folder.name}' (error reading meta)")