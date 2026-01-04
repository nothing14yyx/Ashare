import os
import sys
import json

def list_dir(path):
    try:
        items = os.listdir(path)
        print(json.dumps({"status": "success", "type": "directory", "items": items}, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False))

def read_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(content)
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(1)
    
    cmd = sys.argv[1]
    target_path = sys.argv[2]
    
    if cmd == "read":
        read_file(target_path)
    elif cmd == "list":
        list_dir(target_path)
