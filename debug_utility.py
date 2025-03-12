# debug_util.py
import json
import pprint


def dump_obj(obj, filename="debug_output.json"):
    """Save object to file for inspection"""
    with open(filename, 'w') as f:
        json.dump(obj, f, default=str, indent=2)
    print(f"Dumped object to {filename}")

    # Also print it
    print("\nObject structure:")
    pprint.pprint(obj, depth=5, width=100)