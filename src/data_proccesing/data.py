"""
Reddit Comment Filter Script

This script decompresses a Reddit comment dump (.zst),
filters comments based on timestamp, content, and field presence,
and saves the result as a JSON Lines (.jsonl) file.

Requirements:
- pip install zstandard

Usage:
- Set INPUT_FILE and OUTPUT_FILE paths.
- Run: python data.py

[Created: 10.5.2025 by Sara Vesela]
"""
import json
import zstandard as zstd  # pip install zstandard
from pathlib import Path
import io

# === CONFIGURATION ===
INPUT_FILE = "/home/sarah/Documents/RC_2019-04.zst"     # Input .zst file (compressed)
OUTPUT_FILE = "filtered_comments.jsonl"                 # Output file (.jsonl)
MIN_TIMESTAMP = 1554076800                              # Start of date range (UTC)
MAX_TIMESTAMP = 1555472130                              # End of date range (UTC)
MIN_BODY_LENGTH = 5                                     # Minimum comment length

# === KEYS TO KEEP ===
FIELDS_TO_KEEP = ["id", "author", "created_utc", "body", "score", "subreddit", "controversiality"]

# === FUNCTION ===
"""Check if the comment meets filtering criteria."""
def is_valid(comment):
    try:
        if not all(field in comment for field in FIELDS_TO_KEEP):
            return False
        if not (MIN_TIMESTAMP <= int(comment["created_utc"]) <= MAX_TIMESTAMP):
            return False
        if comment["body"].strip().lower() in ["[deleted]", "[removed]"]:
            return False
        if len(comment["body"].strip()) < MIN_BODY_LENGTH:
            return False
        return True
    except Exception:
        return False

# === MAIN ===
def main():
    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)

    count_total = 0
    count_kept = 0

    with open(output_path, "w", encoding="utf-8") as out_file:
        with open(input_path, "rb") as compressed:
            dctx = zstd.ZstdDecompressor()
            stream_reader = dctx.stream_reader(compressed)
            text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

            for line in text_stream:
                try:
                    #decoded = line.decode("utf-8")
                    comment = json.loads(line)
                    count_total += 1

                    if is_valid(comment):
                        filtered = {k: comment[k] for k in FIELDS_TO_KEEP}
                        out_file.write(json.dumps(filtered, ensure_ascii=False) + "\n")
                        count_kept += 1

                except Exception:
                    continue  # skip broken lines

                # Optional: show progress
                if count_total % 100000 == 0:
                    print(f"Processed: {count_total:,}, Kept: {count_kept:,}")

    print(f"\nDone! Total: {count_total:,}, Filtered: {count_kept:,} saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
