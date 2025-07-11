"""
Reddit Comment Sentiment Labeling and Correlation Analysis

This script performs three main functions:

1. **Sentiment Labeling (main)**:
   - Reads a JSONL file with Reddit comments (`filtered_comments.jsonl`)
   - Uses VADER sentiment analysis to label each comment as positive, neutral, or negative
   - Saves labeled comments into `labeled_comments.csv`
   - Uses multiprocessing for parallel processing

2. **Threshold Testing (testing)**:
   - Explores how different VADER compound score thresholds affect sentiment label distribution

3. **Correlation Analysis**:
   - `compute_correlation()`: Correlation of sentiment label with `score` and `controversiality`
   - `compute_subreddit_correlation()`: Same as above but grouped by subreddit

Dependencies:
- vaderSentiment
- tqdm
- pandas
- multiprocessing
- csv
- json

Run Options:
- Uncomment the desired function in the `if __name__ == "__main__"` block.
[Created: 12.5.2025 by Sara Vesela]
"""
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tqdm import tqdm
import csv
from multiprocessing import Pool, cpu_count
import pandas as pd
import random

# === CONFIGURATION ===
INPUT_FILE = "filtered_comments.jsonl"
OUTPUT_FILE = "labeled_comments.csv"
MAX_ROWS = 100_000       # For testing or threshold analysis
BATCH_SIZE = 100_000     # Chunk size for multiprocessing

# Sentiment thresholds for analysis
THRESHOLDS = [0.05, 0.1, 0.2, 0.3]

# === SETUP ===
analyzer = SentimentIntensityAnalyzer()
output = []

def testing():
    """Analyze how label distribution varies with different sentiment thresholds."""
    results = {t: {"pos": 0, "neg": 0, "neu": 0} for t in THRESHOLDS}
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for i, line in enumerate(tqdm(f, desc="Analyzing comments")):
            if i >= MAX_ROWS:
                break
            try:
                comment = json.loads(line)
                text = comment["body"]
                score = analyzer.polarity_scores(text)["compound"]

                for t in THRESHOLDS:
                    if score >= t:
                        results[t]["pos"] += 1
                    elif score <= -t:
                        results[t]["neg"] += 1
                    else:
                        results[t]["neu"] += 1

            except Exception:
                continue
    print(f"\nSentiment distribution for {MAX_ROWS:,} comments:\n")
    for t in THRESHOLDS:
        total = sum(results[t].values())
        print(f"Threshold ±{t:.2f}")
        print(f"  Positive: {results[t]['pos']:,} ({results[t]['pos'] / total:.1%})")
        print(f"  Negative: {results[t]['neg']:,} ({results[t]['neg'] / total:.1%})")
        print(f"  Neutral : {results[t]['neu']:,} ({results[t]['neu'] / total:.1%})\n")

def compute_correlation():
    """Compute correlation between sentiment label and score/controversiality."""
    # Load first 1 million rows
    #df = pd.read_csv("labeled_comments.csv", nrows=1_000_000)
    # Load 1 milion random rows
    df = pd.read_csv("labeled_comments.csv", skiprows=lambda i: i > 0 and random.random() > 1_000_000/63_000_000, nrows=1_000_000)

    # Map labels to numeric values for correlation
    df["label_numeric"] = df["label"].map({
        "negative": -1,
        "neutral": 0,
        "positive": 1
    })

    # Compute correlation
    correlation = df[["label_numeric", "score", "controversiality"]].corr()
    print(correlation)

# Group by subreddit and compute correlation with label
def correlation_with_label(group):
    """Helper function: computes correlation matrix for a subreddit group."""
    corr = group[["label_numeric", "score", "controversiality"]].corr()
    return corr.loc["label_numeric", ["score", "controversiality"]]

def compute_subreddit_correlation():
    """Group data by subreddit and analyze label correlation with score/controversiality."""
    # Load a 1M-row sample for performance (you can load more if RAM allows)
    #df = pd.read_csv("labeled_comments.csv", nrows=10_000_000)
    df = pd.read_csv("labeled_comments.csv", skiprows=lambda i: i > 0 and random.random() > 10_000_000/63_000_000, nrows=1_000_000)

    # Map label to numeric
    df["label_numeric"] = df["label"].map({
        "negative": -1,
        "neutral": 0,
        "positive": 1
    })
    # Drop rows with missing subreddit or label_numeric
    df = df.dropna(subset=["subreddit", "label_numeric"])
    df["controversiality"] = pd.to_numeric(df["controversiality"], errors='coerce')
    # Add comment counts per subreddit
    subreddit_counts = df["subreddit"].value_counts()

    # Keep only subreddits with at least 100 comments
    valid_subreddits = subreddit_counts[subreddit_counts >= 100].index
    df_filtered = df[df["subreddit"].isin(valid_subreddits)]
    # Apply the function to each group
    subreddit_correlations = df_filtered.groupby("subreddit", group_keys=False).apply(correlation_with_label)

    # Rename for clarity
    subreddit_correlations.columns = ["label_vs_score", "label_vs_controversiality"]

    # Sort by correlation with score
    print("\nTop 10 subreddits where sentiment label correlates with score:\n")
    print(subreddit_correlations.sort_values("label_vs_score", ascending=False).head(10))

    print("\nTop 10 subreddits where sentiment label correlates negatively with score:\n")
    print(subreddit_correlations.sort_values("label_vs_score").head(10))

def label_comment(comment):
    """Label a comment using VADER sentiment analysis."""
    text = comment.get("body", "")
    if not text:
        return None

    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.1:
        label = "positive"
    elif score <= -0.1:
        label = "negative"
    else:
        label = "neutral"

    return {
        "id": comment.get("id"),
        "created_utc": comment.get("created_utc"),
        "subreddit": comment.get("subreddit"),
        "body": text,
        "label": label,
        "score": comment.get("score", 0),
        "controversiality": comment.get("controversiality", 0)
    }

def process_batch(lines):
    """Label all comments in a batch of lines."""
    batch = []
    for line in lines:
        try:
            comment = json.loads(line)
            labeled = label_comment(comment)
            if labeled:
                batch.append(labeled)
        except Exception:
            continue
    return batch

def chunkify(iterator, size):
    """Yield successive chunks from an iterator"""
    chunk = []
    for line in iterator:
        chunk.append(line)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def main():
    """Main function for labeling all comments and writing them to a CSV."""
    with open(INPUT_FILE, "r", encoding="utf-8") as infile, \
         open(OUTPUT_FILE, "w", encoding="utf-8", newline='') as out_csv:

        writer = csv.DictWriter(out_csv, fieldnames=["id", "created_utc", "subreddit", "body", "label", "score", "controversiality"])
        writer.writeheader()
        pool = Pool(cpu_count())  # Use all available CPU cores
        total_labeled = 0
        # tqdm over chunkified input
        for result in tqdm(pool.imap(process_batch, chunkify(infile, BATCH_SIZE)), desc="Labeling in parallel"):
            if result:
                writer.writerows(result)
                total_labeled += len(result)
                print(f"Saved {total_labeled:,} labeled comments")

        pool.close()
        pool.join()

    print(f"\nDone! Total labeled comments saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    # Uncomment the function you want to run:
    # testing()                      # Analyze threshold distributions
    # main()                         # Perform full labeling
    # compute_correlation()         # Global correlation analysis
    compute_subreddit_correlation()  # Correlation grouped by subreddit