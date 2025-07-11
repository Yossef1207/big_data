"""
Reddit Comment Preprocessing and TF-IDF Feature Extraction
This script performs large-scale preprocessing of Reddit comments
and transforms them into TF-IDF feature vectors suitable for machine learning.

The pipeline consists of three main stages:

1. **Text Cleaning**:
   - Loads raw Reddit comments from `labeled_comments.csv`
   - Applies preprocessing: lowercasing, emoji conversion, slang expansion,
     removal of usernames, subreddits, URLs, punctuation, and tokenization
   - Saves cleaned comments into a plain text file (`cleaned_lines.txt`)
   - Also stores associated metadata into chunked CSV files (`meta_chunks/`)

2. **TF-IDF Vectorization**:
   - Loads cleaned text
   - Fits a `TfidfVectorizer` with unigrams and bigrams, filters rare terms
   - Transforms all cleaned comments into a sparse TF-IDF matrix

3. **Saving TF-IDF Matrix**:
   - Saves the full TF-IDF matrix into chunked `.npz` files
   - Each chunk corresponds to `CHUNK_SIZE` rows and is stored in `tfidf_chunks/`

Requirements:
- emoji
- pandas
- nltk
- scikit-learn
- scipy
- A custom `slang.py` file providing a `Slang.abbreviations` dictionary

Run Instructions:
- Place your raw comment file as `labeled_comments.csv` in the same directory
- Ensure `slang.py` exists and includes a slang mapping dictionary
- Run: `python this_script.py`
[Created: 15.5.2025 by Sara Vesela]
"""
import emoji
import pandas as pd
import re
import nltk
import joblib
import numpy as np
import time
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from slang import Slang
from sklearn.feature_extraction.text import TfidfVectorizer
import os
from scipy.sparse import save_npz

nltk.download('punkt_tab')
nltk.download('stopwords')

# === CONFIGURATION ===
INPUT_FILE = "labeled_comments.csv"       # Input CSV containing raw Reddit comments
TEXT_FILE = "cleaned_lines.txt"           # Output file for cleaned text lines
OUT_DIR = "tfidf_chunks_2"                  # Directory to store TF-IDF chunks
META_DIR = "meta_chunks"                  # Directory to store metadata chunks
CHUNK_SIZE = 1_000_000                    # Number of rows per chunk for processing
KEEP_PUNCTUATION = False                 # Reserved for future use

# Create output directories if they don't exist
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

# === Expand abbreviations ===
def expand_slang(text):
    """
        Replaces slang/abbreviations using the Slang.abbreviations dictionary.
        Args:
            text (str): Input text to process.
        Returns:
            str: Text with slang terms expanded.
        """
    words = text.split()
    expanded = [Slang.abbreviations.get(word.lower(), word) for word in words]
    return " ".join(expanded)

def clean_text(text):
    """
        Cleans the given comment text by:
        - Lowercasing
        - Removing emojis
        - Expanding slang
        - Removing usernames, subreddits, URLs, punctuation
        - Tokenizing into words
        Args:
            text (str): Raw Reddit comment.
        Returns:
            str: Cleaned text string.
        """
    try:
        text = str(text).lower()
        text = emoji.demojize(text, delimiters=(" ", " "))
        text = expand_slang(text)
        text = re.sub(r"/?u/\w+", "", text)
        text = re.sub(r"/?r/\w+", "", text)
        text = re.sub(r"http\S+|www.\S+", "", text)
        text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        tokens = word_tokenize(text)
        return " ".join(tokens)
    except Exception:
        return ""


def clean():
    with open(TEXT_FILE, "w", encoding="utf-8") as text_out:
        for i, chunk in enumerate(pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE)):
            print(f"Cleaning chunk {i + 1}")

            # Preprocess body
            chunk["cleaned_body"] = chunk["body"].astype(str).apply(clean_text)

            # Write cleaned text to file
            text_out.write("\n".join(chunk["cleaned_body"]) + "\n")

            # Write metadata (excluding raw body and cleaned text)
            meta = chunk[["id", "created_utc", "subreddit", "score", "controversiality", "label"]].copy()
            meta.to_csv(f"{META_DIR}/meta_chunk_{i + 1}.csv", index=False)

    print("\nDone saving cleaned text and metadata chunks.")

def vectorize():
    # === Fit TF-IDF on all text ===
    print("Fitting TF-IDF on full dataset...")
    with open(TEXT_FILE, encoding="utf-8") as f:
        all_lines = f.readlines()

    # Initialize TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2),          # Unigrams + bigrams
        min_df=5,                    # Ignore terms in < 5 docs
        max_features=50_000          # Limit vocab size
    )
    X_all = vectorizer.fit_transform(all_lines)
    print(f"TF-IDF matrix shape: {X_all.shape}")

    # Save the vectorizer
    joblib.dump(vectorizer, "tfidf_vectorizer.pkl")
    print("Saved vectorizer to tfidf_vectorizer.pkl")
    # === Save in chunks ===
    n_chunks = (X_all.shape[0] + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(n_chunks):
        start = i * CHUNK_SIZE
        end = min((i + 1) * CHUNK_SIZE, X_all.shape[0])
        save_npz(f"{OUT_DIR}/X_chunk_{i+1}.npz", X_all[start:end])
        print(f"Saved chunk {i+1} [{start}:{end}]")

    print("\nDone! All TF-IDF chunks saved.")

def fit_vectorizer(sample_size=1_000_000):
    print(f"Fitting vectorizer on first {sample_size} lines...")
    lines = []
    with open(TEXT_FILE, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= sample_size:
                break
            lines.append(line.strip())
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=10, max_features=25000, sublinear_tf=True)
    vectorizer.fit(lines)
    joblib.dump(vectorizer, "tfidf_vectorizer.pkl")
    print(f"Vectorizer fitted and saved to tfidf_vectorizer.pkl")


def transform_in_chunks(chunk_size=1_000_000):
    vectorizer = joblib.load("tfidf_vectorizer.pkl")
    os.makedirs("tfidf_chunks_", exist_ok=True)

    with open(TEXT_FILE, encoding="utf-8") as f:
        chunk = []
        chunk_id = 1
        for i, line in enumerate(f, 1):
            chunk.append(line.strip())
            if i % chunk_size == 0:
                print(f"Vectorizing chunk {chunk_id} ({len(chunk)} lines)")
                X = vectorizer.transform(chunk)
                save_npz(f"tfidf_chunks_2/X_chunk_{chunk_id}.npz", X)
                chunk = []
                chunk_id += 1

        # Final leftover chunk
        if chunk:
            print(f"Vectorizing final chunk {chunk_id} ({len(chunk)} lines)")
            X = vectorizer.transform(chunk)
            save_npz(f"tfidf_chunks_2/X_chunk_{chunk_id}.npz", X)

    print("All chunks vectorized and saved.")

if __name__ == "__main__":
    #clean()
    #vectorize()
    #fit_vectorizer(sample_size=1_000_000)
    transform_in_chunks(chunk_size=1_000_000)
