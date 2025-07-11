import os
import pandas as pd
import numpy as np
from scipy.sparse import load_npz
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
import joblib
from sklearn.naive_bayes import MultinomialNB
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from scipy.sparse import load_npz, hstack, csr_matrix, vstack

# === CONFIGURATION ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
META_DIR = os.path.join(BASE_DIR, "data_proccesing", "meta_chunks")
TFIDF_DIR = os.path.join(BASE_DIR, "data_proccesing", "tfidf_chunks_2")
MODEL_OUTPUT_PATH = os.path.join(BASE_DIR, "model", "Pre_trained_model", "sentiment_model.pkl")
TRAIN_CHUNKS = [3, 34, 55]
TEST_CHUNKS = [13, 28, 60]
CLASSES = np.array([0, 1, 2])  # negative, neutral, positive

# === Initialize encoders ===
subreddit_encoder = LabelEncoder()

# === Fit encoders on all metadata first ===
print("Fitting encoders on metadata...")
scores = []

MAX_CHUNKS_FOR_ENCODERS = 64
chunk_id = 1
subreddits = set()
score_minmax = MinMaxScaler()

while chunk_id <= MAX_CHUNKS_FOR_ENCODERS:
    meta_path = os.path.join(META_DIR, f"meta_chunk_{chunk_id}.csv")
    if not os.path.exists(meta_path):
        break

    df = pd.read_csv(meta_path)
    subreddits.update(df["subreddit"].astype(str).unique())
    score_minmax.partial_fit(df[["score"]].fillna(0))

    chunk_id += 1

subreddit_encoder.fit(list(subreddits))

def partitial_training():
    # === Initialize model ===
    print("Initializing SGDClassifier with partial_fit...")
    model = SGDClassifier(loss="log_loss", max_iter=5, random_state=42)
    #model = MultinomialNB()
    chunk_id = 1
    n_samples = 0

    while True:
        chunk_path = os.path.join(TFIDF_DIR, f"X_chunk_{chunk_id}.npz")
        meta_path = os.path.join(META_DIR, f"meta_chunk_{chunk_id}.csv")

        if not os.path.exists(chunk_path) or not os.path.exists(meta_path):
            print("path wrong")
            break

        print(f"Processing chunk {chunk_id}...")
        X_text = load_npz(chunk_path)
        df = pd.read_csv(meta_path)
        y = df["label"].map({"negative": 0, "neutral": 1, "positive": 2})

        valid_mask = y.notnull()
        y = y[valid_mask].astype(int)

        # Prepare meta features
        subr = subreddit_encoder.transform(df["subreddit"].astype(str))[valid_mask]
        score = score_minmax.transform(df["score"].fillna(0).values.reshape(-1, 1))[valid_mask]
        contro = df["controversiality"].fillna(0).values.reshape(-1, 1)[valid_mask]

        X_meta = csr_matrix(np.hstack([contro]))
        X = hstack([X_text[valid_mask.values], X_meta])

        if chunk_id == 1:
            model.partial_fit(X, y, classes=CLASSES)
        else:
            model.partial_fit(X, y)

        n_samples += X.shape[0]
        print(f"Trained on chunk {chunk_id} ({X.shape[0]} samples)")
        chunk_id += 1

    print(f"Training complete. Total samples seen: {n_samples}")

    # === Save model ===
    os.makedirs(os.path.dirname(MODEL_OUTPUT_PATH), exist_ok=True)
    joblib.dump(model, MODEL_OUTPUT_PATH)
    print(f"Model saved to: {MODEL_OUTPUT_PATH}")

    # === Optional: Evaluate on last chunk ===
    print("Evaluating model on last chunk...")
    y_pred = model.predict(X)
    print(classification_report(y, y_pred, target_names=["negative", "neutral", "positive"]))

def log_regr():
    print("Loading and combining training chunks...")
    X_chunks = []
    y_all = []

    for chunk_id in TRAIN_CHUNKS:
        tfidf_path = os.path.join(TFIDF_DIR, f"X_chunk_{chunk_id}.npz")
        meta_path = os.path.join(META_DIR, f"meta_chunk_{chunk_id}.csv")
        if not os.path.exists(tfidf_path) or not os.path.exists(meta_path):
            continue

        print(f"  - Loading chunk {chunk_id}")
        X_text = load_npz(tfidf_path)
        df = pd.read_csv(meta_path)

        y = df["label"].map({"negative": 0, "neutral": 1, "positive": 2})
        valid_mask = y.notnull()
        y = y[valid_mask].astype(int)

        subr = subreddit_encoder.transform(df["subreddit"].astype(str).fillna("unknown"))[valid_mask]
        score = score_minmax.transform(df["score"].fillna(0).values.reshape(-1, 1))[valid_mask]
        contro = df["controversiality"].fillna(0).values.reshape(-1, 1)[valid_mask]

        X_meta = csr_matrix(np.hstack([score]))
        X_combined = hstack([X_text[valid_mask.values], X_meta])

        #X_chunks.append(X_combined)
        X_chunks.append(X_text[valid_mask.values])
        y_all.append(y)

    X_train = vstack(X_chunks)
    y_train = pd.concat(y_all, ignore_index=True)
    print(f"Training data shape: {X_train.shape}")

    # === Train logistic regression ===
    print("Training Logistic Regression model...")
    model = LogisticRegression(
        max_iter=300,
        multi_class='multinomial',
        solver='lbfgs'
    )
    model.fit(X_train, y_train)

    # === Load test chunks ===
    print("Evaluating on test chunks...")
    X_chunks_test = []
    y_all_test = []

    for chunk_id in TEST_CHUNKS:
        tfidf_path = os.path.join(TFIDF_DIR, f"X_chunk_{chunk_id}.npz")
        meta_path = os.path.join(META_DIR, f"meta_chunk_{chunk_id}.csv")
        if not os.path.exists(tfidf_path) or not os.path.exists(meta_path):
            continue

        print(f"  - Loading test chunk {chunk_id}")
        X_text = load_npz(tfidf_path)
        df = pd.read_csv(meta_path)

        y = df["label"].map({"negative": 0, "neutral": 1, "positive": 2})
        valid_mask = y.notnull()
        y = y[valid_mask].astype(int)

        subr = subreddit_encoder.transform(df["subreddit"].astype(str).fillna("unknown"))[valid_mask]
        score = score_minmax.transform(df["score"].fillna(0).values.reshape(-1, 1))[valid_mask]
        contro = df["controversiality"].fillna(0).values.reshape(-1, 1)[valid_mask]

        X_meta = csr_matrix(np.hstack([score]))
        X_combined = hstack([X_text[valid_mask.values], X_meta])

        #X_chunks_test.append(X_combined)
        X_chunks_test.append(X_text[valid_mask.values])
        y_all_test.append(y)

    X_test = vstack(X_chunks_test)
    y_test = pd.concat(y_all_test, ignore_index=True)
    print(f"Test data shape: {X_test.shape}")

    # === Evaluate ===
    y_pred = model.predict(X_test)
    print("Evaluation on test data:")
    print(classification_report(y_test, y_pred, target_names=["negative", "neutral", "positive"]))

    # === Save model ===
    os.makedirs(os.path.dirname(MODEL_OUTPUT_PATH), exist_ok=True)
    joblib.dump(model, MODEL_OUTPUT_PATH)
    print(f"Model saved to: {MODEL_OUTPUT_PATH}")


if __name__ == "__main__":
    log_regr()
#SGD
"""Evaluating model on last chunk...
              precision    recall  f1-score   support

    negative       0.84      0.33      0.47    145093
     neutral       0.70      0.81      0.75    195051
    positive       0.73      0.87      0.79    272440

    accuracy                           0.73    612584
   macro avg       0.75      0.67      0.67    612584
weighted avg       0.74      0.73      0.70    612584"""

#SGD with features: score, controversiality
"""Evaluating model on last chunk...
              precision    recall  f1-score   support

    negative       0.84      0.32      0.47    145093
     neutral       0.69      0.81      0.75    195051
    positive       0.72      0.87      0.79    272440

    accuracy                           0.72    612584
   macro avg       0.75      0.67      0.67    612584
weighted avg       0.74      0.72      0.70    612584
"""

#Multinomial
"""Evaluating model on last chunk...
              precision    recall  f1-score   support

    negative       0.79      0.60      0.68    145093
     neutral       0.77      0.65      0.70    195051
    positive       0.71      0.88      0.78    272440

    accuracy                           0.74    612584
   macro avg       0.76      0.71      0.72    612584
weighted avg       0.75      0.74      0.73    612584"""

#Multinomial with features: subreddit, score, controversiality
"""Evaluating model on last chunk...
              precision    recall  f1-score   support

    negative       0.76      0.50      0.61    145093
     neutral       0.62      0.69      0.65    195051
    positive       0.71      0.78      0.75    272440

    accuracy                           0.69    612584
   macro avg       0.70      0.66      0.67    612584
weighted avg       0.70      0.69      0.68    612584"""

#Multinomial with features: subreddit, score, controversiality with tfidf chunks 2
"""Evaluating model on last chunk...
              precision    recall  f1-score   support

    negative       0.76      0.51      0.61    145093
     neutral       0.63      0.69      0.66    195051
    positive       0.71      0.78      0.75    272440

    accuracy                           0.69    612584
   macro avg       0.70      0.66      0.67    612584
weighted avg       0.70      0.69      0.69    612584"""

#Multinomial with features: score, controversiality with tfidf chunks 2
"""Evaluating model on last chunk...
              precision    recall  f1-score   support

    negative       0.79      0.60      0.69    145093
     neutral       0.77      0.66      0.71    195051
    positive       0.71      0.88      0.79    272440

    accuracy                           0.74    612584
   macro avg       0.76      0.71      0.73    612584
weighted avg       0.75      0.74      0.74    612584"""

#Logistic Regression - fitting 3Mil dataset, without features, tfidf chunks 2
#and tfidf chunks same result
"""Evaluation on test data:
              precision    recall  f1-score   support

    negative       0.86      0.84      0.85    724191
     neutral       0.91      0.90      0.90    917448
    positive       0.91      0.93      0.92   1358361

    accuracy                           0.90   3000000
   macro avg       0.89      0.89      0.89   3000000
weighted avg       0.90      0.90      0.90   3000000"""
