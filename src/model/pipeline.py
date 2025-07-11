import pandas as pd
import numpy as np
from scipy.sparse import load_npz, vstack
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import joblib



# 1. Load features (vectorized texts) from .npz files
print("Loading feature matrices...")
X1 = load_npz("../data_proccesing/data_comments/X_chunk_1.npz")
X2 = load_npz("../data_proccesing/data_comments/X_chunk_2.npz")
X = vstack([X1, X2])

# 2. Load meta-data (labels) from .csv files
print("Loading Meta-Data...")
df1 = pd.read_csv("../data_proccesing/data_comments/meta_chunk_1.csv")
df2 = pd.read_csv("../data_proccesing/data_comments/meta_chunk_2.csv")
df = pd.concat([df1, df2], ignore_index=True)

# 3. Convert text labels to numeric classes
print("Preparing Labels...")
y = df["label"].map({"negative": 0, "neutral": 1, "positive": 2})

# 4. Remove invalid rows (e.g., without label)
valid_mask = y.notnull()
X = X[valid_mask]
y = y[valid_mask]

# 5. Split into training and test data
print("Creating Trainings-/Test-Data...")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# 6. Train the model
print("Training the model...")
model = LogisticRegression(
    max_iter=300, #300
    multi_class='multinomial', 
    solver='lbfgs'
)
model.fit(X_train, y_train)

# 7. Model evaluation
print("Evaluation:")
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred, target_names=["negative", "neutral", "positive"]))

# 8. Save the model
print("Saving the model...")
joblib.dump(model, "Pre_trained_model/sentiment_model.pkl")
print(" Model saved as 'sentiment_model.pkl'")


""" last results for 300 epochs, 2 chunks
Evaluation:
              precision    recall  f1-score   support

    negative       0.85      0.82      0.84     94023
     neutral       0.90      0.90      0.90    124089
    positive       0.91      0.92      0.92    181888

    accuracy                           0.89    400000
   macro avg       0.89      0.88      0.88    400000
weighted avg       0.89      0.89      0.89    400000
"""