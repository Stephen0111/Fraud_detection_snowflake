import pandas as pd
from sklearn.ensemble import IsolationForest

df = pd.read_csv("../data/bank_transactions_data_2.csv")

features = ['TransactionAmount','TransactionDuration','LoginAttempts']
X = df[features]

clf = IsolationForest(contamination=0.02, random_state=42)
df['Anomaly'] = clf.fit_predict(X)

print(df[['TransactionID','Anomaly']].head())
