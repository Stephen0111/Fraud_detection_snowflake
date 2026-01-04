import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

df = pd.read_csv("../data/bank_transactions_data_2.csv")

# preprocessing
df['AmountOverBalance'] = df['TransactionAmount'] / df['AccountBalance']

features = ['TransactionAmount','AmountOverBalance','LoginAttempts']
X = df[features]
y = df['FraudLabel']

X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2)

clf = RandomForestClassifier(n_estimators=100)
clf.fit(X_train, y_train)
preds = clf.predict(X_test)

print(classification_report(y_test, preds))
