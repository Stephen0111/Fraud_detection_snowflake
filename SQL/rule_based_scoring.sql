ALTER TABLE RAW.TRANSACTIONS ADD COLUMN FraudScore INT;
ALTER TABLE RAW.TRANSACTIONS ADD COLUMN FraudLabel INT;

UPDATE RAW.TRANSACTIONS
SET FraudScore =
  IFF(TransactionAmount > PERCENTILE_CONT(0.98) WITHIN GROUP (ORDER BY TransactionAmount),1,0)
  + IFF(LoginAttempts >= 3,1,0)
  + IFF(TransactionAmount > AccountBalance,1,0);

UPDATE RAW.TRANSACTIONS
SET FraudLabel = IFF(FraudScore >= 3,1,0);
