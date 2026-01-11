# Bank Transaction Fraud Detection Platform  
*A Production-Grade Cloud Data Engineering & ML Pipeline for Fraud Analytics*

This project implements a **modular, scalable fraud detection data platform** built on **Snowflake**, **Terraform**, **dbt**, and **Python**. It ingests transactional data from banking systems, applies deterministic transformations and feature engineering, executes rule-based fraud scoring and machine learning models (Random Forest & Isolation Forest), and delivers curated datasets for dashboards and anomaly analytics.

The pipeline leverages **Snowpipe** for continuous ingestion, **Terraform** for infrastructure provisioning, **dbt** for transformation logic, and **Python** for model training and scoring.

---

## Architecture Overview
![Project Architecture](Assets/Snowflake.png)

         ┌───────────────────────────┐
         │  Raw Transaction Data      │
         │  (CSV / Bank APIs)         │
         └─────────────┬─────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │ Snowflake Staging Tables   │
         │  Raw ingestion via Snowpipe│
         └─────────────┬─────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │ dbt Transformations        │
         │  - Staging                 │
         │  - Feature Engineering     │
         │  - Fraud Scoring           │
         └─────────────┬─────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │ Analytical Tables / Marts │
         │  Ready for ML & Dashboards│
         └─────────────┬─────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │ ML Models                 │
         │  - Random Forest          │
         │  - Isolation Forest       │
         └─────────────┬─────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │ Dashboards & Reports      │
         │  Fraud Analytics / BI     │
         └───────────────────────────┘
                       │
                       ▼
         ┌───────────────────────────┐
         │ Terraform IaC             │
         │  Snowflake DB/Warehouse   │
         │  Roles/Grants             │
         └───────────────────────────┘

---

## Key Features

### Rule-Based Fraud Scoring  
Implements rule-based logic to detect suspicious patterns:
- High-value transactions (top 2%)
- Amount exceeding account balance
- Excessive login attempts
- Burst activity within short time windows
- Off-hours or weekend transactions  
Rules are aggregated into a **FraudLabel** for supervised ML training.

### Feature Engineering  
- `TransactionCountPerAccount`: frequency-based behavioral metric  
- `AmountOverBalance`: ratio-based risk indicator  
- `DaysSinceLastTransaction`: temporal anomaly detection  
- Categorical encoding and numeric scaling for ML compatibility

### Machine Learning  
- **Random Forest Classifier** for supervised fraud detection  
- **Isolation Forest** for unsupervised anomaly detection  
- Model outputs stored in fraud scoring tables for BI consumption

### Automated Ingestion & Transformation  
- **Snowpipe** enables continuous ingestion from CSVs or APIs  
- **dbt** modularizes transformations, feature engineering, and scoring logic  
- **Terraform** provisions Snowflake databases, warehouses, roles, and grants

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Cloud Warehouse** | Snowflake | Scalable data storage and processing |
| **IaC / Provisioning** | Terraform | Declarative infrastructure setup |
| **Transformations** | dbt | Modular SQL transformations and feature engineering |
| **ML & Analysis** | Python | Model training, scoring, and anomaly detection |
| **SQL** | Snowflake SQL | Querying, scoring logic, and data modeling |
| **Ingestion** | Snowpipe | Continuous data ingestion |
| **Visualization** | BI Tools (Tableau, Power BI) | Dashboards and fraud analytics |

---

## Output Tables

### **staging.transactions_raw**  
Raw transactional data ingested via Snowpipe.  
Schema includes:  
TransactionID, AccountID, TransactionAmount, TransactionDate, TransactionType, Location, DeviceID, IP Address, MerchantID, Channel, CustomerAge, CustomerOccupation, TransactionDuration, LoginAttempts, AccountBalance, PreviousTransactionDate

### **marts.transactions_features**  
Feature-engineered dataset for ML modeling.  
Includes:  
TransactionCountPerAccount, AmountOverBalance, DaysSinceLastTransaction

### **marts.transactions_fraud**  
Fraud scoring output table.  
Includes:  
TransactionID, AccountID, FraudScore, FraudLabel

---

## Workflow Summary

1. **Ingestion:** Raw transaction data ingested via Snowpipe into staging tables  
2. **Transformation:** dbt applies staging logic, feature engineering, and rule-based scoring  
3. **Modeling:** Python scripts train and apply Random Forest and Isolation Forest models  
4. **Analytics:** Outputs include feature importance, confusion matrix, ROC curve  
5. **Dashboards:** BI tools visualize fraud patterns and anomalies

---

## Project Screenshots

### **1. Terraform Commands**
- `terraform init`  
![Terraform Init](Assets/Screen3.png)  
- `terraform plan`  
![Terraform Plan](Assets/Screen2.png)  
![Terraform Plan](Assets/Screen1.png)

---

This platform delivers a **robust, production-grade fraud detection pipeline**, combining automated ingestion, modular transformations, ML scoring, and analytics — fully deployable via infrastructure-as-code.
