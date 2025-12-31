variable "snowflake_organization_name" { type = string }
variable "snowflake_account_name" { type = string }
variable "snowflake_user" { type = string }
variable "snowflake_password" { type = string, sensitive = true }
variable "snowflake_role" { type = string, default = "FRAUD_ENGINEER_ROLE" }
variable "project_name" { type = string, default = "fraud_detection" }
variable "environment" { type = string, default = "dev" }
