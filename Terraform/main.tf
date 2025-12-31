resource "snowflake_database" "fraud_db" {
  name = upper("${var.project_name}_${var.environment}_db")
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.fraud_db.name
  name     = "RAW"
}

resource "snowflake_warehouse" "ingest_wh" {
  name     = upper("${var.project_name}_${var.environment}_WH")
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}
