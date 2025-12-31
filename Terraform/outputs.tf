output "database_name" {
  value = snowflake_database.fraud_db.name
}

output "schema_name" {
  value = snowflake_schema.raw.name
}

output "warehouse_name" {
  value = snowflake_warehouse.ingest_wh.name
}
