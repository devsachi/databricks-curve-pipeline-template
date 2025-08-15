output "metastore_id" {
  description = "ID of the created metastore"
  value       = databricks_metastore.this.id
}

output "catalog_name" {
  description = "Name of the created catalog"
  value       = databricks_catalog.curves.name
}

output "market_data_schema" {
  description = "Name of the market data schema"
  value       = databricks_schema.market_data.name
}

output "curve_outputs_schema" {
  description = "Name of the curve outputs schema"
  value       = databricks_schema.curve_outputs.name
}

output "market_data_location" {
  description = "External location for market data"
  value       = databricks_external_location.market_data.url
}

output "curve_outputs_location" {
  description = "External location for curve outputs"
  value       = databricks_external_location.curve_outputs.url
}
