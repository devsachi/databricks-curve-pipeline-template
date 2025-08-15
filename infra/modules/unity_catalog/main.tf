terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Metastore for Unity Catalog
resource "databricks_metastore" "this" {
  name          = var.metastore_name
  storage_root  = var.storage_root
  force_destroy = var.force_destroy
  owner         = var.owner
}

# Catalog for curve data
resource "databricks_catalog" "curves" {
  name              = "${var.environment}_curves"
  comment           = "Catalog for curve formation pipeline data"
  properties = {
    purpose = "financial_curves"
    environment = var.environment
  }
  depends_on = [databricks_metastore.this]
}

# Schema for market data
resource "databricks_schema" "market_data" {
  catalog_name = databricks_catalog.curves.name
  name         = "market_data"
  comment      = "Schema for raw market data inputs"
  properties = {
    data_type = "market_data"
    update_frequency = "daily"
  }
}

# Schema for curve outputs
resource "databricks_schema" "curve_outputs" {
  catalog_name = databricks_catalog.curves.name
  name         = "curve_outputs"
  comment      = "Schema for processed curve outputs"
  properties = {
    data_type = "curve_outputs"
    update_frequency = "daily"
  }
}

# External location for market data
resource "databricks_external_location" "market_data" {
  name            = "market_data_location"
  url             = "${var.storage_root}/market_data"
  credential_name = var.storage_credential
  comment         = "Location for market data files"
  depends_on      = [databricks_metastore.this]
}

# External location for curve outputs
resource "databricks_external_location" "curve_outputs" {
  name            = "curve_outputs_location"
  url             = "${var.storage_root}/curve_outputs"
  credential_name = var.storage_credential
  comment         = "Location for curve output files"
  depends_on      = [databricks_metastore.this]
}

# Table for market data
resource "databricks_table" "market_data" {
  catalog_name = databricks_catalog.curves.name
  schema_name  = databricks_schema.market_data.name
  name         = "market_data"
  table_type   = "MANAGED"
  
  column {
    name     = "trade_date"
    type     = "DATE"
    comment  = "Trade date for market data point"
  }
  column {
    name     = "asset_class"
    type     = "STRING"
    comment  = "Asset class (IR, FX, Credit, etc.)"
  }
  column {
    name     = "instrument_type"
    type     = "STRING"
    comment  = "Type of instrument"
  }
  column {
    name     = "tenor"
    type     = "STRING"
    comment  = "Tenor of the instrument"
  }
  column {
    name     = "value"
    type     = "DOUBLE"
    comment  = "Market data value"
  }
  column {
    name     = "source"
    type     = "STRING"
    comment  = "Data source identifier"
  }
  column {
    name     = "load_timestamp"
    type     = "TIMESTAMP"
    comment  = "Data load timestamp"
  }
  
  comment = "Market data inputs for curve construction"
}
resource "databricks_catalog" "curve_formation" {
  name    = "curve_formation_${var.environment}"
  comment = "Catalog for curve formation pipeline"
}

resource "databricks_schema" "curves" {
  catalog_name = databricks_catalog.curve_formation.name
  name         = "curves"
  comment      = "Schema for curve data"
}
