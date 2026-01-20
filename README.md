# Databricks Table Mover

A simple utility to move (clone) tables from one Unity Catalog schema to another.

## Usage

1. Clone this repository into your Databricks Workspace (Repos).
2. Open `notebooks/run_migration`.
3. Enter the **Source Schema** (e.g., `sandbox_dev.shipping`).
4. Enter the **Destination Schema** (e.g., `sandbox_prod.shipping`).
5. (Optional) Check `drop_source` to delete tables from the source after a successful move.
6. Run the notebook.

## Features

- **Unity Catalog Support**: Designed for `catalog.schema` paths.
- **Deep Clone**: Uses `DEEP CLONE` for efficient Delta table copying.
- **Failover**: Falls back to `CTAS` if cloning is not supported.
- **Safety**: `drop_source` is disabled by default.
