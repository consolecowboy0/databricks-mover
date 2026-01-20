import logging

from tqdm import tqdm

class SchemaMigrator:
    def __init__(self, spark, source_catalog_schema, dest_catalog_schema):
        """
        Initialize the migrator.
        
        Args:
            spark: SparkSession object.
            source_catalog_schema (str): Source schema in 'catalog.schema' format.
            dest_catalog_schema (str): Destination schema in 'catalog.schema' format.
        """
        self.spark = spark
        self.source = source_catalog_schema
        self.dest = dest_catalog_schema
        self.logger = logging.getLogger("SchemaMigrator")
        
        # Extract schema name from source for prefixing
        if '.' in self.source:
            self.source_schema_name = self.source.split('.')[-1]
        else:
            self.source_schema_name = self.source
            
        # Basic setup
        logging.basicConfig(level=logging.INFO)

    def migrate(self, drop_source=False):
        """
        Migrate all tables from source to destination.
        """
        self.logger.info(f"Starting migration from {self.source} to {self.dest}")
        
        # List tables needs to handle Unity Catalog correctly
        # We can use spark.catalog.listTables(schema) or SQL
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {self.source}").collect()
        except Exception as e:
            self.logger.error(f"Failed to list tables in {self.source}: {e}")
            raise

        for row in tqdm(tables, desc="Migrating tables"):
            table_name = row['tableName']
            # Skip temporary views if any
            if row['isTemporary']:
                continue
                
            self._move_table(table_name, drop_source)

    def migrate_table(self, table_name, drop_source=False):
        """
        Migrate a single table from source to destination.
        """
        self.logger.info(f"Starting single table migration for {table_name}")
        
        # Verify table exists
        try:
            # Efficient check if table exists
            self.spark.sql(f"DESCRIBE {self.source}.{table_name}")
        except Exception:
            self.logger.error(f"Table {table_name} does not exist in {self.source}")
            raise

        self._move_table(table_name, drop_source)

    def _move_table(self, table_name, drop_source):
        src_table = f"{self.source}.{table_name}"
        
        # Prepend source schema name to destination table name
        dest_table_name = f"{self.source_schema_name}_{table_name}"
        dest_table = f"{self.dest}.{dest_table_name}"
        
        self.logger.info(f"Migrating table: {src_table} -> {dest_table}")

        try:
            # Try DEEP CLONE first
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {dest_table} DEEP CLONE {src_table}")
            self.logger.info(f"Successfully cloned {table_name}")
        except Exception as e:
            self.logger.warning(f"DEEP CLONE failed for {table_name}, falling back to CTAS. Error: {e}")
            try:
                # Fallback to CTAS
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {dest_table} AS SELECT * FROM {src_table}")
                self.logger.info(f"Successfully copied {table_name} via CTAS")
            except Exception as e2:
                self.logger.error(f"Failed to migrate {table_name}: {e2}")
                return # Do not drop source if move failed

        if drop_source:
            try:
                self.spark.sql(f"DROP TABLE {src_table}")
                self.logger.info(f"Dropped source table {src_table}")
            except Exception as e:
                self.logger.error(f"Failed to drop source table {src_table}: {e}")
