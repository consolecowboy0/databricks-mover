import unittest
from unittest.mock import MagicMock, call
from databricks_mover.migrator import SchemaMigrator

class TestSchemaMigrator(unittest.TestCase):
    def setUp(self):
        self.mock_spark = MagicMock()
        self.source = "cat_dev.schema_src"
        self.dest = "cat_prod.schema_dest"
        self.migrator = SchemaMigrator(self.mock_spark, self.source, self.dest)

    def test_migrate_simple_flow(self):
        # Mock finding one table using a dictionary since code uses row['key']
        mock_table_row = {'tableName': 'my_table', 'isTemporary': False}
        
        mock_df_tables = MagicMock()
        mock_df_tables.collect.return_value = [mock_table_row]
        
        def side_effect(query):
            if "SHOW TABLES" in query:
                return mock_df_tables
            return MagicMock()

        self.mock_spark.sql.side_effect = side_effect

        self.migrator.migrate(drop_source=False)

        # Verification
        self.mock_spark.sql.assert_any_call(f"SHOW TABLES IN {self.source}")
        expected_query = f"CREATE TABLE IF NOT EXISTS {self.dest}.my_table DEEP CLONE {self.source}.my_table"
        self.mock_spark.sql.assert_any_call(expected_query)

    def test_migrate_fallback_ctas(self):
        mock_table_row = {'tableName': 'my_table', 'isTemporary': False}
        
        mock_df_tables = MagicMock()
        mock_df_tables.collect.return_value = [mock_table_row]

        def sql_side_effect(query):
            if "SHOW TABLES" in query:
                return mock_df_tables
            if "DEEP CLONE" in query:
                raise Exception("Clone not supported")
            return MagicMock()

        self.mock_spark.sql.side_effect = sql_side_effect

        self.migrator.migrate(drop_source=False)
        
        expected_query = f"CREATE TABLE IF NOT EXISTS {self.dest}.my_table AS SELECT * FROM {self.source}.my_table"
        self.mock_spark.sql.assert_any_call(expected_query)

    def test_drop_source(self):
        mock_table_row = {'tableName': 'my_table', 'isTemporary': False}
        
        mock_df_tables = MagicMock()
        mock_df_tables.collect.return_value = [mock_table_row]
        
        def sql_side_effect(query):
            if "SHOW TABLES" in query:
                return mock_df_tables
            return MagicMock()

        self.mock_spark.sql.side_effect = sql_side_effect
        
        self.migrator.migrate(drop_source=True)
        
        self.mock_spark.sql.assert_any_call(f"DROP TABLE {self.source}.my_table")

    def test_migrate_single_table(self):
        self.migrator.migrate_table('specific_table', drop_source=False)
        
        # Verify describe called
        self.mock_spark.sql.assert_any_call(f"DESCRIBE {self.source}.specific_table")
        
        # Verify logic
        expected_query = f"CREATE TABLE IF NOT EXISTS {self.dest}.specific_table DEEP CLONE {self.source}.specific_table"
        self.mock_spark.sql.assert_any_call(expected_query)

    def test_migrate_single_table_not_exists(self):
        def sql_side_effect(query):
            if "DESCRIBE" in query:
                raise Exception("Table not found")
            return MagicMock()
            
        self.mock_spark.sql.side_effect = sql_side_effect
        
        with self.assertRaises(Exception):
            self.migrator.migrate_table('non_existent', drop_source=False)

if __name__ == '__main__':
    unittest.main()
