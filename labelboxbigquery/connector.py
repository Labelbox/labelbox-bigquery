from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import Table
from google.cloud.bigquery import Client as bqClient

def get_columns_function(table:Table):
    """Grabs all column names from a Pandas DataFrame
    Args:
        table               :   Required (google.cloud.bigquery.table.Table) - BigQuery Table
    Returns:
        List of strings corresponding to all column names
    """
    return [schema_field.name for schema_field in table.schema]

def get_unique_values_function(table:Table, column_name:str, client:bqClient=None):
    """ Grabs all unique values from a spark table column as strings
    Args:
        table               :   Required (google.cloud.bigquery.table.Table) - BigQuery Table
        column_name         :   Required (str) - Spark Table column name
        client              :   Required (bigquery.Client) - BigQuery Client Object        
    Returns:
        List of unique values from a spark table column as strings
    """
    query_job = bq_client.query(f"""SELECT DISTINCT {column_name} FROM {table.table_id}""")
    return [row.values()[0] for row in query_job]

def add_column_function(table:Table, column_name:str, client:bqClient=None, default_value:str=""):
    """ Adds a column of empty values to an existing Pandas DataFrame
    Args:
        table               :   Required (google.cloud.bigquery.table.Table) - BigQuery Table
        column_name         :   Required (str) - Spark Table column name
        default_value       :   Optional (str) - Value to insert for every row in the newly created column
        client              :   Required (bigquery.Client) - BigQuery Client Object        
    Returns:
        Your table with a new column given the column_name and default_value  
    """
    original_schema = table.schema
    new_schema = original_schema[:]
    new_schema.append(SchemaField(column_name, "STRING"))
    table.schema = new_schema
    table = bq_client.update_table(table, ["schema"])   
    return table
