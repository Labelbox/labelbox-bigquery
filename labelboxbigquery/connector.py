from labelbox import Client
from labelbase.metadata import get_metadata_schema_to_name_key
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import Table
from google.cloud.bigquery import Client as bqClient

def create_upload_dict(table:Table, lb_client:Client, bq_client:bqClient, row_data_col:str, global_key_col:str="", 
                       external_id_col:str="", metadata_index:dict={}, attachment_index:dict={}local_files:bool=False, 
                       divider:str="///", verbose=False):
    """ Structures a query from your BigQuery table, then transforms the payload into an upload dictionary
    Args:
        table               :   Required (google.cloud.bigquery.table.Table) - BigQuery Table
        lb_client           :   Required (labelbox.client.Client) - Labelbox Client object
        bq_client           :   Required (bigquery.Client) - BigQuery Client object
        row_data_col        :   Required (str) - Column containing asset URL or file path
        global_key_col      :   Optional (str) - Column name containing the data row global key - defaults to row data
        external_id_col     :   Optional (str) - Column name containing the data row external ID - defaults to global key
        metadata_index      :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type}
                                metadata_type must be either "enum", "string", "datetime" or "number"
        attachment_index    :   Optional (dict) - Dictionary where {key=column_name : value=attachment_type}
                                attachment_type must be one of "IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"
        local_files         :   Optional (bool) - Determines how to handle row_data_col values
                                If True, treats row_data_col values as file paths uploads the local files to Labelbox
                                If False, treats row_data_col values as urls (assuming delegated access is set up)
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas
        verbose             :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information
    Returns:
        Two values:
        - global_key_to_upload_dict - Dictionary where {key=global_key : value=data row dictionary in upload format}
        - errors - List of dictionaries containing conversion error information; see connector.create_data_rows() for more information        
    """
    global_key_to_upload_dict = {}
    try:
        global_key_col = global_key_col if global_key_col else row_data_col
        external_id_col = external_id_col if external_id_col else global_key_col     
        if verbose:
            print(f'Creating upload list - {get_table_length_function(table)} rows in BigQuery Table')
        if get_table_length_function(table=table) != get_unique_values_function(table=table, column_name=global_key_col):
            print(f"Warning: Your global key column is not unique - upload will resume, only uploading 1 data row for duplicate global keys")
        metadata_name_key_to_schema = get_metadata_schema_to_name_key(client=lb_client, lb_mdo=False, divider=divider, invert=True)
        column_names = get_columns_function(table=table)
        if row_data_col not in column_names:
            raise ValueError(f'Error: No column matching provided "row_data_col" column value {row_data_col}')
        else:
            index_value = 0
            query_lookup = {row_data_col:index_value}
            col_query = row_data_col
            index_value += 1
        if global_key_col not in column_names:
            raise ValueError(f'Error: No column matching provided "global_key_col" column value {global_key_col}')
        else:
            col_query += f", {global_key_col}"
            query_lookup[global_key_col] = index_value
            index_value += 1
        if external_id_col not in column_names:
            raise ValueError(f'Error: No column matching provided "gloabl_key" column value {external_id_col}')
        else:
            col_query+= f", {external_id_col}"    
            query_lookup[external_id_col] = index_value            
            index_value += 1
        if metadata_index:
            for metadata_field_name in metadata_index:
                mdf = metadata_field_name.replace(" ", "_")
                if mdf not in column_names:
                    raise ValueError(f'Error: No column matching metadata_index key {metadata_field_name}')
                else:
                    col_query+=f', {mdf}'
                    query_lookup[mdf] = index_value
                    index_value += 1
        if attachment_index:
            for attachment_field_name in attachment_index:
                atf = attachment_field_name.replace(" ", "_")
                attachment_whitelist = ["IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"]
                if attachment_index[attachment_field_name] not in attachment_whitelist:
                    raise ValueError(f'Error: Invalid value for attachment_index key {attachment_field_name} : {attachment_index[attachment_field_name]}\n must be one of {attachment_whitelist}')
                if atf not in column_names:
                    raise ValueError(f'Error: No column matching attachment_index key {attachment_field_name}')
                else:
                    col_query+=f', {atf}'
                    query_lookup[atf] = index_value
                    index_value += 1
        # Query your row_data, external_id, global_key and metadata_index key columns from 
        query = f"""SELECT {col_query} FROM {table.project}.{table.dataset_id}.{table.table_id}"""
        query_job = bq_client.query(query)
        # Iterate over your query payload to construct a list of data row dictionaries in Labelbox format
        global_key_to_upload_dict = {}
        for row in query_job:
            data_row_upload_dict = {
                "row_data" : row[query_lookup[row_data_col]],
                "metadata_fields" : [{"schema_id":metadata_name_key_to_schema['lb_integration_source'],"value":"BigQuery"}],
                "global_key" : str(row[query_lookup[global_key_col]]),
                "external_id" : str(row[query_lookup[external_id_col]])
            }
            if metadata_index:
                for metadata_field_name in metadata_index:
                    mdf = metadata_field_name.replace(" ", "_")
                    metadata_schema_id = metadata_name_key_to_schema[metadata_field_name]
                    mdx_value = f"{metadata_field_name}///{row[query_lookup[mdf]]}"
                    if mdx_value in metadata_name_key_to_schema.keys():
                        metadata_value = metadata_name_key_to_schema[mdx_value]
                    else:
                        metadata_value = row[query_lookup[mdf]]
                    data_row_upload_dict['metadata_fields'].append({
                        "schema_id" : metadata_schema_id,
                        "value" : metadata_value
                    })
            if attachment_index:
                data_row_upload_dict['attachments'] = [{"type" : attachment_index[attachment_field_name], "value" : row[query_lookup[attachment_field_name]]} for attachment_field_name in attachment_index]
            global_key_to_upload_dict[row[query_lookup[global_key_col]]] = data_row_upload_dict
        errors = None
    except Exception as e:
        global_key_to_upload_dict = global_key_to_upload_dict if global_key_to_upload_dict else {}
        errors = e
    return global_key_to_upload_dict, errors

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

def get_table_length_function(table:Table):
    """ Tells you the size of a Pandas DataFrame
    Args:
        table               :   Required (google.cloud.bigquery.table.Table) - BigQuery Table
    Returns:
        The length of your table as an integer
    """  
    return int(table.num_rows)
