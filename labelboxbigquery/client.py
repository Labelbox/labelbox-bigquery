from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind, DataRowMetadata
from labelboxbigquery import connector
from labelbase.metadata import sync_metadata_fields, get_metadata_schema_to_name_key
from labelbase.uploaders import batch_create_data_rows
from google.cloud import bigquery
from google.oauth2 import service_account


class Client:
    """ A LabelBigQuery Client, containing a Labelbox Client and BigQuery Client Object
    Args:
        lb_api_key                  :   Required (str) - Labelbox API Key
        google_key                  :   Required (dict) - Google Service Account Permissions dict, how to create one here: https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating
        google_project_name         :   Required (str) - Google Project ID / Name
        lb_endpoint                 :   Optinoal (bool) - Labelbox GraphQL endpoint
        lb_enable_experimental      :   Optional (bool) - If `True` enables experimental Labelbox SDK features
        lb_app_url                  :   Optional (str) - Labelbox web app URL
        
    Attributes:
        lb_client                   :   labelbox.Client object
        bq_client                   :   bigquery.Client object
        
    Key Functions:
        create_data_rows_from_table :   Creates Labelbox data rows (and metadata) given a BigQuery table
        create_table_from_dataset   :   Creates a BigQuery table given a Labelbox dataset
        upsert_table_metadata       :   Updates BigQuery table metadata columns given a Labelbox dataset
        upsert_labelbox_metadata    :   Updates Labelbox metadata given a BigQuery table
    """
    def __init__(
        self, 
        lb_api_key=None, 
        google_project_name=None,
        google_key=None,
        lb_endpoint='https://api.labelbox.com/graphql', 
        lb_enable_experimental=False, 
        lb_app_url="https://app.labelbox.com"):  

        self.lb_client = labelboxClient(lb_api_key, endpoint=lb_endpoint, enable_experimental=lb_enable_experimental, app_url=lb_app_url)
        bq_creds = service_account.Credentials.from_service_account_file(google_key) if google_key else None
        self.bq_client = bigquery.Client(project=google_project_name, credentials=bq_creds) 

    def create_data_rows_from_table(self, bq_table_id, lb_dataset, row_data_col, global_key_col=None, external_id_col=None, metadata_index={}, attachment_index={}, skip_duplicates=False):
        """ Creates Labelbox data rows given a BigQuery table and a Labelbox Dataset
        Args:
            bq_table_id         : Required (str) - BigQuery Table ID structured in the following format: "google_project_name.dataset_name.table_name"
            lb_dataset          : Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to            
            row_data_col        : Required (str) - Column name where the data row row data URL is located
            global_key_col      : Optional (str) - Column name where the data row global key is located - defaults to the row_data column
            external_id_col     : Optional (str) - Column name where the data row external ID is located - defaults to the row_data column
            metadata_index      : Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
            attachment_index    : Optional (dict) - Dictionary where {key=column_name : value=attachment_type}
                                    attachment_type must be one of "IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"
            skip_duplicates   : Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
        Returns:
            List of errors from data row upload - if successful, is an empty list
        """
        table = sync_metadata_fields(
            client=self.lb_client, table=table, get_columns_function=connector.get_columns_function, add_column_function=connector.add_column_function, 
            get_unique_values_function=connector.get_unique_values_function, metadata_index=metadata_index, verbose=verbose
        )        
        # If df returns False, the sync failed - terminate the upload
        if type(table) == bool:
            return {"upload_results" : [], "conversion_errors" : []}
        
        # Create a dictionary where {key=global_key : value=labelbox_upload_dictionary} - this is unique to Pandas
        global_key_to_upload_dict, conversion_errors = connector.create_upload_dict(
            table=table, lb_client=self.lb_client, bq_client=self.bq_client,
            row_data_col=row_data_col, global_key_col=global_key_col, external_id_col=external_id_col, 
            metadata_index=metadata_index, local_files=local_files, divider=divider, verbose=verbose
        )
        # If there are conversion errors, let the user know; if there are no successful conversions, terminate the upload
        if conversion_errors:
            print(f'There were {len(conversion_errors)} errors in creating your upload list - see result["conversion_errors"] for more information')
            if global_key_to_upload_dict:
                print(f'Data row upload will continue')
            else:
                print(f'Data row upload will not continue')  
                return {"upload_results" : [], "conversion_errors" : errors}
            
        # Upload your data rows to Labelbox
        upload_results = batch_create_data_rows(
            client=self.lb_client, dataset=lb_dataset, global_key_to_upload_dict=global_key_to_upload_dict, 
            skip_duplicates=skip_duplicates, divider=divider, verbose=verbose
        )
        return {"upload_results" : upload_results, "conversion_errors" : conversion_errors}            
    

    def create_table_from_dataset(self, bq_dataset_id, bq_table_name, lb_dataset, metadata_index={}):
        """ Creates a BigQuery Table from a Labelbox dataset given a BigQuery Dataset ID, desired Table name, and optional metadata_index
        Args:
            bq_dataset_id   :   Required (str) - BigQuery Dataset ID structured in the following format: "google_project_name.dataset_name"
            bq_table_name   :   Required (str) - Desired BigQuery Table name
            lb_dataset      :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to
            metadata_index  :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"        
        Returns:
            If any, a list of errors from attempting to create BigQuery table rows from Labelbox data rows
        """
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        # Create dictionary where {key = metadata_field_name : value = metadata_schema_id}
        metadata_schema_to_name_key = self.__get_metadata_schema_to_name_key(lb_mdo, invert=False)
        # Construct your BigQuery Table Schema with data_row_id and row_data columns
        table_schema = [bigquery.SchemaField("data_row_id", "STRING", mode="REQUIRED"), bigquery.SchemaField("row_data", "STRING", mode="REQUIRED")]
        # If data rows have external IDs, add an external_id column
        data_row_export = list(lb_dataset.export_data_rows(include_metadata=True))
        if data_row_export[0].external_id:
            table_schema.append(bigquery.SchemaField("external_id", "STRING"))
        # If data rows have global_key IDs, add an global_key column            
        if data_row_export[0].global_key:
            table_schema.append(bigquery.SchemaField("global_key", "STRING"))
        if metadata_index:
            # For each key in the metadata_index, make a column
            for metadata_field_name in metadata_index.keys():
                mdf = metadata_field_name.replace(" ", "_")
                table_schema.append(bigquery.SchemaField(mdf, "STRING"))
        # Make your BigQuery table
        bq_table_name = bq_table_name.replace("-","_") # BigQuery tables shouldn't have "-" in them, as this causes errors when performing SQL updates
        bq_table = self.bq_client.create_table(bigquery.Table(f"{bq_dataset_id}.{bq_table_name}", schema=table_schema))
        # Add your data rows as table rows
        rows_to_insert = []
        for lb_data_row in data_row_export:
            row_dict = {"data_row_id" : lb_data_row.uid, "row_data" : lb_data_row.row_data}
            if lb_data_row.external_id:
                row_dict['external_id'] = lb_data_row.external_id
            if lb_data_row.global_key:
                row_dict['global_key'] = lb_data_row.global_key
            if metadata_index:
                field_to_value = {}
                if lb_data_row.metadata_fields:
                    for data_row_metadata in lb_data_row.metadata_fields:
                        if data_row_metadata['value'] in metadata_schema_to_name_key.keys():
                            field_to_value[data_row_metadata['name']] = metadata_schema_to_name_key[data_row_metadata['value']].split("///")[1]
                        else:
                            field_to_value[data_row_metadata['name']] = data_row_metadata['value']
                for metadata_field_name in metadata_index:
                    mdf = metadata_field_name.replace(" ", "_")
                    if metadata_field_name in field_to_value.keys():
                        row_dict[mdf] = field_to_value[metadata_field_name]
            rows_to_insert.append(row_dict)
        errors = self.bq_client.insert_rows_json(bq_table, rows_to_insert)
        if not errors:
            print(f'Success\nCreated BigQuery Table with ID {bq_table.table_id}')
        else:
            print(errors)
        return errors

    def upsert_table_metadata(self, bq_table_id, lb_dataset, global_key_col, metadata_index={}):
        """ Upserts a BigQuery Table based on the most recent metadata in Labelbox, only updates columns provided via a metadata_index keys
        Args:
            bq_table_id     :   Required (str) - BigQuery Table ID structured in the following format: "google_project_name.dataset_name.table_name"
            lb_dataset      :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to
            global_key_col  :   Required (str) - Global key column name to map Labelbox data rows to the BigQuery table rows
            metadata_index  :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"        
        Returns:
            Updated BigQuery table
        """
        # Sync metadata index keys with metadata ontology
        bq_table = sync_metadata_fields(
            client=self.lb_client, table=table, get_columns_function=connector.get_columns_function, add_column_function=connector.add_column_function, 
            get_unique_values_function=connector.get_unique_values_function, metadata_index=metadata_index, verbose=verbose
        )      
        bq_table = self.bq_client.get_table(bq_table_id)
        data_rows = lb_dataset.export_data_rows(include_metadata=True)
        metadata_schema_to_name_key = self.__get_metadata_schema_to_name_key(self.lb_client.get_data_row_metadata_ontology(), invert=False)
        # If a new metadata column needs to be made, make it
        if metadata_index:
            column_names = [schema_field.name.lower() for schema_field in bq_table.schema]
            for metadata_field_name in metadata_index.keys():
                mdf = metadata_field_name.lower().replace(" ", "_")
                if mdf not in column_names:
                    new_schema = bq_table.schema[:]
                    new_schema.append(bigquery.SchemaField(mdf, "STRING"))
                    bq_table.schema = new_schema
                    bq_table = self.bq_client.update_table(bq_table, ["schema"])
        # Make one SQL update per data row
        for data_row in data_rows:
            query = False
            query_str = f"UPDATE {bq_table_id}\nSET"
            field_to_value = {}
            if data_row.metadata_fields:
                for drm in data_row.metadata_fields:
                    mdf = drm['name'].lower().replace(" ", "_")
                    field_to_value[mdf] = metadata_schema_to_name_key[drm['value']].split("///")[1] if drm['value'] in metadata_schema_to_name_key.keys() else drm['value']
            if metadata_index:
                for metadata_field_name in metadata_index.keys():
                    mdf = metadata_field_name.lower().replace(" ", "_")
                    if mdf in field_to_value.keys():
                        query = True
                        query_str += f'\n   {mdf} = "{field_to_value[mdf]}",'     
            query_str = query_str[:-1]                
            if query:
                query_str += f'\nWHERE {global_key_col} = "{data_row.global_key}";'
                query_job = self.bq_client.query(query_str)
                query_job.result()
        print(f'Success')

    def upsert_labelbox_metadata(self, bq_table_id, global_key_col, global_keys_list=[], metadata_index={}):
        """ Updates Labelbox data row metadata based on the most recent metadata from a Databricks spark table, only updates metadata fields provided via a metadata_index keys
        Args:
            bq_table_id         :   Required (str) - BigQuery Table ID structured in the following format: "google_project_name.dataset_name.table_name"
            global_key_col      :   Required (str) - Global key column name to map Labelbox data rows to the BigQuery table rows
            global_keys_list    :   Optional (list) - List of global keys you wish to upsert - defaults to the whole table
            metadata_index      :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"        
        Returns:
            List of errors from metadata ontology bulk upsert - if successful, is an empty list
        """
        # Sync metadata index keys with metadata ontology
        check = self._sync_metadata_fields(bq_table_id, metadata_index)
        if not check:
          return None        
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        bq_table = self.bq_client.get_table(bq_table_id)
        metadata_schema_to_name_key = self.__get_metadata_schema_to_name_key(lb_mdo, invert=False)
        metadata_name_key_to_schema = self.__get_metadata_schema_to_name_key(lb_mdo, invert=True)        
        # Create a query to pull global key and metadata from BigQuery
        col_query = f"""{global_key_col}, """
        for metadata_field in list(metadata_index.keys()):
            col_query += metadata_field
        query_str = f"""SELECT {col_query} FROM {bq_table_id}"""            
        query_job = self.bq_client.query(query_str)
        query_job.result()          
        query_dict = {x[global_key_col] : x for x in query_job}
        # Either use global_keys provided or all the global keys in the provided global_key_col
        global_keys = global_keys_list if global_keys_list else list(query_dict.keys())
        # Grab data row IDs with global_key list
        data_row_ids = self.lb_client.get_data_row_ids_for_global_keys(global_keys)['results']
        drid_to_global_key = {data_row_ids[i]: global_keys[i] for i in range(len(global_keys))}
        # Get data row metadata with list of data row IDs
        data_row_metadata = lb_mdo.bulk_export(data_row_ids)
        upload_metadata = []  
        for data_row in data_row_metadata:
            drid = data_row.data_row_id
            new_metadata = data_row.fields[:]
            for field in new_metadata:
                field_name = metadata_schema_to_name_key[field.schema_id]
                if field_name in list(metadata_index.keys()):
                    ### Get the table value given a global key for each column in 
                    table_value = query_dict[drid_to_global_key[drid]][field_name]
                    name_key = f"{field_name}///{table_value}"
                    field.value = metadata_name_key_to_schema[name_key] if name_key in metadata_name_key_to_schema.keys() else table_value
            upload_metadata.append(DataRowMetadata(data_row_id=drid, fields=new_metadata))
        results = lb_mdo.bulk_upsert(upload_metadata)
        return results        
