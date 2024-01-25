import labelbox
from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbase.metadata import sync_metadata_fields, get_metadata_schema_to_name_key
from labelbase.downloader import export_and_flatten_labels
from google.cloud import bigquery
from google.oauth2 import service_account
from uuid import uuid4
import pandas as pd
import labelpandas
from datetime import datetime
import ast

#TODO- remove
import sys

# BigQuery limits special characters that can be used in column names and they have to be unicode
DIVIDER_MAPPINGS = {'&' : '\u0026', '%' : '\u0025', '>' : '\u003E', '#' : '\u0023', '|' : '\u007c'}

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
        self.bq_creds = service_account.Credentials.from_service_account_file(google_key) if google_key else None
        self.bq_client = bigquery.Client(project=google_project_name, credentials=self.bq_creds)
        self.lp_client = labelpandas.Client(lb_api_key=lb_api_key)
        self.google_project_name = google_project_name












    def _validate_divider(self, divider):
        unicode_divider = ''
        for char in divider:
            if char not in DIVIDER_MAPPINGS:
                raise ValueError(f"Restricted character(s) found in divider - {char}. The allowed characters are {[key for key in DIVIDER_MAPPINGS.keys()]}")
            unicode_divider += DIVIDER_MAPPINGS[char]
        return unicode_divider

    def _sync_metadata_fields(self, bq_table_id, metadata_index={}):
        """ Ensures Labelbox's Metadata Ontology has all necessary metadata fields given a metadata_index
        Args:
            bq_table_id     :   Required (str) - Table ID structured in the following schema: google_project_name.dataset_name.table_name
            metadata_index  :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
        Returns:
            True if the sync is successful, False if not
        """
        # Get your metadata ontology
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        bq_table = self.bq_client.get_table(bq_table_id)
        # Convert your meatdata_index values from strings into labelbox.schema.data_row_metadata.DataRowMetadataKind types
        conversion = {"enum" : DataRowMetadataKind.enum, "string" : DataRowMetadataKind.string, "datetime" : DataRowMetadataKind.datetime, "number" : DataRowMetadataKind.number}
        # Grab all the metadata field names
        lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
        # Iterate over your metadata_index, if a metadata_index key is not an existing metadata_field, then create it in Labelbox
        if metadata_index:
            for column_name in metadata_index.keys():
                metadata_type = metadata_index[column_name]
                if metadata_type not in conversion.keys():
                    print(f'Error: Invalid value for metadata_index field {column_name}: {metadata_type}')
                    return False
                if column_name not in lb_metadata_names:
                    # For enum fields, grab all the unique values from that column as a list
                    if metadata_type == "enum":
                        query_job = self.bq_client.query(f"""SELECT DISTINCT {column_name} FROM {bq_table.table_id}""")
                        enum_options = [row.values()[0] for row in query_job]
                    else:
                        enum_options = []
                    lb_mdo.create_schema(name=column_name, kind=conversion[metadata_type], options=enum_options)
                    lb_mdo = self.lb_client.get_data_row_metadata_ontology()
                    lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
        # Iterate over your metadata_index, if a metadata_index key is not an existing column name, then create it in BigQuery  
        if metadata_index:
            column_names = [schema_field.name for schema_field in bq_table.schema]
            for metadata_field_name in metadata_index.keys():
                if metadata_field_name not in column_names:
                    original_schema = bq_table.schema
                    new_schema = original_schema[:]
                    new_schema.append(bigquery.SchemaField(metadata_field_name, "STRING"))
                    bq_table.schema = new_schema
                    bq_table = self.bq_client.update_table(bq_table, ["schema"])                   
        # Track data rows loaded from BigQuery
        if "lb_integration_source" not in lb_metadata_names:
            lb_mdo.create_schema(name="lb_integration_source", kind=DataRowMetadataKind.string)
        return True

    def __get_metadata_schema_to_name_key(self, lb_mdo:labelbox.schema.data_row_metadata.DataRowMetadataOntology, divider="///", invert=False):
        """ Creates a dictionary where {key=metadata_schema_id: value=metadata_name_key} 
        - name_key is name for all metadata fields, and for enum options, it is "parent_name{divider}child_name"
        Args:
            lb_mdo              :   Required (labelbox.schema.data_row_metadata.DataRowMetadataOntology) - Labelbox metadata ontology
            divider             :   Optional (str) - String separating parent and enum option metadata values
            invert              :   Optional (bool) - If True, will make the name_key the dictionary key and the schema_id the dictionary value
        Returns:
            Dictionary where {key=metadata_schema_id: value=metadata_name_key}
        """
        lb_metadata_dict = lb_mdo.reserved_by_name
        lb_metadata_dict.update(lb_mdo.custom_by_name)
        metadata_schema_to_name_key = {}
        for metadata_field_name in lb_metadata_dict:
            if type(lb_metadata_dict[metadata_field_name]) == dict:
                metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name][next(iter(lb_metadata_dict[metadata_field_name]))].parent] = str(metadata_field_name)
                for enum_option in lb_metadata_dict[metadata_field_name]:
                    metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name][enum_option].uid] = f"{str(metadata_field_name)}{str(divider)}{str(enum_option)}"
            else:
                metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name].uid] = str(metadata_field_name)
        return_value = metadata_schema_to_name_key if not invert else {v:k for k,v in metadata_schema_to_name_key.items()}
        return return_value
    
    def __batch_create_data_rows(self, client, dataset, global_key_to_upload_dict, skip_duplicates=True, batch_size=20000):
        """ Checks to make sure no duplicate global keys are uploaded before batch uploading data rows
        Args:
            client                      : Required (labelbox.client.Client) : Labelbox Client object
            dataset                     : Required (labelbox.dataset.Dataset) : Labelbox Dataset object
            global_key_to_upload_dict   : Required (dict) : Dictionary where {key=global_key : value=data_row_dict to-be-uploaded to Labelbox}
            skip_duplicates             : Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
            batch_size                  : Optional (int) : Upload batch size, 20,000 is recommended
        Returns:
            A concatenated list of upload results for all batch uploads
        """
        def __check_global_keys(client, global_keys):
            """ Checks if data rows exist for a set of global keys
            Args:
                client                  : Required (labelbox.client.Client) : Labelbox Client object
                global_keys             : Required (list(str)) : List of global key strings
            Returns:
                True if global keys are available, False if not
            """
            query_keys = [str(x) for x in global_keys]
            # Create a query job to get data row IDs given global keys
            query_str_1 = """query get_datarow_with_global_key($global_keys:[ID!]!){dataRowsForGlobalKeys(where:{ids:$global_keys}){jobId}}"""
            query_job_id = client.execute(query_str_1, {"global_keys":global_keys})['dataRowsForGlobalKeys']['jobId']
            # Get the results of this query job
            query_str_2 = """query get_job_result($job_id:ID!){dataRowsForGlobalKeysResult(jobId:{id:$job_id}){data{
                            accessDeniedGlobalKeys\ndeletedDataRowGlobalKeys\nfetchedDataRows{id}\nnotFoundGlobalKeys}jobStatus}}"""
            res = client.execute(query_str_2, {"job_id":query_job_id})['dataRowsForGlobalKeysResult']['data']
            return res
        global_keys_list = list(global_key_to_upload_dict.keys())
        payload = __check_global_keys(client, global_keys_list)
        loop_counter = 0
        if payload:
            while len(payload['notFoundGlobalKeys']) != len(global_keys_list):
                loop_counter += 1
                if payload['deletedDataRowGlobalKeys']:
                    client.clear_global_keys(payload['deletedDataRowGlobalKeys'])
                    payload = __check_global_keys(client, global_keys_list)
                    continue
                if payload['fetchedDataRows']:
                    for i in range(0, len(payload['fetchedDataRows'])):
                        if payload['fetchedDataRows'][i] != "":
                            if skip_duplicates:
                                global_key = str(global_keys_list[i])
                                del global_key_to_upload_dict[str(global_key)]
                            else:
                                global_key = str(global_keys_list[i])
                                new_upload_dict = global_key_to_upload_dict[str(global_key)]
                                del global_key_to_upload_dict[str(global_key)]
                                new_global_key = f"{global_key}_{loop_counter}"
                                new_upload_dict['global_key'] = new_global_key
                                global_key_to_upload_dict[new_global_key] = new_upload_dict
                    global_keys_list = list(global_key_to_upload_dict.keys())
                    payload = __check_global_keys(client, global_keys_list)
        upload_list = list(global_key_to_upload_dict.values())
        upload_results = []
        for i in range(0,len(upload_list),batch_size):
            batch = upload_list[i:] if i + batch_size >= len(upload_list) else upload_list[i:i+batch_size]
            task = dataset.create_data_rows(batch)
            errors = task.errors
            if errors:
                print(f'Data Row Creation Error: {errors}')
                return errors
            else:
                upload_results.extend(task.result)
        return upload_results   
    
    def export_to_BigQuery(self, project, bq_dataset_id:str, bq_table_name:str, create_table:bool=False,
                           include_metadata:bool=False, include_performance:bool=False, include_agreement:bool=False,
                           include_label_details:bool=False, verbose:bool=False, mask_method:str="png", divider="|||"):
        
        divider = self._validate_divider(divider)
        flattened_labels_dict = export_and_flatten_labels(
            client=self.lb_client, project=project, include_metadata=include_metadata, 
            include_performance=include_performance, include_agreement=include_agreement,
            include_label_details=include_label_details, mask_method=mask_method, verbose=verbose, divider=divider
        )

        #Make sure all 
        flattened_labels_dict = [{key: str(val) for key, val in dict.items()} for dict in flattened_labels_dict]
        print(flattened_labels_dict)

        for row in flattened_labels_dict:   
            row['global_key'] = str(uuid4())
        table = pd.DataFrame.from_dict(flattened_labels_dict)
        label_ids = table['label_id'].to_numpy()
        labels_str = ""
        for label_id in label_ids:
            labels_str += "'" + label_id + "',"
        labels_str = labels_str[:-1]
        columns = table.columns.values.tolist()
        table_schema = [bigquery.SchemaField(col, "STRING") for col in columns]
        bq_table_name = bq_table_name.replace("-","_") # BigQuery tables shouldn't have "-" in them, as this causes errors when performing SQL updates
        
        if create_table:
            bq_table = self.bq_client.create_table(bigquery.Table(f"{self.google_project_name}.{bq_dataset_id}.{bq_table_name}", schema=table_schema))
            labels_to_insert = flattened_labels_dict
        else:
            bq_table = self.bq_client.get_table(bigquery.Table(f"{self.google_project_name}.{bq_dataset_id}.{bq_table_name}"))
            query = """
                SELECT updated_at, label_id
                FROM {0}
                WHERE label_id in ({1})
            """
            query = query.format(f"{self.google_project_name}.{bq_dataset_id}.{bq_table_name}", labels_str)
            print(query)
            query_job = self.bq_client.query(query)  # API request
            rows = list(query_job.result())
            # rows = self.bq_client.query_and_wait(query)
            labels_to_update = []
            labels_to_insert = []
            for label in flattened_labels_dict:
                label_in_table = False
                for row in rows:
                    if label['label_id'] == row[1]:
                        label_in_table = True
                        row_time = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f%z")
                        label_time = datetime.strptime(label["updated_at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                        if label_time > row_time:
                            labels_to_update.append(label)
                if not label_in_table:
                    labels_to_insert.append(label)
            if len(labels_to_update) > 0:
                job_config = bigquery.LoadJobConfig(
                    schema=table_schema,
                    write_disposition="WRITE_TRUNCATE",
                )
                job = self.bq_client.load_table_from_json(
                    flattened_labels_dict, f"{self.google_project_name}.{bq_dataset_id}.{bq_table_name}", job_config=job_config
                )  # Make an API request.
                print(job.result().errors())
                return
        print(f"inserting {len(labels_to_insert)} data rows to table")
        errors = self.bq_client.insert_rows_json(bq_table, labels_to_insert)
        if not errors:
            print(f'Success\nCreated BigQuery Table with ID {bq_table.table_id}')
        else:
            print(errors)
        return errors

    def create_data_rows_from_table_lp(self, bq_credential_json, bq_table_id, dataset_id, project_id, model_id, model_run_id, row_data_col, global_key_col, external_id_col, metadata_index={}, attachment_index={},  divider="|||", upload_method="", skip_duplicates=False, mask_method:str="png", verbose:bool=False):
        bq_divider = self._validate_divider(divider)
        print(bq_divider)
        bq_table = self.bq_client.get_table(bq_table_id)
        column_names = [schema_field.name for schema_field in bq_table.schema]
        if row_data_col not in column_names:
            print(f'Error: No column matching provided "row_data_col" column value {row_data_col}')
            return None
        else:
            index_value = 0
            query_lookup = {row_data_col:index_value}
            col_query = row_data_col
            index_value += 1
        if global_key_col:
            if global_key_col not in column_names:
                 print(f'Error: No column matching provided "global_key_col" column value {global_key_col}')
                 return None
            else:
                col_query += f", {global_key_col}"
                query_lookup[global_key_col] = index_value
                index_value += 1
        else:
            print(f'No global_key_col provided, will default global_key_col to {row_data_col} column')
            global_key_col = row_data_col
            col_query += f", {global_key_col}"
            query_lookup[global_key_col] = index_value
            index_value += 1
        if external_id_col:
            if external_id_col not in column_names:
                print(f'Error: No column matching provided "gloabl_key" column value {external_id_col}')
                return None
            else:
                col_query+= f", {external_id_col}"    
                query_lookup[external_id_col] = index_value            
                index_value += 1    
        if metadata_index:
            for metadata_field_name in metadata_index:
                mdf = metadata_field_name.replace(" ", "_")
                if mdf not in column_names:
                    print(f'Error: No column matching metadata_index key {metadata_field_name}')
                    return None
                else:
                    col_query+=f', {mdf}'
                    query_lookup[mdf] = index_value
                    index_value += 1
        if attachment_index:
            attachment_whitelist = ["IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"]
            for attachment_field_name in attachment_index:
                atf = attachment_field_name.replace(" ", "_")
                if attachment_index[attachment_field_name] not in attachment_whitelist:
                    print(f'Error: Invalid value for attachment_index key {attachment_field_name} : {attachment_index[attachment_field_name]}\n must be one of {attachment_whitelist}')
                    return None
                if atf not in column_names:
                    print(f'Error: No column matching attachment_index key {attachment_field_name}')
                    return None
                else:
                    col_query+=f', {atf}'
                    query_lookup[atf] = index_value
                    index_value += 1
        for column in column_names:
            print(column)
            if divider in column:

                col_query+= f", `{column.replace(divider, bq_divider)}`"
                query_lookup[column] = index_value
                index_value += 1                
        # Query your row_data, external_id, global_key and metadata_index key columns from 
        query = f"""SELECT {col_query} FROM {bq_table.project}.{bq_table.dataset_id}.{bq_table.table_id}"""
        print(query)
        # query_job = self.bq_client.query(query)
        # credentials = service_account.Credentials.from_service_account_file(
        #     bq_credential_json,
        #     scopes=['https://www.googleapis.com/auth/cloud-platform'])
        df = pd.read_gbq(query, credentials=self.bq_creds)
        return_payload = self.lp_client.create_data_rows_from_table(df, dataset_id=dataset_id, project_id=project_id, priority=5, 
                                                                    upload_method=upload_method, skip_duplicates=skip_duplicates, mask_method=mask_method, verbose=verbose, divider=divider)
        return return_payload


    #TODO - update to use validate columns like in labelpandas
    def create_data_rows_from_table(
            self, bq_table_id:str="", lb_dataset:labelbox.schema.dataset.Dataset=None, row_data_col:str="", global_key_col:str=None, 
            external_id_col:str=None, metadata_index:dict={}, attachment_index:dict={}, skip_duplicates:bool=False, divider:str="|||"):
        """ Creates Labelbox data rows given a BigQuery table and a Labelbox Dataset
        Args:
            bq_table_id       : Required (str) - BigQuery Table ID structured in the following format: "google_project_name.dataset_name.table_name"
            lb_dataset        : Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to            
            row_data_col      : Required (str) - Column name where the data row row data URL is located
            global_key_col    : Optional (str) - Column name where the data row global key is located - defaults to the row_data column
            external_id_col   : Optional (str) - Column name where the data row external ID is located - defaults to the row_data column
            metadata_index    : Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
            attachment_index  : Optional (dict) - Dictionary where {key=column_name : value=attachment_type} - attachment_type must be one of "IMAGE", "VIDEO", "TEXT", "HTML"
            skip_duplicates   : Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
            divider           : Optional (str) - String delimiter for schema name keys and suffix added to duplocate global keys
        Returns:
            List of errors from data row upload - if successful, is an empty list
        """
        
        divider = self._validate_divider(divider)
        # Sync metadata index keys with metadata ontology
        check = self._sync_metadata_fields(bq_table_id, metadata_index)
        if not check:
          return None
        # Create a metadata_schema_dict where {key=metadata_field_name : value=metadata_schema_id}
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        metadata_name_key_to_schema = self.__get_metadata_schema_to_name_key(lb_mdo, invert=True)
        # Ensure your row_data, external_id, global_key and metadata_index keys are in your BigQery table, build your query
        bq_table = self.bq_client.get_table(bq_table_id)
        column_names = [schema_field.name for schema_field in bq_table.schema]
        if row_data_col not in column_names:
            print(f'Error: No column matching provided "row_data_col" column value {row_data_col}')
            return None
        else:
            index_value = 0
            query_lookup = {row_data_col:index_value}
            col_query = row_data_col
            index_value += 1
        if global_key_col:
            if global_key_col not in column_names:
                 print(f'Error: No column matching provided "global_key_col" column value {global_key_col}')
                 return None
            else:
                col_query += f", {global_key_col}"
                query_lookup[global_key_col] = index_value
                index_value += 1
        else:
            print(f'No global_key_col provided, will default global_key_col to {row_data_col} column')
            global_key_col = row_data_col
            col_query += f", {global_key_col}"
            query_lookup[global_key_col] = index_value
            index_value += 1
        if external_id_col:
            if external_id_col not in column_names:
                print(f'Error: No column matching provided "gloabl_key" column value {external_id_col}')
                return None
            else:
                col_query+= f", {external_id_col}"    
                query_lookup[external_id_col] = index_value            
                index_value += 1    
        if metadata_index:
            for metadata_field_name in metadata_index:
                mdf = metadata_field_name.replace(" ", "_")
                if mdf not in column_names:
                    print(f'Error: No column matching metadata_index key {metadata_field_name}')
                    return None
                else:
                    col_query+=f', {mdf}'
                    query_lookup[mdf] = index_value
                    index_value += 1
        if attachment_index:
            attachment_whitelist = ["IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"]
            for attachment_field_name in attachment_index:
                atf = attachment_field_name.replace(" ", "_")
                if attachment_index[attachment_field_name] not in attachment_whitelist:
                    print(f'Error: Invalid value for attachment_index key {attachment_field_name} : {attachment_index[attachment_field_name]}\n must be one of {attachment_whitelist}')
                    return None
                if atf not in column_names:
                    print(f'Error: No column matching attachment_index key {attachment_field_name}')
                    return None
                else:
                    col_query+=f', {atf}'
                    query_lookup[atf] = index_value
                    index_value += 1                
        # Query your row_data, external_id, global_key and metadata_index key columns from 
        query = f"""SELECT {col_query} FROM {bq_table.project}.{bq_table.dataset_id}.{bq_table.table_id}"""
        query_job = self.bq_client.query(query)
        # Iterate over your query payload to construct a list of data row dictionaries in Labelbox format
        global_key_to_upload_dict = {}
        for row in query_job:
            if len(row[query_lookup[row_data_col]]) <= 200:
                global_key = row[query_lookup[row_data_col]]
            else:
                print("Global key too long (>200 characters). Replacing with randomly generated global key.")
                global_key = str(uuid4())
            data_row_upload_dict = {
                "row_data" : row[query_lookup[row_data_col]],
                "metadata_fields" : [{"schema_id":metadata_name_key_to_schema['lb_integration_source'],"value":"BigQuery"}],
                "global_key" : str(global_key)
            }
            if external_id_col:
                data_row_upload_dict['external_id'] = row[query_lookup[external_id_col]]
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
        # Batch upload your list of data row dictionaries in Labelbox format
        if type(lb_dataset) == str:
            lb_dataset = self.lb_client.get_dataset(lb_dataset)
        upload_results = self.__batch_create_data_rows(client=self.lb_client, dataset=lb_dataset, global_key_to_upload_dict=global_key_to_upload_dict)


        print(f'Success')
        return upload_results

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
        print(len(rows_to_insert))
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
        check = self._sync_metadata_fields(bq_table_id, metadata_index)
        if not check:
          return None        
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
            upload_metadata.append(labelbox.schema.data_row_metadata.DataRowMetadata(data_row_id=drid, fields=new_metadata))
        results = lb_mdo.bulk_upsert(upload_metadata)
        return results        


#testing
table_name = "bigquery_test_update_3"
project_id = "clo3jk10r027i07vi4di5026g"
dataset_name = "testing_labelbox_bigquery"
google_creds_file = '/Users/luksta/Downloads/test-integration-385419-c039b7e10d74.json'
project_name = 'test-integration-385419'
client = Client('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJjbGJwZ3hmYWVod3c2MDc2dGVkbzVland4Iiwib3JnYW5pemF0aW9uSWQiOiJjbGJvNTl6YTUwbDR5MDd6cWMyaHAxdGFkIiwiYXBpS2V5SWQiOiJjbGw1cDMyMTUwaGUwMDcwbGNwY3Vha2llIiwic2VjcmV0IjoiNTM5NTYxZGExMGU1NjZiMDYxMGI4NGM3M2U3NGFiYjgiLCJpYXQiOjE2OTE3MDQzMzgsImV4cCI6MjMyMjg1NjMzOH0.v1xnfQk8hkBcVJaoRPBBIv5R5ou4u12i9wuhP6Ul6VE', google_project_name=project_name, google_key=google_creds_file)
client.export_to_BigQuery(project_id, dataset_name, table_name, create_table=False, include_label_details=True)