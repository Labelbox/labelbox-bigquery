# Labelbox Connector for Google BigQuery

Access the Labelbox Connector for Google BigQuery to easily perform the following functionalities:
- `create_data_rows_from_table` :   Creates Labelbox data rows (and metadata) given a BigQuery table
- `create_table_from_dataset`   :   Creates a BigQuery table given a Labelbox dataset
- `upsert_table_metadata`       :   Updates BigQuery table metadata columns given a Labelbox dataset
- `upsert_labelbox_metadata`    :   Updates Labelbox metadata given a BigQuery table

The Demo code supplied in this Github is designed to run in a Google Colab, but the code can be adapted to any notebook environment.

Labelbox is the enterprise-grade training data solution with fast AI enabled labeling tools, labeling automation, human workforce, data management, a powerful API for integration & SDK for extensibility. Visit [Labelbox](http://labelbox.com/) for more information.

This library is currently in beta. It may contain errors or inaccuracies and may not function as well as commercially released software. Please report any issues/bugs via [Github Issues](https://github.com/Labelbox/labelbigquery/issues).


## Table of Contents

* [Requirements](#requirements)
* [Installation](#installation)
* [Documentation](#documentation)
* [Authentication](#authentication)
* [Contribution](#contribution)

## Requirements

* [Google Cloud BigQuery Authenticated Client](https://cloud.google.com/bigquery/docs/reference/libraries)
* [Google BigQuery SDK](https://pypi.org/project/google-cloud-bigquery/)
* [Labelbox account](http://app.labelbox.com/)
* [Generate a Labelbox API key](https://labelbox.com/docs/api/getting-started#create_api_key)

## Installation

Install Labelbox-BigQuery to your Python environment. The installation will also add the Labelbox SDK and BigQuery SDK.

```
pip install labelboxbigquery
```

## Documentation

Labelbox-BigQuery includes several methods to help facilitate your workflow between BigQuery and Labelbox. 

1. Add your CSV contents to BigQuery (only necessary if you don't have your data in BigQuery yet):

```
   #define headers and fields for BigQuery data load
    SELECTED_HEADERS = {
        'conversation_id',
        'normalized_query'
    }

    SCHEMA_FIELDS = [
        bigquery.SchemaField("conversation_id", "STRING"),
        bigquery.SchemaField("normalized_query", "STRING"),
    ]

    labelboxbigquery.load_data_to_big_query(bq_client, args.table_name, args.csv_file_name,
                                         SELECTED_HEADERS = SELECTED_HEADERS,SCHEMA_FIELDS = SCHEMA_FIELDS)
```
Where "SELECTED_HEADERS" and "SCHEMA_FIELDS" specifies the columns of your CSV that you want to send to BigQuery, along with the type definitions for proper storage in BigQuery.

Labelbox-bigquery for text requires two columns of data; a unique identifier (becomes the "External ID" in our system), and a corresponding text string. Here is a chatbot example table:

| conversation_id | normalized_query                 |
|-------------|--------------------------------------|
| sample_1   | Some text string here for labeling.  |
| sample_2  | Some text string here for labeling.  |
| sample_3  | Some text string here for labeling.  |

2. Submit a query to BigQuery for your target columns. This will also write individual text files to a "data" folder. The file names are based off the unique identifier ("conversation id" in the above example).
```
    query = fr'SELECT conversation_id, STRING_AGG(normalized_query, "\n") FROM {args.table_name} GROUP BY conversation_id'
    file_names = labelboxbigquery.fetch_and_write_rows(bq_client, query=query)
```

3. Submit your files to Labelbox for annotation in the text editor.

```
    lb_dataset = labelboxbigquery.make_dataset_and_data_rows(lb_client, file_names, args.dataset_name)
    print("Dataset unique identifier: " + lb_dataset.uid)
```

While using Labelbox-BigQuery, you will likely also use the Labelbox SDK (e.g. for programmatic ontology creation). These resources will help familiarize you with the Labelbox Python SDK: 
* [Visit our docs](https://labelbox.com/docs/python-api) to learn how the SDK works
* View our [Labelbox-BigQuery demo code](https://github.com/Labelbox/labelbox-bigquery/tree/main/demo_code) for inspiration.
* view our [API reference](https://labelbox.com/docs/python-api/api-reference).

## Authentication

Labelbox uses API keys to validate requests. You can create and manage API keys on [Labelbox](https://app.labelbox.com/account/api-keys). 

## Contribution
Please consult `CONTRIB.md`


