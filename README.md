# Labelbox Connector for Google BigQuery

Access the Labelbox Connector for Google BigQuery to easily perform the following functionalities:
- `lbbq.client.create_data_rows_from_table` :   Creates Labelbox data rows (and metadata) given a BigQuery table
- `lbbq.client.create_table_from_dataset`   :   Creates a BigQuery table given a Labelbox dataset
- `lbbq.client.upsert_table_metadata`       :   Updates BigQuery table metadata columns given a Labelbox dataset
- `lbbq.client.upsert_labelbox_metadata`    :   Updates Labelbox metadata given a BigQuery table

The Demo code supplied in this Github is designed to run in a Google Colab, but the code can be adapted to any notebook environment.

Labelbox is the enterprise-grade training data solution with fast AI enabled labeling tools, labeling automation, human workforce, data management, a powerful API for integration & SDK for extensibility. Visit [Labelbox](http://labelbox.com/) for more information.

This library is currently in beta. It may contain errors or inaccuracies and may not function as well as commercially released software. Please report any issues/bugs via [Github Issues](https://github.com/Labelbox/labelbigquery/issues).


## Table of Contents

* [Requirements](#requirements)
* [Configuration](#configuration)
* [Use](#Use)

## Requirements

* [Google Cloud BigQuery Authenticated Client](https://cloud.google.com/bigquery/docs/reference/libraries)
* [Google BigQuery SDK](https://pypi.org/project/google-cloud-bigquery/)
* [Labelbox account](http://app.labelbox.com/)
* [Generate a Labelbox API key](https://labelbox.com/docs/api/getting-started#create_api_key)

## Configuration

Install Labelbox-BigQuery to your Python environment. The installation will also add the Labelbox SDK and BigQuery SDK.

```
pip install labelbox-bigquery
import labelboxbigquery
```

## Use

The `client` class requires the following arguments:
- `lb_api_key` = Labelbox API Key
- `google_key` = Google Service Account Permissions dict, how to create one [here](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating)
- `google_project_name` = Google Project ID / Name
