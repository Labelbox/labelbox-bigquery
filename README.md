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
pip install labelboxbigquery
import labelboxbigquery
```

## Use

The `client` class requires the following arguments:
- `lb_api_key` = Labelbox API Key
- `google_key` = Google Service Account Permissions dict, how to create one [here](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating)
- `google_project_name` = Google Project ID / Name


## Provenance
[![SLSA 3](https://slsa.dev/images/gh-badge-level3.svg)](https://slsa.dev)

To enhance the software supply chain security of Labelbox's users, as of 0.1.8, every release contains a [SLSA Level 3 Provenance](https://github.com/slsa-framework/slsa-github-generator/blob/main/internal/builders/generic/README.md) document.  
This document provides detailed information about the build process, including the repository and branch from which the package was generated.

By using the [SLSA framework's official verifier](https://github.com/slsa-framework/slsa-verifier), you can verify the provenance document to ensure that the package is from a trusted source. Verifying the provenance helps confirm that the package has not been tampered with and was built in a secure environment.

Example of usage for the 0.1.8 release wheel:

```
VERSION=0.1.8 #tag
gh release download ${VERSION} --repo Labelbox/labelbox-bigquery

slsa-verifier verify-artifact --source-branch main --builder-id 'https://github.com/slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@refs/tags/v2.0.0' --source-uri "git+https://github.com/Labelbox/labelbox-bigquery" --provenance-path multiple.intoto.jsonl ./labelboxbigquery-${VERSION}-py3-none-any.whl
```