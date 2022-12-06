import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="labelbox_bigquery",
    version="0.1.0",
    author="Labelbox",
    author_email="ecosystem+bigquery@labelbox.com",
    description="Labelbox Connector for BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://labelbox.com",
    packages=setuptools.find_packages(),
    install_requires=["labelbox", "google-cloud-bigquery"],
    keywords=["labelbox", "bigquery", "labelbox_bigquery"],
)
