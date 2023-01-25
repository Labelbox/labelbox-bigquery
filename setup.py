import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="labelboxbigquery",
    version="0.1.2",
    author="Labelbox",
    author_email="raphael@labelbox.com",
    description="Labelbox Connector for BigQuery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://labelbox.com",
    packages=setuptools.find_packages(),
    install_requires=["labelbox", "google-cloud-bigquery", "labelbase"],
    keywords=["labelbox", "bigquery", "labelboxbigquery", "labelbase"],
)
