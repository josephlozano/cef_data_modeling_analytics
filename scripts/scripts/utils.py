import pandas as pd
import yaml
from zipfile import ZipFile
import cgi
import requests
import io
import os
import logging
import urllib

def load_yaml_config(filename):
    with open(filename, "r") as file:
        config = yaml.safe_load(file)
    return config


def sanitize_filename(filename):
    return filename.lower().replace(" ", "_").replace("-", "_")


def get_filename_and_text_file_from_url(url):
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise ValueError(f"Could not download file from {url}")
    params = cgi.parse_header(response.headers.get("Content-Disposition", ""))[-1]
    if "filename" not in params:
        logging.warning("Could not find a filename in the response headers. Using the URL's basename.")
        filename = urllib.parse.urlparse(url).path
    else:
        filename = params["filename"]

    filename = os.path.basename(filename)

    yield (filename, response.raw)


def get_zip_file_from_url(url):
    response = requests.get(url)
    zip_file = ZipFile(io.BytesIO(response.content))
    return zip_file


def get_filenames_and_text_files_from_zip(url):
    zip_file = get_zip_file_from_url(url)
    for file in zip_file.namelist():
        if file.endswith(".txt"):
            filename = os.path.basename(file)
            yield (filename, zip_file.open(file))


def export_df_to_csv(df, filename, **kwargs):
    df.to_csv(filename, **kwargs)
