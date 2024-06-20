import logging
from scripts.scripts import utils
import os
import pandas as pd

import urllib.parse

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    config = utils.load_yaml_config("config.yml")

    for data_source in config["data_sources"]:
        base_url = data_source["base_url"]
        if not base_url.endswith("/"):
            base_url += "/"
        default_pandas_kwargs = data_source.get("default_pandas_kwargs", {})
        for dataset in data_source["datasets"]:
            url_subdirectory = dataset.get("url_subdirectory")
            if url_subdirectory and url_subdirectory.startswith("/"):
                url_subdirectory = url_subdirectory[1:]

            output_directory = utils.sanitize_filename(dataset["name"])

            for file in dataset["files"]:
                logging.info(f"Processing file: {file['path']}")
                filename_with_extension = file["path"]
                if filename_with_extension.startswith("/"):
                    filename_with_extension = filename_with_extension[1:]

                _, file_extension = os.path.splitext(filename_with_extension)

                # Build the URL
                if url_subdirectory:
                    url = urllib.parse.urljoin(
                        base_url, url_subdirectory + "/" + filename_with_extension
                    )
                else:
                    url = urllib.parse.urljoin(base_url, filename_with_extension)

                logging.debug(f"URL: {url}")

                if file_extension == ".zip":
                    filenames_and_file_buffers = (
                        utils.get_filenames_and_text_files_from_zip(url)
                    )
                else:
                    try:
                        filenames_and_file_buffers = (
                            utils.get_filename_and_text_file_from_url(url)
                        )
                    except Exception as e:
                        logging.error(f"Error: {e}")
                        continue

                for filename, file_buffer in filenames_and_file_buffers:
                    df = pd.read_csv(file_buffer, **default_pandas_kwargs)
                    export_file_path = os.path.join(output_directory, filename + ".csv")
                    os.makedirs(output_directory, exist_ok=True)
                    df.to_csv(export_file_path, index=False)
                    logging.info(f"Exported file: {export_file_path}")


if __name__ == "__main__":
    main()
