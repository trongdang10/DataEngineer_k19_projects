from pathlib import Path 

from gcp_data_collection import gcs
from config.settings import settings

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
# # set to your actual local file path and destination path in the bucket
LOCAL_FILE_PATH = Path("data/glamira_ubl_oct2019_nov2019.tar.gz") 
DESTINATION_PATH = "glamira.tar.gz"


def main():
    if not LOCAL_FILE_PATH.is_file():
        raise FileNotFoundError(f"Local file {LOCAL_FILE_PATH} does not exist.")
    
    # ensure the bucket exists
    gcs.create_bucket()

    # upload the file
    gcs.upload_file(local_path=LOCAL_FILE_PATH, dest_path=DESTINATION_PATH)
    print(f"Uploaded {LOCAL_FILE_PATH} to gs://{settings.gcs_raw_bucket}/{DESTINATION_PATH}")


if __name__ == "__main__":
    main()