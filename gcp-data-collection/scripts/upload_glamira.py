from pathlib import Path 

from gcp_data_collection import gcs
from config.settings import settings   


LOCAL_FILE_PATH = Path("data/glamira_ubl_oct2019_nov2019.tar.gz")
DESTINATION_PATH = "raw/glamira_ubl_oct2019_nov2019.tar.gz"

if not LOCAL_FILE_PATH.is_file():
    raise FileNotFoundError(f"Local file {LOCAL_FILE_PATH} does not exist.")

# ensure the bucket exists
# gcs.create_bucket()

# upload the file
gcs.upload_file(
    local_path=LOCAL_FILE_PATH,
    dest_path=DESTINATION_PATH)

print(f"Uploaded {LOCAL_FILE_PATH} to gs://{settings.gcs_raw_bucket}/{DESTINATION_PATH}")