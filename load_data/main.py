import os
import requests
from google.cloud import storage

def download_2019_fhv_data():
    # Config
    repo = "DataTalksClub/nyc-tlc-data"
    tag = "fhv"
    bucket_name = os.getenv("GCP_GCS_BUCKET")
    
    # Get assets from GitHub API
    api_url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    response = requests.get(api_url)
    response.raise_for_status()
    assets = response.json().get('assets', [])

    # Filter for 2019 files only
    files_to_download = [a for a in assets if "2019" in a['name']]
    
    if not files_to_download:
        print("No 2019 files found.")
        return

    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for asset in files_to_download:
        file_name = asset['name']
        download_url = asset['browser_download_url']
        
        print(f"Streaming {file_name} to GCS...")
        
        # Use streaming to keep memory footprint low
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            blob = bucket.blob(file_name)
            # upload_from_file handles the raw stream from the request
            blob.upload_from_file(r.raw)
            
        print(f"Successfully uploaded: {file_name}")

if __name__ == "__main__":
    download_2019_fhv_data()


# import os
# import zipfile
# import requests
# from google.cloud import storage

# # --- CONFIGURATION ---
# URL = "https://example.com/taxi_data.zip"  # Your source URL
# BUCKET_NAME = "your-gcs-bucket-name"
# LOCAL_ZIP = "data.zip"
# EXTRACT_DIR = "./unzipped_files"

# def download_unzip_upload():
#     # 1. Download the zipped file into the container
#     print(f"Downloading {URL}...")
#     r = requests.get(URL)
#     with open(LOCAL_ZIP, 'wb') as f:
#         f.write(r.content)

#     # 2. Extract contents locally
#     print("Extracting files...")
#     if not os.path.exists(EXTRACT_DIR):
#         os.makedirs(EXTRACT_DIR)
        
#     with zipfile.ZipFile(LOCAL_ZIP, 'r') as zip_ref:
#         zip_ref.extractall(EXTRACT_DIR)

#     # 3. Upload extracted CSVs to GCS bucket
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(BUCKET_NAME)

#     print(f"Uploading files to bucket: {BUCKET_NAME}")
#     for filename in os.listdir(EXTRACT_DIR):
#         if filename.endswith(".csv"):
#             local_path = os.path.join(EXTRACT_DIR, filename)
#             # The 'blob' is the destination path in your bucket
#             blob = bucket.blob(f"taxi_data/{filename}") 
            
#             print(f"Uploading {filename}...")
#             blob.upload_from_filename(local_path)

#     print("All files uploaded successfully.")

# if __name__ == "__main__":
#     download_unzip_upload()
