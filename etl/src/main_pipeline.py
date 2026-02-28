import os
import requests
import zipfile
import glob

# Configuration from Environment Variables
TRUD_KEY = os.getenv("TRUD_API_KEY")
# GCS_MOUNT_PATH should be /mnt/gcs in Cloud Run and your WSL2 path locally
OUTPUT_BASE_DIR = os.getenv("GCS_MOUNT_PATH", "/mnt/gcs")

# NHS TRUD Items (24: dm+d main, 25: dm+d supplementary/BNF/ATC)
ITEMS = ["24", "25"]

def extract_nested_zips(directory_path):
    """
    Recursively find and extract any nested zip files within the given directory.

    This function is necessary for NHS TRUD Item 25, which contains nested
    archives (e.g., the BNF zip file is contained within the outer item 25 zip).
    Extracts all nested zip files in-place within their respective directories.

    Args:
        directory_path (str): The absolute or relative path to the directory
            to scan for nested zip files.
    """
    # Find all .zip files in the directory and subdirectories
    search_pattern = os.path.join(directory_path, "**", "*.zip")
    nested_zips = glob.glob(search_pattern, recursive=True)

    for zip_file_path in nested_zips:
        print(f"Found nested zip: {zip_file_path}")
        extract_dir = os.path.dirname(zip_file_path)
        
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                print(f"Extracting nested archive {os.path.basename(zip_file_path)} into {extract_dir}...")
                zip_ref.extractall(extract_dir)
                
                extracted_files = zip_ref.namelist()
                print(f"Successfully extracted {len(extracted_files)} files from nested zip.")
            
            # Optional: Remove the nested zip file after successful extraction to save space
            # os.remove(zip_file_path)
            # print(f"Cleaned up nested zip: {zip_file_path}")
            
        except Exception as e:
            print(f"Error extracting nested zip {zip_file_path}: {e}")

def process_trud():
    """
    Orchestrate the download and extraction of NHS TRUD datasets.

    This function authenticates with the NHS TRUD API, determines the latest
    release for specified items (Item 24: dm+d main, Item 25: BNF/ATC), downloads
    the archive payloads into temporary storage, and extracts them into a
    configured Google Cloud Storage bucket path (or local equivalent).

    Returns:
        None
    """
    if not TRUD_KEY:
        print("Error: TRUD_API_KEY environment variable is not set.")
        return

    # Create the output directory if it doesn't exist
    if not os.path.exists(OUTPUT_BASE_DIR):
        print(f"Creating local directory: {OUTPUT_BASE_DIR}")
        os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    for item_id in ITEMS:
        print(f"\n--- Starting Processing for Item {item_id} ---")
        
        # 1. GET METADATA
        metadata_url = f"https://isd.digital.nhs.uk/trud/api/v1/keys/{TRUD_KEY}/items/{item_id}/releases?latest"
        response = requests.get(metadata_url)

        if response.status_code != 200:
            print(f"Failed to get metadata for {item_id}. Status: {response.status_code}")
            print(f"Response: {response.text}")
            continue

        data = response.json()

        if "releases" not in data or len(data["releases"]) == 0:
            print(f"No releases found for item {item_id}.")
            continue

        # Get download details for the latest release
        release = data["releases"][0]
        download_url = release.get("archiveFileUrl")
        zip_file_name = release.get("archiveFileName")
        
        if not download_url:
            print(f"No download URL found for {item_id}.")
            continue

        # 2. DOWNLOAD THE MAIN ZIP
        temp_zip_path = os.path.join("/tmp", zip_file_name)
        print(f"Downloading {zip_file_name} to temporary storage...")
        
        try:
            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()
                with open(temp_zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024): # 1MB chunks
                        f.write(chunk)

            # 3. EXTRACT FILES TO GCS BUCKET
            item_output_path = os.path.join(OUTPUT_BASE_DIR, f"item_{item_id}")
            os.makedirs(item_output_path, exist_ok=True)
            
            print(f"Extracting main archive to GCS: {item_output_path}...")
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(item_output_path)
                extracted_files = zip_ref.namelist()
                print(f"Successfully extracted {len(extracted_files)} files from main archive.")
            
            # 4. HANDLE NESTED ZIPS (Crucial for Item 25 / BNF)
            print(f"Checking for nested zip archives in {item_output_path}...")
            extract_nested_zips(item_output_path)
            
        except requests.exceptions.RequestException as e:
            print(f"Network error downloading {item_id}: {e}")
        except zipfile.BadZipFile:
             print(f"Downloaded file for item {item_id} is not a valid zip file.")
        except Exception as e:
            print(f"Unexpected error processing item {item_id}: {e}")
        
        finally:
            # 5. CLEAN UP TEMPORARY ZIP
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)
                print(f"Cleaned up main temporary file: {temp_zip_path}")

    print("\nAll items processed successfully.")

if __name__ == "__main__":
    process_trud()