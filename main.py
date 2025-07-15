from utils import unzip_file
from tracking_processor import process_skynet_reports
import os

def main():
    # Paths
    zip_path = os.path.join("data", "skynet_report.zip")  # Update if your ZIP file has a different name
    extract_to = os.path.join("data", "unzipped")
    output_folder = "output"

    # Step 1: Unzip the Skynet report
    print("Unzipping report...")
    unzip_file(zip_path, extract_to)

    # Step 2: Process all .xls files inside the extracted folder
    print("Processing Skynet report files...")
    process_skynet_reports(extract_to, output_folder)

    print("✅ All reports processed successfully!")

if __name__ == "__main__":
    main()
