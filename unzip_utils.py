import zipfile
import os
from pathlib import Path

def unzip_file(zip_path, extract_to='unzipped'):
    """Extracts a zip file to the specified directory."""
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    print(f"✅ Extracted '{zip_path}' to '{extract_to}'")
    return Path(extract_to)
