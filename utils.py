import requests
import zipfile
import io
import pandas as pd

def download_zip(url):
    """Download a csv file from a URL"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f'Failed to download the file from {url}. Error: {e}')

def unzip_files(zip_content, output_folder='./data'):
    with zipfile.ZipFile(zip_content, 'r') as zip_file:
        # Extract all files to the output folder
        zip_file.extractall(output_folder)
        print(f"Files extracted to {output_folder}")