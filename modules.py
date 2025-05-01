from astroquery.vizier import Vizier
from tqdm import tqdm
import os
from typing import List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd


def download_catalogs(
    catalogs: List[str],
    save_dir: str = "data",
    force_redownload: bool = False
) -> List[str]:
    """
    Download catalogs with Parquet format and local cache checking
    
    Args:
        catalogs: List of Vizier catalog IDs
        save_dir: Directory to save/load Parquet files
        force_redownload: Ignore existing files and re-download
        
    Returns:
        List of paths to local Parquet files
    """
    os.makedirs(save_dir, exist_ok=True)
    vizier = Vizier(columns=['*'], row_limit=-1)
    saved_files = []

    for catalog in tqdm(catalogs, desc="Processing catalogs"):
        try:
            # Generate safe filename
            parquet_path = os.path.join(save_dir, f"{catalog.replace('/', '_')}.parquet")
            
            # Skip existing files unless force_redownload
            if not force_redownload and os.path.exists(parquet_path):
                tqdm.write(f"Skipping {catalog} - already exists")
                saved_files.append(parquet_path)
                continue
                
            # Download catalog
            result = vizier.get_catalogs(catalog)
            if not result:
                tqdm.write(f"No data for {catalog}")
                continue
                
            # Convert to DataFrame
            df = result[0].to_pandas()
            
            # Save as Parquet
            df.to_parquet(
                parquet_path,
                engine='pyarrow',
                compression='snappy'
            )
            saved_files.append(parquet_path)
            tqdm.write(f"Saved {catalog} ({len(df)} rows) to {parquet_path}")
            
        except Exception as e:
            tqdm.write(f"Failed {catalog}: {str(e)}")
            continue
            
    return saved_files


def download_gaia_vari_short_timescale(
    save_dir: str = "vari_short_timescale_files",
)-> str:

    # URL of the directory to scrape (replace with the actual URL of the directory)
    base_url = "https://cdn.gea.esac.esa.int/Gaia/gdr3/Variability/vari_short_timescale/"

    # Directory where you want to save the files
    download_dir = f"./data/{save_dir}"

    # Create download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

    # Send a GET request to the directory page
    response = requests.get(base_url)
    response.raise_for_status()  # Check if the request was successful

    # Parse the directory listing page
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract all links (file names)
    file_links = soup.find_all("a")

    # Filter out directories and non-GZ files
    files_to_download = [urljoin(base_url, link.get("href")) for link in file_links if link.get("href").endswith(".csv.gz")]

    # Download the files
    for file_url in tqdm(files_to_download, desc="Downloading data"):
        file_name = os.path.join(download_dir, file_url.split("/")[-1])
        
        # Check if the file already exists
        if os.path.exists(file_name):
            continue
        
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()  # Check if the request was successful
            with open(file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    print(f"Download complete. Saved at {download_dir}")

    return download_dir


def merge_gaia_vari_short_timescale(
    directory_path: str = './data/vari_short_timescale_files',
    save_name: str = "vari_short_timescale_files",
) -> str:
    # Check if the file already exists
    if os.path.exists(f"./data/{save_name}.parquet"):
        print(f"File {save_name}.parquet already exist")
        return f"./data/{save_name}.parquet"

    # List all files in the directory
    file_list = [f for f in os.listdir(directory_path) if f.endswith('.csv.gz')]

    # Initialize an empty list to store dataframes
    df_list = []

    # Load each file and append to the list
    for file in tqdm(file_list, desc="Processing data"):
        file_path = os.path.join(directory_path, file)
        df = pd.read_csv(file_path, comment='#')  # Load the file, skipping metadata
        df_list.append(df)

    # Merge all dataframes into one
    merged_data = pd.concat(df_list, ignore_index=True)

    merged_data.to_parquet(
                f"./data/{save_name}.parquet",
                engine='pyarrow',
                compression='snappy'
            )
    
    return f"./data/{save_name}.parquet"


# Example Usage
if __name__ == "__main__":
    # See more catalogues at https://vizier.cds.unistra.fr/
    catalogs = [
        "B/vsx/vsx",
    ]
    
    # Download only missing catalogs
    parquet_files = download_catalogs(catalogs)

    download_gaia_vari_short_timescale()
