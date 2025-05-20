from astroquery.vizier import Vizier
from tqdm import tqdm
import os
from typing import List
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import psutil
from typing import Optional


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


def download_gaia(
    url: str,  # URL of the directory to scrape (replace with the actual URL of the directory)
    save_dir: str,  # Name of directory where you want to save the files
)-> str:
    # Directory where you the files will save
    download_dir = f"./data/{save_dir}"

    # Create download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

    # Send a GET request to the directory page
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Parse the directory listing page
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract all links (file names)
    file_links = soup.find_all("a")

    # Filter out directories and non-GZ files
    files_to_download = [urljoin(url, link.get("href")) for link in file_links if link.get("href").endswith(".csv.gz")]

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


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("merge_gaia.log"),
        logging.StreamHandler()
    ]
)

def get_memory_usage() -> Optional[str]:
    """Return current process memory usage in MB"""
    try:
        process = psutil.Process(os.getpid())
        return f"{process.memory_info().rss / 1024 ** 2:.1f}MB"
    except Exception as e:
        logging.warning(f"Could not get memory usage: {str(e)}")
        return None

def merge_gaia(directory_path: str, save_name: str) -> str:
    """
    Merge multiple Gaia CSV.gz files into a single Parquet file with memory optimization
    
    Args:
        directory_path: Path to directory containing CSV.gz files
        save_name: Name for output Parquet file (without extension)
        
    Returns:
        Path to created Parquet file
    """
    output_path = f"./data/{save_name}.parquet"
    
    try:
        # Validate directory
        if not os.path.isdir(directory_path):
            raise ValueError(f"Invalid directory: {directory_path}")
            
        # List CSV.gz files
        file_list = [f for f in os.listdir(directory_path) if f.endswith('.csv.gz')]
        if not file_list:
            raise ValueError("No CSV.gz files found in directory")
            
        logging.info(f"Found {len(file_list)} CSV.gz files to process")
        logging.info(f"Initial memory usage: {get_memory_usage() or 'unknown'}")

        # Initialize Parquet writer
        writer = None
        schema = None
        processed_files = 0
        total_rows = 0

        for file in tqdm(file_list, desc="Processing files"):
            file_path = os.path.join(directory_path, file)
            try:
                # Read CSV
                logging.info(f"Processing {file}...")
                df = pd.read_csv(file_path, comment='#')
                
                # Validate schema consistency
                if schema is None:
                    schema = pa.Table.from_pandas(df).schema
                    writer = pq.ParquetWriter(output_path, schema, compression='snappy')
                else:
                    current_schema = pa.Table.from_pandas(df).schema
                    if current_schema != schema:
                        logging.warning(f"Schema mismatch in {file}, skipping")
                        continue
                        
                # Write to Parquet
                table = pa.Table.from_pandas(df)
                writer.write_table(table)
                
                # Update counters
                processed_files += 1
                total_rows += len(df)
                
                # Clean up memory
                del df, table
                
                # Log memory usage every 10 files
                if processed_files % 10 == 0:
                    logging.info(f"Intermediate memory usage: {get_memory_usage() or 'unknown'}")
                    logging.info(f"Processed {processed_files} files, {total_rows} rows so far")
                    
            except Exception as e:
                logging.error(f"Error processing {file}: {str(e)}")
                continue

        # Final cleanup
        if writer:
            writer.close()
            
        logging.info(f"Successfully processed {processed_files}/{len(file_list)} files")
        logging.info(f"Total rows written: {total_rows}")
        logging.info(f"Final memory usage: {get_memory_usage() or 'unknown'}")
        logging.info(f"Output file created at: {output_path}")
        
        return output_path

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)
        raise


# Example Usage
if __name__ == "__main__":
    """
    # See more catalogues at https://vizier.cds.unistra.fr/
    catalogs = [
        "B/vsx/vsx",
    ]
    
    # Download only missing catalogs
    parquet_files = download_catalogs(catalogs)
    """
    
    files_directory_path = download_gaia(
        url = "https://cdn.gea.esac.esa.int/Gaia/gdr3/Photometry/epoch_photometry/",
        save_dir = "photometry_epoch_photometry_files",
    )
    merged_data_path = merge_gaia(
        directory_path = files_directory_path,
        save_name = "photometry_epoch_photometry",
        )