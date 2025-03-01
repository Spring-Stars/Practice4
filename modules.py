from astroquery.vizier import Vizier
import pandas as pd
from tqdm import tqdm
import os
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional

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

def load_merged_data(
    file_paths: List[str],
    merge: bool = True
) -> Optional[pd.DataFrame]:
    """
    Load cached Parquet files
    
    Args:
        file_paths: List of Parquet file paths
        merge: Combine into single DataFrame
        
    Returns:
        Merged DataFrame or list of DataFrames
    """
    data = []
    for path in file_paths:
        if not os.path.exists(path):
            print(f"Missing file: {path}")
            continue
        try:
            df = pd.read_parquet(path)
            data.append(df)
        except Exception as e:
            print(f"Error reading {path}: {str(e)}")
    
    if not data:
        return None
    
    return pd.concat(data, ignore_index=True) if merge else data

# Example Usage
if __name__ == "__main__":
    # See more catalogues at https://vizier.cds.unistra.fr/
    catalogs = [
        "I/122",    # Hipparcos Input Catalog
        "I/16",     # SAO Catalog
        "I/161",    # HIC (Hipparcos Input Catalog)
        "I/242",    # 2MASS All-Sky Catalog
        "I/261",    # Tycho-2 Catalog
        "I/306A"    # USNO-B1.0 Catalog
    ]
    
    # Download only missing catalogs
    parquet_files = download_catalogs(catalogs)
    
    # Load and merge data
    merged_df = load_merged_data(parquet_files)
    
    if merged_df is not None:
        print(f"Merged data shape: {merged_df.shape}")
        merged_df.to_csv("data/merged_catalogs.csv")
        print("Saved merged dataset")