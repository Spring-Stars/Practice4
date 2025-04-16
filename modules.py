from astroquery.vizier import Vizier
from tqdm import tqdm
import os
from typing import List

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


# Example Usage
if __name__ == "__main__":
    # See more catalogues at https://vizier.cds.unistra.fr/
    catalogs = [
        "B/vsx/vsx",
    ]
    
    # Download only missing catalogs
    parquet_files = download_catalogs(catalogs)