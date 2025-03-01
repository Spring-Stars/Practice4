from astroquery.vizier import Vizier
import pandas as pd
from tqdm import tqdm
import os
from typing import List

def download_multiple_catalogs(catalogs: List[str], save_dir: str = "data") -> List[str]:
    """
    Download multiple Vizier catalogs and save them as separate CSV files.
    
    Parameters:
    - catalogs (List[str]): List of Vizier catalog IDs.
    - save_dir (str): Directory to save downloaded files.
    
    Returns:
    - List[str]: Paths to successfully saved files.
    """
    vizier = Vizier(columns=['*'], row_limit=-1)
    saved_files = []
    
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)
    
    for catalog in catalogs:
        try:
            # Get catalog data
            result = vizier.get_catalogs(catalog)
            
            if not result:
                print(f"\nNo data found for {catalog}")
                continue
                
            # Convert to DataFrame
            df = result[0].to_pandas()
            
            # Generate filename
            clean_catalog = catalog.replace('/', '_')
            filename = f"{clean_catalog}_full.csv"
            save_path = os.path.join(save_dir, filename)
            
            # Save to CSV
            df.to_csv(save_path, index=False)
            saved_files.append(save_path)
            print(f"\nSuccessfully saved {catalog} ({len(df)} rows) to {save_path}")
            
        except Exception as e:
            print(f"\nFailed to download {catalog}: {str(e)}")
            continue
            
    return saved_files