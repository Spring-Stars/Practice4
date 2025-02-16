import pandas as pd

if __name__ == "__main__":
    base_name = "1603053657497A" # Имя файла без разрешения
    df = pd.read_csv(f"data/{base_name}.csv")
    df.to_parquet(f"data/{base_name}.parquet")
