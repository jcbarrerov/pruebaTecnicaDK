import pandas as pd
from typing import List

def create_dataframe(rows:List[str], columns:List[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=columns)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    print("DataFrame created successfully!")
    return df

def load_csv(df:pd.DataFrame, date:str, path:str | None = None) -> None:
    if path == None:
        path_csv = f"OFFEI_cleansed_{date}.csv"
    else: 
        path_csv = f"{path}/OFFEI_cleansed_{date}.csv" 

    df.to_csv(path_csv, index=False)
    print("CSV file loaded successfully!")
    






