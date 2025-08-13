import pandas as pd
from pathlib import Path

def clean_csv(input_file: Path, output_file: Path):
    df = pd.read_csv(input_file, dtype=str)
    df = df.drop_duplicates(subset=["id"])
    df.to_csv(output_file, index=False)
    print(f"[Preprocess] Input: {len(df)} unique IDs saved to {output_file}")
