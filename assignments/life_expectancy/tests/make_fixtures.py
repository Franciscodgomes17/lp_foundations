"""Utility script to generate testing fixtures for Assignment 3.
This script:
1. Creates a small sample of the raw EU life expectancy dataset.
2. Ensures the sample contains both PT and non-PT regions.
3. Saves the sample as a fixture for isolated testing.
4. Runs the cleaning function on the sample to generate the expected output fixture.
"""
from pathlib import Path
import pandas as pd
from life_expectancy.cleaning import Region, clean_df
def main() -> None:
    """Generate sample input and expected output fixtures."""
    root = Path(__file__).resolve().parents[1]
    data_in = root / "data" / "eu_life_expectancy_raw.tsv"
    fixtures = root / "tests" / "fixtures"
    fixtures.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv(data_in, sep="\t")
    first_col = raw.columns[0]
    meta = raw[first_col].astype(str)
    pt_mask = meta.str.contains(",PT")
    non_pt_mask = ~pt_mask
    pt_sample = raw[pt_mask].head(20)
    non_pt_sample = raw[non_pt_mask].head(20)
    sample = pd.concat([pt_sample, non_pt_sample], ignore_index=True)
    sample_path = fixtures / "eu_life_expectancy_raw.tsv"
    sample.to_csv(sample_path, sep="\t", index=False)
    print(f"Wrote sample raw fixture: {sample_path}")
    sample_loaded = pd.read_csv(sample_path, sep="\t")
    expected_pt = clean_df(sample_loaded, region=Region.PT)
    expected_pt_path = fixtures / "pt_life_expectancy_expected.csv"
    expected_pt.to_csv(expected_pt_path, index=False)
    print(f"Wrote expected PT fixture: {expected_pt_path}")
if __name__ == "__main__":
    main()
