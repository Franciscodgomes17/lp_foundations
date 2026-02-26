"""Data cleaning utilities for EU life expectancy dataset."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import pandas as pd
# pylint: disable=too-few-public-methods
@dataclass(frozen=True)
class Paths:
    """Default input/output locations relative to this module."""
    data_dir: Path = Path(__file__).resolve().parent / "data"
    input_file: Path = data_dir / "eu_life_expectancy_raw.tsv"
    output_file: Path = data_dir / "pt_life_expectancy.csv"
# pylint: disable=too-few-public-methods
class Cols:
    """Column name constants."""
    METADATA = "metadata"
    UNIT = "unit"
    SEX = "sex"
    AGE = "age"
    REGION = "region"
    YEAR = "year"
    VALUE = "value"
    ID_COLS = [UNIT, SEX, AGE, REGION]
def load_data(input_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load raw EU life expectancy dataset from a TSV file.
    """
    input_path = input_path or Paths.input_file
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    return pd.read_csv(input_path, sep="\t")
def clean_df(df: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
    """
    Pure transformation:
    - Rename first column to METADATA
    - Split metadata into unit/sex/age/region
    - Unpivot wide -> long
    - Clean year/value
    - Filter by country and drop NaNs
    """
    if df.empty:
        return df.copy()
    first_col = df.columns[0]
    df_renamed = df.rename(columns={first_col: Cols.METADATA})
    meta = df_renamed[Cols.METADATA].astype(str).str.split(",", expand=True)
    meta.columns = Cols.ID_COLS
    wide = pd.concat([meta, df_renamed.drop(columns=[Cols.METADATA])], axis=1)
    long_df = wide.melt(
        id_vars=Cols.ID_COLS,
        var_name=Cols.YEAR,
        value_name=Cols.VALUE,
    )
    long_df[Cols.YEAR] = long_df[Cols.YEAR].astype(str).str.strip().astype(int)
    cleaned_value = (
        long_df[Cols.VALUE]
        .astype(str)
        .str.strip()
        .replace({":": ""})
        .str.replace(r"[^\d.\-]", "", regex=True)
    )
    long_df[Cols.VALUE] = pd.to_numeric(cleaned_value, errors="coerce")
    result = long_df[long_df[Cols.REGION] == country].dropna(subset=[Cols.VALUE])
    return result
def save_data(df: pd.DataFrame, output_path: Optional[Path] = None) -> None:
    """
    Save cleaned dataset to CSV.
    """
    output_path = output_path or Paths.output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
def clean_data(country: str = "PT") -> None:
    """
    I/O wrapper kept for backward compatibility with the previous lesson/tests:
    - Load raw data
    - Clean it using the pure transformer
    - Save to disk
    """
    raw = load_data()
    cleaned = clean_df(raw, country=country)
    save_data(cleaned)
def main(country: str = "PT") -> pd.DataFrame:
    """
    Orchestrates the full pipeline and returns the cleaned DataFrame.
    """
    raw = load_data()
    cleaned = clean_df(raw, country=country)
    save_data(cleaned)
    return cleaned
if __name__ == "__main__":  # pragma: no cover
    main()
