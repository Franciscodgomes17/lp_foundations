"""Data cleaning utilities for EU life expectancy dataset."""

# pylint: disable=invalid-name

from __future__ import annotations

from pathlib import Path
import pandas as pd


# --- file locations
data_dir = Path(__file__).resolve().parent / "data"
input_file = data_dir / "eu_life_expectancy_raw.tsv"
output_file = data_dir / "pt_life_expectancy.csv"


col_metadata = "metadata"
col_unit = "unit"
col_sex = "sex"
col_age = "age"
col_region = "region"
col_year = "year"
col_value = "value"

id_cols = [col_unit, col_sex, col_age, col_region]


def transform_life_expectancy(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Pure transformation:
    - Split metadata column into unit/sex/age/region
    - Unpivot wide -> long
    - Clean year/value
    - Filter by country and drop NaNs
    """
    first_col = df.columns[0]
    df = df.rename(columns={first_col: col_metadata})

    meta = df[col_metadata].str.split(",", expand=True)
    meta.columns = id_cols

    df = pd.concat([meta, df.drop(columns=[col_metadata])], axis=1)

    df_long = df.melt(
        id_vars=id_cols,
        var_name=col_year,
        value_name=col_value,
    )

    df_long[col_year] = df_long[col_year].astype(str).str.strip().astype(int)

    cleaned_value = (
        df_long[col_value]
        .astype(str)
        .str.strip()
        .replace({":": ""})
        .str.replace(r"[^\d\.\-]", "", regex=True)
    )
    df_long[col_value] = pd.to_numeric(cleaned_value, errors="coerce")

    df_long = df_long[df_long[col_region] == country].dropna(subset=[col_value])

    return df_long


def clean_data(country: str = "PT") -> None:
    """
    I/O wrapper:
    - Reads raw TSV from disk
    - Applies transformation
    - Writes CSV to disk
    """
    df = pd.read_csv(input_file, sep="\t")
    result = transform_life_expectancy(df, country=country)
    result.to_csv(output_file, index=False)


if __name__=="__main__": # pragma: no cover

    clean_data()
