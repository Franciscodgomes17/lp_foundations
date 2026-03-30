"""Data cleaning utilities for EU life expectancy dataset."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional
import pandas as pd
# pylint: disable=too-few-public-methods
@dataclass(frozen=True)
class Paths:
    """Default input/output locations relative to this module."""

    data_dir: Path = Path(__file__).resolve().parent / "data"
    input_file: Path = data_dir / "eurostat_life_expect.json"
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
    CLEANED_COLS = [UNIT, SEX, AGE, REGION, YEAR, VALUE]
class Region(Enum):
    """Valid region values present in the dataset."""
    AL = "AL"
    AM = "AM"
    AT = "AT"
    AZ = "AZ"
    BE = "BE"
    BG = "BG"
    BY = "BY"
    CH = "CH"
    CY = "CY"
    CZ = "CZ"
    DE = "DE"
    DE_TOT = "DE_TOT"
    DK = "DK"
    EA18 = "EA18"
    EA19 = "EA19"
    EE = "EE"
    EEA30_2007 = "EEA30_2007"
    EEA31 = "EEA31"
    EFTA = "EFTA"
    EL = "EL"
    ES = "ES"
    EU27_2007 = "EU27_2007"
    EU27_2020 = "EU27_2020"
    EU28 = "EU28"
    FI = "FI"
    FR = "FR"
    FX = "FX"
    GE = "GE"
    HR = "HR"
    HU = "HU"
    IE = "IE"
    IS = "IS"
    IT = "IT"
    LI = "LI"
    LT = "LT"
    LU = "LU"
    LV = "LV"
    MD = "MD"
    ME = "ME"
    MK = "MK"
    MT = "MT"
    NL = "NL"
    NO = "NO"
    PL = "PL"
    PT = "PT"
    RO = "RO"
    RS = "RS"
    RU = "RU"
    SE = "SE"
    SI = "SI"
    SK = "SK"
    SM = "SM"
    TR = "TR"
    UA = "UA"
    UK = "UK"
    XK = "XK"
    @classmethod
    def actual_countries(cls) -> list["Region"]:
        """Return only actual countries, excluding aggregate regions."""
        excluded = {
            cls.DE_TOT,
            cls.EA18,
            cls.EA19,
            cls.EEA30_2007,
            cls.EEA31,
            cls.EFTA,
            cls.EU27_2007,
            cls.EU27_2020,
            cls.EU28,
        }
        return [region for region in cls if region not in excluded]
# pylint: disable=too-few-public-methods
class DataLoaderStrategy(ABC):
    """Strategy interface for loading and normalizing data files."""
    @abstractmethod
    def load(self, input_path: Path) -> pd.DataFrame:
        """Load data and normalize it into the shared schema."""
# pylint: disable=too-few-public-methods
class TsvLoader(DataLoaderStrategy):
    """Load the legacy TSV format into the shared schema."""
    def load(self, input_path: Path) -> pd.DataFrame:
        df = pd.read_csv(input_path, sep="\t")
        first_col = df.columns[0]
        df_renamed = df.rename(columns={first_col: Cols.METADATA})

        meta = df_renamed[Cols.METADATA].astype(str).str.split(",", expand=True)
        meta.columns = Cols.ID_COLS

        wide = pd.concat([meta, df_renamed.drop(columns=[Cols.METADATA])], axis=1)
        return wide.melt(
            id_vars=Cols.ID_COLS,
            var_name=Cols.YEAR,
            value_name=Cols.VALUE,
        )
# pylint: disable=too-few-public-methods
class JsonLoader(DataLoaderStrategy):
    """Load the new JSON format into the shared schema."""
    def load(self, input_path: Path) -> pd.DataFrame:
        df = pd.read_json(input_path)
        normalized = df.rename(
            columns={
                "country": Cols.REGION,
                "life_expectancy": Cols.VALUE,
            }
        )
        return normalized[Cols.CLEANED_COLS]
def get_loader(input_path: Path) -> DataLoaderStrategy:
    """Return the loader strategy that matches the file extension."""
    loaders = {
        ".tsv": TsvLoader(),
        ".json": JsonLoader(),
    }
    try:
        return loaders[input_path.suffix.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported file format: {input_path.suffix}") from exc
def load_data(input_path: Optional[Path] = None) -> pd.DataFrame:
    """Load the raw dataset using the proper strategy for its format."""
    if input_path is None:
        input_path = Paths.input_file
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    loader = get_loader(input_path)
    return loader.load(input_path)
def clean_df(df: pd.DataFrame, region: Region = Region.PT) -> pd.DataFrame:
    """Clean the normalized dataset and filter it to a single region."""
    if df.empty:
        return pd.DataFrame(columns=Cols.CLEANED_COLS)

    result = df.copy()
    result[Cols.YEAR] = pd.to_numeric(
        result[Cols.YEAR].astype(str).str.strip(),
        errors="coerce",
    )
    cleaned_value = (
        result[Cols.VALUE]
        .astype(str)
        .str.strip()
        .replace({":": ""})
        .str.replace(r"[^\d.\-]", "", regex=True)
    )
    result[Cols.VALUE] = pd.to_numeric(cleaned_value, errors="coerce")

    result = result[result[Cols.REGION] == region.value].dropna(
        subset=[Cols.YEAR, Cols.VALUE]
    )
    result[Cols.YEAR] = result[Cols.YEAR].astype(int)
    return result[Cols.CLEANED_COLS]
def save_data(df: pd.DataFrame, output_path: Optional[Path] = None) -> None:
    """Save cleaned dataset to CSV."""
    if output_path is None:
        output_path = Paths.output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main(region: Region = Region.PT) -> pd.DataFrame:
    """Run the full data cleaning pipeline."""
    raw = load_data()
    cleaned = clean_df(raw, region=region)
    save_data(cleaned)
    return cleaned
if __name__ == "__main__":  # pragma: no cover
    main()
