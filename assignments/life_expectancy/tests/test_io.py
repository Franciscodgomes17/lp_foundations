"""Tests for loading and saving life expectancy data."""

from unittest.mock import patch

import pandas as pd
import pytest

from life_expectancy.cleaning import Cols, Paths, Region, load_data, main, save_data
from . import FIXTURES_DIR


def test_load_data_reads_expected_tsv() -> None:
    """Ensure TSV data is normalized into the shared schema."""
    df = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")

    assert list(df.columns) == Cols.CLEANED_COLS
    assert df.iloc[0].to_dict() == {
        "unit": "YR",
        "sex": "F",
        "age": "Y1",
        "region": "PT",
        "year": "2021 ",
        "value": ": ",
    }


def test_load_data_reads_expected_json() -> None:
    """Ensure JSON data is normalized into the shared schema."""
    df = load_data(FIXTURES_DIR / "eurostat_life_expect_sample.json")

    assert list(df.columns) == Cols.CLEANED_COLS
    assert len(df) == 3
    assert df.iloc[0].to_dict() == {
        "unit": "YR",
        "sex": "F",
        "age": "Y1",
        "region": "PT",
        "year": 2020,
        "value": 83.3,
    }


def test_load_data_uses_default_input_path() -> None:
    """Ensure load_data uses the default input path when none is provided."""
    with patch.object(
        Paths,
        "input_file",
        FIXTURES_DIR / "eurostat_life_expect_sample.json",
    ):
        df = load_data()

    assert list(df.columns) == Cols.CLEANED_COLS
    assert len(df) == 3


def test_load_data_raises_for_missing_file(tmp_path) -> None:
    """Ensure a missing file raises FileNotFoundError."""
    missing_file = tmp_path / "missing.json"

    with pytest.raises(FileNotFoundError, match="Input file not found"):
        load_data(missing_file)


def test_load_data_raises_for_unsupported_extension(tmp_path) -> None:
    """Ensure unsupported file types are rejected."""
    bad_file = tmp_path / "sample.txt"
    bad_file.write_text("invalid", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file format"):
        load_data(bad_file)


def test_save_data_calls_to_csv_once() -> None:
    """Ensure save_data calls DataFrame.to_csv without writing a file."""
    df = pd.DataFrame({"a": [1]})

    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        save_data(df)
        mock_to_csv.assert_called_once()


def test_main_returns_dataframe_and_saves_once() -> None:
    """Ensure main returns a DataFrame and triggers saving."""
    with patch("life_expectancy.cleaning.load_data") as mock_load, patch(
        "pandas.DataFrame.to_csv"
    ) as mock_to_csv:
        mock_load.return_value = pd.DataFrame(
            {
                "unit": ["YR"],
                "sex": ["F"],
                "age": ["Y65"],
                "region": ["PT"],
                "year": [2020],
                "value": ["21.5"],
            }
        )

        result = main(region=Region.PT)

        assert isinstance(result, pd.DataFrame)
        mock_to_csv.assert_called_once()
