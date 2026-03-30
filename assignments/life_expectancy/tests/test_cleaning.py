"""Tests for the cleaning module."""

import pandas as pd
from pandas.testing import assert_frame_equal

from life_expectancy.cleaning import Cols, Region, clean_df, load_data
from . import FIXTURES_DIR


def test_clean_df_matches_expected_fixture(
    pt_life_expectancy_expected: pd.DataFrame,
) -> None:
    """
    Clean the sample TSV fixture and compare the result
    to the expected PT output fixture.
    """
    loaded = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
    result = clean_df(loaded, region=Region.PT)

    assert_frame_equal(
        result.reset_index(drop=True),
        pt_life_expectancy_expected.reset_index(drop=True),
    )


def test_clean_df_filters_json_region() -> None:
    """Ensure clean_df keeps only the selected region for JSON input."""
    loaded = load_data(FIXTURES_DIR / "eurostat_life_expect_sample.json")
    result = clean_df(loaded, region=Region.PT)

    assert list(result["region"]) == ["PT", "PT"]


def test_clean_df_returns_empty_dataframe_with_expected_columns() -> None:
    """Ensure empty inputs keep the expected normalized schema."""
    result = clean_df(pd.DataFrame(), region=Region.PT)

    assert list(result.columns) == Cols.CLEANED_COLS
    assert result.empty


def test_region_actual_countries_excludes_aggregate_regions() -> None:
    """Ensure aggregate regions are excluded from the country list."""
    countries = Region.actual_countries()

    assert Region.PT in countries
    assert Region.EU27_2020 not in countries
