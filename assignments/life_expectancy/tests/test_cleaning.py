"""Tests for the cleaning module."""
import pandas as pd
from pandas.testing import assert_frame_equal
from life_expectancy.cleaning import clean_df
def test_clean_df_matches_expected_fixture(
    eu_life_expectancy_raw_sample: pd.DataFrame,
    pt_life_expectancy_expected: pd.DataFrame,
) -> None:
    """
    Clean the sample raw fixture and compare the resulting DataFrame
    to the expected PT fixture.
    """
    result = clean_df(eu_life_expectancy_raw_sample, country="PT")
    assert_frame_equal(
        result.reset_index(drop=True),
        pt_life_expectancy_expected.reset_index(drop=True),
    )