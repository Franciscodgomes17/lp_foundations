from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
from . import FIXTURES_DIR
from life_expectancy.cleaning import load_data, save_data, main
def test_load_data_reads_expected_tsv(eu_life_expectancy_raw_sample):
    """Ensure load_data loads the expected DataFrame from a valid path."""
    df = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
    assert_frame_equal(df, eu_life_expectancy_raw_sample)
def test_save_data_calls_to_csv_once():
    """Ensure save_data calls DataFrame.to_csv without writing a file."""
    df = pd.DataFrame({"a": [1]})
    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        save_data(df)
        mock_to_csv.assert_called_once()
def test_main_returns_dataframe_and_saves_once():
    """Ensure main returns a DataFrame and triggers saving."""
    with patch("life_expectancy.cleaning.load_data") as mock_load, \
         patch("pandas.DataFrame.to_csv") as mock_to_csv:

        mock_load.return_value = pd.DataFrame(
            {"metadata": ["YR,F,Y65,PT"], "2020": ["21.5"]}
        )
        result = main(country="PT")
        assert isinstance(result, pd.DataFrame)
        mock_to_csv.assert_called_once()
        