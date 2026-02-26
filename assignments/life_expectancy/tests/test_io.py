from unittest.mock import patch
from pathlib import Path
import pandas as pd
from life_expectancy.cleaning import save_data
from life_expectancy.cleaning import load_data
from life_expectancy.cleaning import main
def test_load_data_returns_dataframe(eu_life_expectancy_raw_sample):
    """Ensure load_data loads a DataFrame from a valid path."""
    df = load_data(
        Path(__file__).resolve().parent / "fixtures" / "eu_life_expectancy_raw.tsv"
    )
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
def test_save_data_calls_to_csv():
    """Ensure save_data calls DataFrame.to_csv without writing a file."""
    df = pd.DataFrame({"a": [1]})
    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        save_data(df)
        assert mock_to_csv.called
def test_main_returns_dataframe_and_saves():
    """Ensure main returns a DataFrame and triggers saving."""
    with patch("life_expectancy.cleaning.load_data") as mock_load, \
         patch("pandas.DataFrame.to_csv") as mock_save:

        mock_load.return_value = pd.DataFrame({
            "metadata": ["YR,F,Y65,PT"],
            "2020": ["21.5"]
        })
        result = main(country="PT")
        assert isinstance(result, pd.DataFrame)
        assert mock_save.called