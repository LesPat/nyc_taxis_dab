from unittest.mock import MagicMock, patch

from nyc_taxis_dab import taxis


@patch("nyc_taxis_dab.taxis.read_s3_mixed")
def test_load_taxi_fares_uses_mixed_reader(mock_read_mixed):
    mock_read_mixed.return_value = MagicMock()

    taxis.load_taxi_fares("s3://nyc-taxis-traffic-analysis-raw/stg_taxis_data/")

    mock_read_mixed.assert_called_once_with("s3://nyc-taxis-traffic-analysis-raw/stg_taxis_data/")
