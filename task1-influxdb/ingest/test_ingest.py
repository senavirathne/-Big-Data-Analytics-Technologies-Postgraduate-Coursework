import os
import unittest
from unittest.mock import patch

import ingest


class IngestionTests(unittest.TestCase):
    def test_assigned_url_cannot_be_replaced(self) -> None:
        with patch.dict(os.environ, {"WIND_CSV_URL": "https://example.invalid/other.csv"}):
            with self.assertRaisesRegex(RuntimeError, "alternate datasets are not accepted"):
                ingest.source_url()

    def test_expected_source_contract_is_explicit(self) -> None:
        self.assertEqual(["ts", "ws", "wd"], ingest.EXPECTED_COLUMNS)
        self.assertEqual("PAFA", ingest.STATION)
        self.assertEqual(345_587, ingest.EXPECTED_RECORD_COUNT)

    def test_original_timestamp_and_source_fields_are_preserved(self) -> None:
        line = ingest.point_for_row(
            {"ts": "1980-01-01 08:00:00", "ws": "2.88", "wd": "40.00"}
        ).to_line_protocol()

        self.assertIn("airport_wind,station=PAFA", line)
        self.assertIn("ws=2.88", line)
        self.assertIn("wd=40", line)
        self.assertTrue(line.endswith("315561600000000000"), line)

    def test_missing_direction_omits_only_that_field(self) -> None:
        line = ingest.point_for_row(
            {"ts": "1996-07-23 20:00:00", "ws": "5.44", "wd": ""}
        ).to_line_protocol()

        self.assertIn("ws=5.44", line)
        self.assertNotIn("wd=", line)

    def test_invalid_historical_timestamp_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid original timestamp"):
            ingest.point_for_row({"ts": "1980-01", "ws": "2.88", "wd": "40.00"})


if __name__ == "__main__":
    unittest.main()
