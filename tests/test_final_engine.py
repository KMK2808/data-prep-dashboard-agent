import unittest

import pandas as pd

from app.final_engine import CleaningOptions, apply_cleaning, default_column_configs, profile_dataframe


class FinalEngineTests(unittest.TestCase):
    def test_apply_cleaning_converts_and_deduplicates(self) -> None:
        raw_df = pd.DataFrame(
            {
                "amount": ["1,200", "1,200", "950"],
                "date_text": ["2025-01-01", "2025-01-01", "2025-01-02"],
                "status": [" Delivered ", " Delivered ", None],
            }
        )
        cleaned_df, operations = apply_cleaning(raw_df, CleaningOptions(), default_column_configs(raw_df))
        self.assertEqual(len(cleaned_df), 2)
        self.assertTrue(pd.api.types.is_numeric_dtype(cleaned_df["amount"]))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df["date_text"]))
        self.assertIn("Removed 1 duplicate rows.", operations)

    def test_profile_has_quality_score(self) -> None:
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "x", "y"]})
        profile = profile_dataframe(df)
        self.assertIn("quality_score", profile)
        self.assertLessEqual(profile["quality_score"], 100)


if __name__ == "__main__":
    unittest.main()
