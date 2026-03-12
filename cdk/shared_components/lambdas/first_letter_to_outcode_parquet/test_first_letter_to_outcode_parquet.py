from unittest.mock import patch

import polars
import pytest
from first_letter_to_outcode_parquet import (
    ConflictingDuplicateUPRNError,
    IdenticalDuplicateUPRNError,
    check_duplicate_uprns,
)


def make_df(rows):
    return polars.DataFrame(
        rows,
        schema={
            "uprn": polars.Utf8,
            "postcode": polars.Utf8,
            "addressbase_source": polars.Utf8,
        },
    )


class TestCheckDuplicateUPRNs:
    def test_missing_uprn_column_raises(self):
        df = polars.DataFrame(
            [
                {
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                }
            ],
            schema={
                "postcode": polars.Utf8,
                "addressbase_source": polars.Utf8,
            },
        )
        with pytest.raises(polars.exceptions.ColumnNotFoundError):
            check_duplicate_uprns(df, "A")

    def test_no_duplicates_returns_unchanged(self):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "2",
                    "postcode": "AA1 1BB",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
            ]
        )
        result = check_duplicate_uprns(df, "A")
        assert result.equals(df)

    def test_identical_duplicates_are_deduplicated(self):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "2",
                    "postcode": "AA1 1BB",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
            ]
        )
        result = check_duplicate_uprns(df, "A")
        assert result["uprn"].to_list() == ["1", "2"]
        assert len(result) == 2

    @patch("first_letter_to_outcode_parquet.sentry_sdk")
    def test_identical_duplicates_report_to_sentry(self, mock_sentry):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
            ]
        )
        check_duplicate_uprns(df, "A")

        mock_sentry.new_scope.assert_called_once()
        scope = mock_sentry.new_scope().__enter__()
        scope.capture_exception.assert_called()
        exc = scope.capture_exception.call_args[0][0]
        assert isinstance(exc, IdenticalDuplicateUPRNError)

    def test_conflicting_duplicates_raise(self):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v2/addressbase_cleaned",
                },
            ]
        )
        with pytest.raises(
            ConflictingDuplicateUPRNError,
            match="1 UPRN has duplicated rows for first_letter=A Duplicate uprns are not in identical rows. This indicates a data integrity issue that needs investigation.",
        ):
            check_duplicate_uprns(df, "A")

    def test_conflicting_duplicates_message_includes_first_letter(self):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v2/addressbase_cleaned",
                },
            ]
        )
        with pytest.raises(
            ConflictingDuplicateUPRNError, match="first_letter=A"
        ):
            check_duplicate_uprns(df, "A")

    def test_mix_of_identical_and_conflicting_raises(self):
        """If even one UPRN has conflicting data, it should raise."""
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "2",
                    "postcode": "AA1 1BB",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "2",
                    "postcode": "AA1 1BB",
                    "addressbase_source": "s3://path/to/v2/addressbase_cleaned",
                },
            ]
        )
        with pytest.raises(ConflictingDuplicateUPRNError):
            check_duplicate_uprns(df, "A")

    def test_empty_dataframe(self):
        df = make_df([])
        result = check_duplicate_uprns(df, "A")
        assert len(result) == 0

    def test_single_row(self):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                }
            ]
        )
        result = check_duplicate_uprns(df, "A")
        assert result.equals(df)

    def test_multiple_uprns_all_with_identical_duplicates(self):
        df = make_df(
            [
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "1",
                    "postcode": "AA1 1AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "2",
                    "postcode": "AA1 1BB",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "2",
                    "postcode": "AA1 1BB",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
                {
                    "uprn": "3",
                    "postcode": "SW1A 3AA",
                    "addressbase_source": "s3://path/to/v1/addressbase_cleaned",
                },
            ]
        )
        result = check_duplicate_uprns(df, "A")
        assert result["uprn"].n_unique() == 3
        assert len(result) == 3
