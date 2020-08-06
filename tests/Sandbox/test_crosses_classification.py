"""
    TestGetShots: Verification tests to base OptaTransformer.opta_shots tests from
"""
import pandas as pd
from sandbox.crosses_classification import crosses_classification

class TestCrossesClassification:
    """Test the get shots functionality from set_pieces_classification
    """
    path_match_results = 'tests/fixtures/inputs/g1059702/srml-8-2019-f1059702-matchresults.xml'
    path_events = 'tests/fixtures/inputs/g1059702/f24-8-2019-1059702-eventdetails.xml'

    def test_opta_cross_classification_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """
        output = crosses_classification(self.path_events)

        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_opta_cross_classification_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'competition_id', 'competition_name', 'season_id', 'season_name',
            'game_id', 'match_day', 'game_date', 'cross_to_remove',
            'out_of_pitch', 'ending_too_wide', 'OPTA Pull Back Qualifier'
        ]
        output = crosses_classification(self.path_events)

        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_opta_cross_classification_output_columns_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/outputs/g1059702/df_crosses_classification.csv", index_col=None)

        output = crosses_classification(self.path_events)

        assert all(verification_result.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_opta_cross_classification_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/outputs/g1059702/df_crosses_classification.csv", index_col=None)

        output = crosses_classification(self.path_events)

        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )
