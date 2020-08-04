"""
    TestGetShots: Verification tests to base OptaTransformer.opta_shots tests from
"""
import pandas as pd
from sandbox.set_pieces_classification import get_shots

class TestGetShots:
    """Test the get shots functionality from set_pieces_classification
    """
    path_match_results = 'tests/fixtures/inputs/g1059702/srml-8-2019-f1059702-matchresults.xml'
    path_events = 'tests/fixtures/inputs/g1059702/f24-8-2019-1059702-eventdetails.xml'

    def test_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """
        output = get_shots(self.path_events, self.path_match_results)

        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'from_penalty',
            'on_target',
            'off_target'
        ]
        output = get_shots(self.path_events, self.path_match_results)

        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_get_shots_output_columns_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/outputs/g1059702/df_opta_shots.csv", index_col=None)

        output = get_shots(self.path_events, self.path_match_results)

        assert all(verification_result.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_get_shots_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/outputs/g1059702/df_opta_shots.csv", index_col=None)

        output = get_shots(self.path_events, self.path_match_results)

        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )
