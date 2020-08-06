"""
    TestGetShots: Verification tests to base OptaTransformer.opta_shots tests from
"""
import pandas as pd
from scripts_from_fed.opta_files_manipulation_functions import opta_event_file_manipulation
from sandbox.crosses_loop_in_tracking_data import crosses_classification, only_open_play_crosses

class TestCrossesClassification:
    """Test the get shots functionality from set_pieces_classification
    """
    path_match_results = 'tests/fixtures/inputs/g1059702/srml-8-2019-f1059702-matchresults.xml'
    path_events = 'tests/fixtures/inputs/g1059702/f24-8-2019-1059702-eventdetails.xml'

    def test_cross_classification_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """
        output = crosses_classification(self.path_events)

        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_cross_classification_output_contains_required_fields(self):
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

    def test_cross_classification_output_columns_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/outputs/g1059702/df_crosses_classification.csv", index_col=None)

        output = crosses_classification(self.path_events)

        assert all(verification_result.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_cross_classification_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/outputs/g1059702/df_crosses_classification.csv", index_col=None)

        output = crosses_classification(self.path_events)

        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )

    def test_only_open_play_crosses_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(only_open_play_crosses)
        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_only_open_play_crosses_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'competition_id', 'competition_name', 'season_id', 'season_name',
            'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
            'home_score', 'home_team_id', 'home_team_name', 'away_score',
            'away_team_id', 'away_team_name', 'fixture', 'result',
            'unique_event_id', 'event_id', 'period_id', 'min', 'sec', 'timestamp',
            'type_id', 'player_id', 'team_id', 'outcome', 'keypass', 'assist', 'x',
            'y', 'qualifier_id', 'value', 'open_play_cross'
        ]
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(only_open_play_crosses)

        print(output)
        print(output.columns)
        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_only_open_play_crosses_output_columns_as_expected(self):
        """test output is as expected
        """
        verification_df = pd.read_csv("tests/fixtures/outputs/g1059702/df_only_open_play_crosses.csv", index_col=None)
        verification_df = verification_df.where(pd.notnull(verification_df), None)
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(only_open_play_crosses)
        output = output.where(pd.notnull(output), None)

        assert all(verification_df.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_only_open_play_crosses_output_as_expected(self):
        """test output is as expected
        """

        verification_df = pd.read_csv("tests/fixtures/outputs/g1059702/df_only_open_play_crosses.csv", index_col=None)
        verification_df = verification_df.where(pd.notnull(verification_df), None)
        data, _, _, _, _, _, _, _, _ = opta_event_file_manipulation(self.path_events)

        output = data.groupby(['unique_event_id']).apply(only_open_play_crosses)
        output = output.where(pd.notnull(output), None)
        print("{}\n\n\n\n{}".format(verification_df.head().values, output.head().values))
        assert (verification_df.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )
