"""
    TestGetShots: Verification tests to base OptaTransformer.opta_shots tests from
"""
import pandas as pd
from sandbox.set_pieces_classification import get_shots, corners_classification

class TestGetShots:
    """Test the get shots functionality from set_pieces_classification
    """
    path_match_results = 'tests/fixtures/crosses_loop_in_tracking/inputs/g1059702/srml-8-2019-f1059702-matchresults.xml'
    path_events = 'tests/fixtures/crosses_loop_in_tracking/inputs/g1059702/f24-8-2019-1059702-eventdetails.xml'
    path_metadata = 'tests/fixtures/crosses_loop_in_tracking/inputs/g1059702/g1059702_SecondSpectrum_Metadata.json'
    season = '2019-20'
    competition ='FA Cup'

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

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/g1059702/df_opta_shots.csv", index_col=None)

        output = get_shots(self.path_events, self.path_match_results)

        assert all(verification_result.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_get_shots_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/g1059702/df_opta_shots.csv", index_col=None)

        output = get_shots(self.path_events, self.path_match_results)

        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )


    def test_corners_classification(self):

        
        (
            opta_output_final_corners,
            opta_output_shots_corners,
            opta_output_aerial_duels_corners
        ) = corners_classification(
            path_events='tests/fixtures/set_piece_classification/inputs/g1059702/f24-8-2019-1059702-eventdetails.xml',
            path_match_results=self.path_match_results,
            path_track_meta=self.path_metadata,
            season=self.season,
            competition=self.competition
        )

        # base_path = 'tests/fixtures/set_piece_classification/outputs/g1059702'
        # opta_output_final_corners.to_csv(os.path.join(base_path, 'df_final_corners.csv'), index=False)
        # opta_output_shots_corners.to_csv(os.path.join(base_path, 'df_shots_corners.csv'), index=False)
        # opta_output_aerial_duels_corners.to_csv(os.path.join(base_path, 'df_aerial_duels_corners.csv'), index=False)
        print("final corners=\t{}\nshots_corners=\t{}\naerial duels corners=\t{}".format(
            opta_output_final_corners,
            opta_output_shots_corners,
            opta_output_aerial_duels_corners
        ))
        raise Exception("Fake issue")