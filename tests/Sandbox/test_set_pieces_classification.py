"""
    TestGetShots: Verification tests to base OptaTransformer.opta_shots tests from
"""
import os
import pandas as pd
import numpy as np
from sandbox.set_pieces_classification import get_shots, corners_classification

from tabulate import tabulate

def pprint_dataframe(df):
    """[summary]

    Args:
        df ([type]): [description]
    """
    print(tabulate(df))

class TestGetShots:
    """Test the get shots functionality from set_pieces_classification
    """
    path_match_results = 'tests/fixtures/crosses_loop_in_tracking/inputs/srml-8-2019-f1059702-matchresults.xml'
    path_events = 'tests/fixtures/crosses_loop_in_tracking/inputs/f24-8-2019-1059702-eventdetails.xml'
    path_metadata = 'tests/fixtures/crosses_loop_in_tracking/inputs/g1059702_SecondSpectrum_Metadata.json'
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

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_opta_shots.csv", index_col=None)

        output = get_shots(self.path_events, self.path_match_results)

        assert all(verification_result.columns == output.columns), (
            "Failed: did not return required output dataframe"
        )

    def test_get_shots_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/crosses_loop_in_tracking/outputs/df_opta_shots.csv", index_col=None)

        output = get_shots(self.path_events, self.path_match_results)

        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )

    def test_corners_classification_success_building_final_corners_w_correct_columns(self):

        
        (
            opta_output_final_corners,
            _,
            _
        ) = corners_classification(
            # path_events='data/raw_data/g1059702/f24-8-2019-1059702-eventdetails.xml',
            path_events='tests/fixtures/set_piece_classification/inputs/test_corners_final_corners.xml',
            path_match_results=self.path_match_results,
            path_track_meta=self.path_metadata,
            season=self.season,
            competition=self.competition
        )

        base_path = 'tests/fixtures/set_piece_classification/outputs'
        verified_df = pd.read_csv(os.path.join(base_path, 'df_final_corners.csv'), index_col=None)
        print("final corners=\t{}\nverified_df=\t{}".format(
            opta_output_final_corners,
            verified_df
        ))

        assert all((opta_output_final_corners.columns == verified_df.columns)),(
            "Failed: Building opta_output_final_corners dataframe with incorrect columns."
        )


    def test_corners_classification_success_building_final_corners(self):

        
        (
            opta_output_final_corners,
            _,
            _
        ) = corners_classification(
            path_events='tests/fixtures/set_piece_classification/inputs/test_corners_final_corners.xml',
            path_match_results=self.path_match_results,
            path_track_meta=self.path_metadata,
            season=self.season,
            competition=self.competition
        )
        opta_output_final_corners = opta_output_final_corners.replace([' ', '', np.nan, None], 'nan')
        
        base_path = 'tests/fixtures/set_piece_classification/outputs'
        # opta_output_final_corners.to_csv(os.path.join(base_path, 'df_final_corners.csv'), index=False)
        verified_df = pd.read_csv(os.path.join(base_path, 'df_final_corners.csv'), index_col=None)
        verified_df = verified_df.replace([' ', '', np.nan, None], 'nan')
        print("final corners=\t{}\nverified_df=\t{}".format(
            opta_output_final_corners,
            verified_df
        ))
        assert (opta_output_final_corners.equals(verified_df)),(
            "Failed: Building opta_output_final_corners dataframe"
        )

    def test_corners_classification_success_building_shots_corners(self):

        
        (
            _,
            opta_output_shots_corners,
            _
        ) = corners_classification(
            path_events='tests/fixtures/set_piece_classification/inputs/test_corners_shots.xml',
            path_match_results=self.path_match_results,
            path_track_meta=self.path_metadata,
            season=self.season,
            competition=self.competition
        )

        base_path = 'tests/fixtures/set_piece_classification/outputs'
        # opta_output_shots_corners.to_csv(os.path.join(base_path, 'df_shots_corners.csv'), index=False)
        verified_df = pd.read_csv(os.path.join(base_path, 'df_shots_corners.csv'), index_col=None)
        print("final corners=\t{}\nverified_df=\t{}".format(
            opta_output_shots_corners,
            verified_df
        ))
        assert (opta_output_shots_corners.equals(verified_df)),(
            "Failed: Building opta_output_shots_corners dataframe"
        )

    def test_corners_classification_success_building_aerial_duel_corners_dataframe(self):

        
        (
            _,
            _,
            opta_output_aerial_duels_corners
        ) = corners_classification(
            # path_events='data/raw_data/g1059702/f24-8-2019-1059702-eventdetails.xml',
            path_events='tests/fixtures/set_piece_classification/inputs/test_corners_aerial_duel.xml',
            path_match_results=self.path_match_results,
            path_track_meta=self.path_metadata,
            season=self.season,
            competition=self.competition
        )
        opta_output_aerial_duels_corners = opta_output_aerial_duels_corners.replace([' ', '', np.nan, None], 'nan')
        base_path = 'tests/fixtures/set_piece_classification/outputs'
        # opta_output_aerial_duels_corners.to_csv(os.path.join(base_path, 'df_aerial_duels_corners.csv'), index=False)
        verified_df = pd.read_csv(os.path.join(base_path, 'df_aerial_duels_corners.csv'), index_col=None)
        verified_df = verified_df.replace([' ', '', np.nan, None], 'nan')
        print("aerial duels corners=\t{}\nverfification=\t{}".format(
            opta_output_aerial_duels_corners,
            verified_df
        ))
        assert opta_output_aerial_duels_corners.equals(verified_df),(
            "Failed: Building aerial deils corners dataframe"
        )
