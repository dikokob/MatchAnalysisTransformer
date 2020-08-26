"""
    TestSetPieceClassification: Verification tests to base OptaTransformer.
"""
import os
import pandas as pd
import numpy as np
from transformers.SpectrumMatchAnalysisTransformer import SpectrumMatchAnalysisTransformer
from transformers.OPTATransformer import OPTATransformer
from transformers.SetPieceClassificationTansformer import SetPieceClassificationTansformer

from tabulate import tabulate

def pprint_dataframe(df):
    """[summary]

    Args:
        df ([type]): [description]
    """
    print(tabulate(df))

class TestSetPieceClassification:
    """Test the get shots functionality from set_pieces_classification
    """
    path_match_results = 'tests/fixtures/srml-8-2019-f1059702-matchresults.xml'
    path_metadata = 'tests/fixtures/g1059702_SecondSpectrum_Metadata.json'
    
    path_squads_info = 'tests/fixtures/set_piece_classification/inputs/srml-8-2019-squads.xml'
    season = '2019-20'
    competition ='FA Cup'

    spectrumTransformer = SpectrumMatchAnalysisTransformer('SpectrumMatchAnalysis')
    opta_transformer = OPTATransformer()

    def get_set_piece_classification_transformer(self):


        return SetPieceClassificationTansformer()

    def test_get_opta_squad_data_success_on_load(self):

        spc_transformer = self.get_set_piece_classification_transformer()
        data_squads = spc_transformer.get_opta_squad_data(
            path_squads='tests/fixtures/set_piece_classification/inputs/test_squad_info_loads_successful.xml',
            num_of_squads=None
        )

        base_path = 'tests/fixtures/set_piece_classification/outputs'
        verified_df = pd.read_csv(os.path.join(base_path, 'df_squads_info.csv'), index_col=None)
        pprint_dataframe(data_squads)

        assert (data_squads.equals(verified_df)),(
            "Failed: squads info did was not loaded as expected"
        )

    def test_corners_classification_success_building_final_corners_w_correct_columns(self):


        path_events = 'tests/fixtures/set_piece_classification/inputs/test_corners_final_corners.xml'
        spc_transformer = self.get_set_piece_classification_transformer()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=path_events)
        df_player_names_raw, players_df_lineup, match_info = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.path_metadata
        )
        df_opta_shots = self.opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )
        df_opta_crosses = self.opta_transformer.opta_crosses_classification(
            df_opta_events=opta_event_data_df
        )
        (
            opta_output_final_corners,
            _,
            _
        ) = spc_transformer.corners_classification(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            match_info=match_info,
            opta_match_info=opta_match_info,
            df_opta_crosses=df_opta_crosses,
            df_opta_shots=df_opta_shots
        )
        
        # (
        #     opta_output_final_corners,
        #     _,
        #     _
        # ) = corners_classification(
        #     path_events='tests/fixtures/set_piece_classification/inputs/test_corners_final_corners.xml',
        #     path_match_results=self.path_match_results,
        #     path_track_meta=self.path_metadata,
        #     season=self.season,
        #     competition=self.competition
        # )

        base_path = 'tests/fixtures/set_piece_classification/outputs'
        verified_df = pd.read_csv(os.path.join(base_path, 'df_final_corners.csv'), index_col=None)
        print("final corners=\t{}\nverified_df=\t{}".format(
            opta_output_final_corners,
            verified_df
        ))

        assert all((opta_output_final_corners.columns == verified_df.columns)),(
            "Failed: Building opta_output_final_corners dataframe with incorrect columns."
        )


    # def test_corners_classification_success_building_final_corners(self):

        
    #     (
    #         opta_output_final_corners,
    #         _,
    #         _
    #     ) = corners_classification(
    #         path_events='tests/fixtures/set_piece_classification/inputs/test_corners_final_corners.xml',
    #         path_match_results=self.path_match_results,
    #         path_track_meta=self.path_metadata,
    #         season=self.season,
    #         competition=self.competition
    #     )
    #     opta_output_final_corners = opta_output_final_corners.replace([' ', '', np.nan, None], 'nan')
        
    #     base_path = 'tests/fixtures/set_piece_classification/outputs'
    #     verified_df = pd.read_csv(os.path.join(base_path, 'df_final_corners.csv'), index_col=None)
    #     verified_df = verified_df.replace([' ', '', np.nan, None], 'nan')
    #     print("final corners=\t{}\nverified_df=\t{}".format(
    #         opta_output_final_corners,
    #         verified_df
    #     ))
    #     assert (opta_output_final_corners.equals(verified_df)),(
    #         "Failed: Building opta_output_final_corners dataframe"
    #     )

    # def test_corners_classification_success_building_shots_corners(self):

        
    #     (
    #         _,
    #         opta_output_shots_corners,
    #         _
    #     ) = corners_classification(
    #         path_events='tests/fixtures/set_piece_classification/inputs/test_corners_shots.xml',
    #         path_match_results=self.path_match_results,
    #         path_track_meta=self.path_metadata,
    #         season=self.season,
    #         competition=self.competition
    #     )

    #     base_path = 'tests/fixtures/set_piece_classification/outputs'
    #     verified_df = pd.read_csv(os.path.join(base_path, 'df_shots_corners.csv'), index_col=None)
    #     print("final corners=\t{}\nverified_df=\t{}".format(
    #         opta_output_shots_corners,
    #         verified_df
    #     ))
    #     assert (opta_output_shots_corners.equals(verified_df)),(
    #         "Failed: Building opta_output_shots_corners dataframe"
    #     )

    # def test_corners_classification_success_building_aerial_duel_corners_dataframe(self):

        
    #     (
    #         _,
    #         _,
    #         opta_output_aerial_duels_corners
    #     ) = corners_classification(
    #         path_events='tests/fixtures/set_piece_classification/inputs/test_corners_aerial_duel.xml',
    #         path_match_results=self.path_match_results,
    #         path_track_meta=self.path_metadata,
    #         season=self.season,
    #         competition=self.competition
    #     )
    #     opta_output_aerial_duels_corners = opta_output_aerial_duels_corners.replace([' ', '', np.nan, None], 'nan')
    #     base_path = 'tests/fixtures/set_piece_classification/outputs'
    #     verified_df = pd.read_csv(os.path.join(base_path, 'df_aerial_duels_corners.csv'), index_col=None)
    #     verified_df = verified_df.replace([' ', '', np.nan, None], 'nan')
    #     print("aerial duels corners=\t{}\nverfification=\t{}".format(
    #         opta_output_aerial_duels_corners,
    #         verified_df
    #     ))
    #     assert opta_output_aerial_duels_corners.equals(verified_df),(
    #         "Failed: Building aerial deils corners dataframe"
    #     )
