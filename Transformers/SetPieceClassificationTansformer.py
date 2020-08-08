import pandas as pd
import logging
import os

class SetPieceClassificationTansformer:
    config = None
    logger = None
    data_source = None
    match_date = None

    def __init__(self):
        #self.config = config
        self.logger = logging.getLogger('{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))

    def set_piece_summary(self, df_opta_output_final_freekicks, df_opta_output_shots_freekicks, df_opta_output_aerial_duels_freekicks, df_opta_output_final_corners, df_opta_output_shots_corners, df_opta_output_aerial_duels_corners)-> (pd.DataFrame, pd.DataFrame, pd.DataFrame):


    def set_piece_classification (self, df_opta_events, match_info, opta_match_info, df_opta_crosses, df_opta_shots)-> (pd.DataFrame, pd.DataFrame, pd.DataFrame):


    def corners_classification (self, df_opta_events, match_info, opta_match_info, df_opta_crosses, df_opta_shots) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
