import pandas as pd
import numpy as np
import logging
import os
import json
import time
import xmltodict


class TrackingDataTransformer:
    config = None
    logger = None
    data_source = None
    match_date = None

    def __init__(self):
        #self.config = config
        self.logger = logging.getLogger('{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))

    def transform(self, df_track_players, df_crosses_label, df_crosses_output,
                  df_second_phase_set_pieces, df_player_names_raw, players_df_lineup,
                  opta_match_info, match_info, df_opta_events):
        """[summary]

        Args:
            df_track_players (pd.DataFrame): [track_players_df]
            df_crosses_label (pd.DataFrame): [crosses v4]
            df_crosses_output (pd.DataFrame): [Crosses Output]
            df_second_phase_set_pieces (pd.DataFrame): [Set Pieces with 2nd Phase Output]
            df_player_names_raw (pd.DataFrame): [player_names_raw]
            players_df_lineup (pd.DataFrame): [players_df_lineup]
            opta_match_info (dict): [opta_event_file_manipulation, is a dict see SecondSpectrumTransformer]
            match_info (dict): [match meta_data, is a dict see SecondSpectrumTransformer]
            df_opta_events (pd.DataFrame): [event_data opta_event_file_manipulation, see SecondSpectrumTransformer]

        Returns:
            pd.DataFrame: [tracking data set pieces]
            pd.DataFrame: [tracking data crosses]


        """
    @staticmethod
    def which_team_from_left_to_right(period):
        """[summary]

        Args:
            period (int): [description]

        Returns:
            str: [description]


        """

    @staticmethod
    def get_events_after_crosses(data_event_full, data_event_crosses):
        """[summary]

       Args:
           data_event_full (pd.DataFrame): [description]
           data_event_crosses (pd.DataFrame): [description]

       Returns:
           pd.DataFrame: [description]


       """

    @staticmethod
    def get_events_before_crosses(data_event_full, data_event_crosses):
        """[summary]

        Args:
            data_event_full (pd.DataFrame): [description]
            data_event_crosses (pd.DataFrame): [description]

        Returns:
            pd.DataFrame: [description]


        """
    @staticmethod
    def remove_first_letter (x):
        """[summary]

        Args:
            x (str): [description]

        Returns:
            str: [description]

        """
    @staticmethod
    def area_triangle (v1, v2, v3):
        """[summary]

        Args:
            v1 (np): [description]
            v2 (np): [description]
            v3 (np): [description]

        Returns:
            np: [description]

        """

    @staticmethod
    def largest_triangle_sampling (x_path, y_path, frame_path, temporary_x, temporary_y, temporary_frame,
                                   list_frames, current_player):
        """[summary]

        Args:

            x_path (): [description]
            y_path (): [description]
            frame_path (): [description]
            temporary_x (): [description]
            temporary_y (): [description]
            temporary_frame (): [description]
            list_frames (): [description]
            current_player (): [description]

        Returns:
            tuple: [description]

        """

    @staticmethod
    def distance_coordinates_centre_y(y):
        """[summary]

        Args:
            y (): [description]

        Returns:
            float: [description]

        """
    @staticmethod
    def distance_coordinates_start2(x, y, xcrosser, ycrosser):


