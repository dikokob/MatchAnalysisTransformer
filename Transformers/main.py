import os
import glob
import pandas as pd
import json

from SpectrumMatchAnalysisTransformer import SpectrumMatchAnalysisTransformer
from OPTATransformer import OPTATransformer
from SetPieceClassificationTansformer import SetPieceClassificationTansformer
from TrackingDataTransformer import TrackingDataTransformer

spectrumTransformer = SpectrumMatchAnalysisTransformer('SpectrumMatchAnalysis')
optaTransformer = OPTATransformer()
setPieceTransformer = SetPieceClassificationTansformer()
trackingDataTransformer = TrackingDataTransformer()

if __name__ == "__main__":

    script_dir = os.path.dirname(os.path.realpath(__file__))
    # we should always try and run from root directory here
    # os path will deal with the windows paths
    input_data_dir = os.path.join('data', 'raw_data')
    processed_data_dir = os.path.join('data', 'processed')
    # Loop through all rawdata
    for session in os.listdir(input_data_dir):


        print('Processing Session: {}'.format(session))
        # Set processed folder
        processed_folder = os.path.join(processed_data_dir, session)

        # Get all files for the specific session
        files_folder = os.path.join(input_data_dir, session, '*')
        files = glob.glob(files_folder)
        print("files = {}".format(files))
        # ===========================================================================================================================

        # Process Spectrum Match Analysis
        df_track_players, df_player_names_raw, players_df_lineup, match_info , df_opta_events, opta_match_info, \
        df_time_possession  = [None for i in range(7)]

        # Get processed files 
        try:
            df_track_players = pd.read_csv(os.path.join(processed_folder,'df_track_players.csv'))
            df_player_names_raw = pd.read_csv(os.path.join(processed_folder, 'df_player_names_raw.csv'))
            players_df_lineup = pd.read_csv(os.path.join(processed_folder, 'players_df_lineup.csv'))
            df_opta_events = pd.read_csv(os.path.join(processed_folder, 'df_opta_events.csv'))
            df_time_possession = pd.read_csv(os.path.join(processed_folder, 'df_time_possession.csv'))

            with open(os.path.join(processed_folder,'match_info.json'), 'r') as f:
                match_info = json.load(f)

            with open(os.path.join(processed_folder,'opta_match_info.json'), 'r') as f:
                opta_match_info = json.load(f)

        except Exception as e:
            print('Getting raw processed files from local, however failed to load them: {}'.format(e))

        # If processed files don't exsit process raw
        if any(x is None for x in [df_track_players, df_player_names_raw, players_df_lineup, match_info ,
                                        df_opta_events, opta_match_info,
                                        df_time_possession]):
            (
                df_track_players,
                df_player_names_raw,
                players_df_lineup,
                match_info,
                df_opta_events,
                opta_match_info,
                df_time_possession
            ) = spectrumTransformer.transform(session, files)

            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)

            df_track_players.to_csv(os.path.join(processed_folder, 'df_track_players.csv'), index=False)
            df_player_names_raw.to_csv(os.path.join(processed_folder, 'df_player_names_raw.csv'), index=False)
            players_df_lineup.to_csv(os.path.join(processed_folder, 'players_df_lineup.csv'), index=False)
            df_opta_events.to_csv(os.path.join(processed_folder, 'df_opta_events.csv'), index=False)
            df_time_possession.to_csv(os.path.join(processed_folder, 'df_time_possession.csv'), index=False)

            with open(os.path.join(processed_folder, 'match_info.json'), 'w') as fp:
                json.dump(match_info, fp)

            with open(os.path.join(processed_folder, 'opta_match_info.json'), 'w') as fp:
                json.dump(opta_match_info, fp)

        # ===========================================================================================================================

        df_opta_core_stats, df_opta_core_stats_time_possession, \
        df_opta_core_stats_time_possession_from_event, df_opta_crosses, df_opta_shots= [None for i in range(5)]

        # Get processed files 
        try:
            df_opta_core_stats = pd.read_csv(os.path.join(processed_folder, 'df_opta_core_stats.csv'))
            df_opta_core_stats_time_possession = pd.read_csv(os.path.join(processed_folder, 'df_opta_core_stats_time_possession.csv'))
            df_opta_core_stats_time_possession_from_event = pd.read_csv(os.path.join(processed_folder, 'df_opta_core_stats_time_possession_from_event.csv'))
            df_opta_crosses = pd.read_csv(os.path.join(processed_folder, 'df_opta_crosses.csv'))
            df_opta_shots = pd.read_csv(os.path.join(processed_folder, 'df_opta_shots.csv'))
            #df_crosses_label = pd.read_csv(os.path.join(processed_folder, 'df_crosses_label.csv'))

        except Exception as e:
            print('Getting raw processed files from local, however failed to load them: {}'.format(e))

        # If processed files don't exsit process
        if any(x is None for x in [df_opta_core_stats, df_opta_core_stats_time_possession, df_opta_core_stats_time_possession_from_event, df_opta_crosses, df_opta_shots]):

            df_opta_core_stats = optaTransformer.opta_core_stats(df_opta_events, opta_match_info, df_player_names_raw, players_df_lineup, match_info)
            df_opta_core_stats_time_possession = optaTransformer.opta_core_stats_with_time_possession(df_time_possession, df_opta_core_stats)
            df_opta_core_stats_time_possession_from_event = optaTransformer.opta_core_stats_with_time_possession_from_events(df_opta_core_stats, df_opta_events)
            df_opta_crosses = optaTransformer.opta_crosses_classification(df_opta_events)
            df_opta_shots = optaTransformer.opta_shots(df_opta_events, df_player_names_raw, opta_match_info)
            #df_crosses_label = optaTransformer.cross_label(df_opta_events)

            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)

            df_opta_core_stats.to_csv(os.path.join(processed_folder, 'df_opta_core_stats.csv'), index=False)
            df_opta_core_stats_time_possession.to_csv(os.path.join(processed_folder, 'df_opta_core_stats_time_possession.csv'), index=False)
            df_opta_core_stats_time_possession_from_event.to_csv(os.path.join(processed_folder, 'df_opta_core_stats_time_possession_from_event.csv'), index=False)
            df_opta_crosses.to_csv(os.path.join(processed_folder, 'df_opta_crosses.csv'), index=False)
            df_opta_shots.to_csv(os.path.join(processed_folder, 'df_opta_shots.csv'), index=False)
            #df_crosses_label.to_csv(os.path.join(processed_folder, 'df_crosses_label.csv'), index=False)

        # ===========================================================================================================================

        df_opta_output_final_corners, df_opta_output_shots_corners, df_opta_output_aerial_duels_corners, \
        df_opta_output_final_freekicks, df_opta_output_shots_freekicks, df_opta_output_aerial_duels_freekicks, \
        df_set_pieces, df_shots_from_set_pieces, df_aerial_duels_from_set_pieces, \
        df_crosses_output, df_second_phase_set_pieces, df_second_phase_set_pieces_shots, \
        df_shots_from_crosses, df_aerial_duels_from_crosses = \
            [None for i in range(14)]

        # Get processed files
        try:
            df_opta_output_final_corners = pd.read_csv(os.path.join(processed_folder, 'df_opta_output_final_corners.csv'))
            df_opta_output_shots_corners = pd.read_csv(os.path.join(processed_folder, 'df_opta_output_shots_corners.csv'))
            df_opta_output_aerial_duels_corners = pd.read_csv(os.path.join(processed_folder, 'df_opta_output_aerial_duels_corners.csv'))
            df_opta_output_final_freekicks = pd.read_csv(os.path.join(processed_folder, 'df_opta_output_final_freekicks.csv'))
            df_opta_output_shots_freekicks = pd.read_csv(os.path.join(processed_folder, 'df_opta_output_shots_freekicks.csv'))
            df_opta_output_aerial_duels_freekicks = pd.read_csv(os.path.join(processed_folder, 'df_opta_output_aerial_duels_freekicks.csv'))
            df_set_pieces = pd.read_csv(os.path.join(processed_folder, 'df_set_pieces.csv'))
            df_shots_from_set_pieces = pd.read_csv(os.path.join(processed_folder, 'df_shots_from_set_pieces.csv'))
            df_aerial_duels_from_set_pieces = pd.read_csv(os.path.join(processed_folder, 'df_aerial_duels_from_set_pieces.csv'))
            df_crosses_output = pd.read_csv(os.path.join(processed_folder, 'df_crosses_output.csv'))
            df_second_phase_set_pieces = pd.read_csv(os.path.join(processed_folder, 'df_second_phase_set_pieces.csv'))
            df_second_phase_set_pieces_shots = pd.read_csv(os.path.join(processed_folder, 'df_second_phase_set_pieces_shots.csv'))
            df_shots_from_crosses = pd.read_csv(os.path.join(processed_folder, 'df_shots_from_crosses.csv'))
            df_aerial_duels_from_crosse = pd.read_csv(os.path.join(processed_folder, 'df_aerial_duels_from_crosses.csv'))

        except Exception as e:
            print('Getting raw processed files from local, however failed to load them: {}'.format(e))

        # If processed files don't exsit process
        if any(x is None for x in [df_opta_output_final_corners, df_opta_output_shots_corners,
                                   df_opta_output_aerial_duels_corners, df_opta_output_final_freekicks,
                                   df_opta_output_shots_freekicks, df_opta_output_aerial_duels_freekicks,
                                   df_set_pieces, df_shots_from_set_pieces, df_aerial_duels_from_set_pieces]):

            df_opta_output_final_corners, df_opta_output_shots_corners, df_opta_output_aerial_duels_corners = \
                setPieceTransformer.corners_classification(df_opta_events, df_player_names_raw, match_info,
                                                           opta_match_info, df_opta_crosses, df_opta_shots)

            df_opta_output_final_freekicks, df_opta_output_shots_freekicks, df_opta_output_aerial_duels_freekicks = \
                setPieceTransformer.set_piece_classification(df_opta_events, match_info, opta_match_info,
                                                             df_opta_crosses, df_opta_shots, df_player_names_raw)

            df_set_pieces, df_shots_from_set_pieces, df_aerial_duels_from_set_pieces = setPieceTransformer.\
                extract_set_piece_statistics(df_opta_output_final_freekicks, df_opta_output_shots_freekicks,
                                             df_opta_output_aerial_duels_freekicks, df_opta_output_final_corners,
                                             df_opta_output_shots_corners, df_opta_output_aerial_duels_corners,
                                             opta_match_info)

            df_crosses_output, df_second_phase_set_pieces, df_second_phase_set_pieces_shots, \
            df_shots_from_crosses, df_aerial_duels_from_crosses = \
            setPieceTransformer.crosses_contextualisation(match_info, opta_match_info, df_player_names_raw,
                                                          df_opta_events, df_opta_crosses, df_opta_shots,
                                                          df_set_pieces, df_shots_from_set_pieces)


            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)

            df_opta_output_final_corners.to_csv(os.path.join(processed_folder, 'df_opta_output_final_corners.csv'), index=False)
            df_opta_output_shots_corners.to_csv(os.path.join(processed_folder, 'df_opta_output_shots_corners.csv'), index=False)
            df_opta_output_aerial_duels_corners.to_csv(os.path.join(processed_folder, 'df_opta_output_aerial_duels_corners.csv'), index=False)
            df_opta_output_final_freekicks.to_csv(os.path.join(processed_folder, 'df_opta_output_final_freekicks.csv'), index=False)
            df_opta_output_shots_freekicks.to_csv(os.path.join(processed_folder, 'df_opta_output_shots_freekicks.csv'), index=False)
            df_opta_output_aerial_duels_freekicks.to_csv(os.path.join(processed_folder, 'df_opta_output_aerial_duels_freekicks.csv'), index=False)
            df_set_pieces.to_csv(os.path.join(processed_folder, 'df_set_pieces.csv'), index=False)
            df_shots_from_set_pieces.to_csv(os.path.join(processed_folder, 'df_shots_from_set_pieces.csv'), index=False)
            df_aerial_duels_from_set_pieces.to_csv(os.path.join(processed_folder, 'df_aerial_duels_from_set_pieces.csv'), index=False)
            df_crosses_output.to_csv(os.path.join(processed_folder, 'df_crosses_output.csv'), index=False)
            df_second_phase_set_pieces.to_csv(os.path.join(processed_folder, 'df_second_phase_set_pieces.csv'), index=False)
            df_second_phase_set_pieces_shots.to_csv(os.path.join(processed_folder, 'df_second_phase_set_pieces_shots.csv'), index=False)
            df_shots_from_crosses.to_csv(os.path.join(processed_folder, 'df_shots_from_crosses.csv'), index=False)
            df_aerial_duels_from_crosses.to_csv(os.path.join(processed_folder, 'df_aerial_duels_from_crosses.csv'), index=False)


        # ===========================================================================================================================

        df_tracking_data_set_pieces, df_tracking_data_crosses = [None for i in range(2)]
        
        # Get processed files 
        try:
            df_tracking_data_set_pieces = pd.read_csv(os.path.join(processed_folder,'df_tracking_data_set_pieces.csv'))
            df_tracking_data_crosses = pd.read_csv(os.path.join(processed_folder, 'df_tracking_data_crosses.csv'))
            print(df_tracking_data_crosses.head())
        except Exception as e:
            print('Getting raw processed files from local, however failed to load them: {}'.format(e))

        # If processed files don't exsit process
        if any(x is None for x in [df_tracking_data_set_pieces,df_tracking_data_crosses]):

            df_tracking_data_set_pieces = trackingDataTransformer.transform(df_track_players,df_opta_crosses,df_crosses_output,df_second_phase_set_pieces,df_player_names_raw,players_df_lineup,opta_match_info,match_info,df_opta_events,'Set Piece')
            df_tracking_data_crosses = trackingDataTransformer.transform(df_track_players,df_opta_crosses,df_crosses_output,df_second_phase_set_pieces,df_player_names_raw,players_df_lineup,opta_match_info,match_info,df_opta_events,'Cross')

            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)

            print(df_tracking_data_crosses.head())
            
            df_tracking_data_set_pieces.to_csv(os.path.join(processed_folder, 'df_tracking_data_set_pieces.csv'), index=False)
            df_tracking_data_crosses.to_csv(os.path.join(processed_folder, 'df_tracking_data_crosses.csv'), index=False)
               
