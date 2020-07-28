from SpectrumMatchAnalysisTransformer import SpectrumMatchAnalysisTransformer
from OPTATransformer import OPTATransformer
import os
import glob
import pandas as pd
import json


spectrumTransformer = SpectrumMatchAnalysisTransformer('SpectrumMatchAnalysis')
optaTransformer = OPTATransformer()

if __name__ == "__main__":

    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Loop through all rawdata
    for session in os.listdir(os.path.abspath(os.path.join(script_dir, 'rawdata'))):

        
        print('Processing Session: {}'.format(session))  
        # Set processed folder
        processed_folder = '{}/processed/{}'.format(script_dir, session)

        # Get all files for the specific session
        files_folder = os.path.abspath(os.path.join(script_dir, 'rawdata\{}\*'.format(session)))
        files = glob.glob(files_folder)

        # ===========================================================================================================================

        # Process Spectrum Match Analysis
        df_track_players, df_player_names_raw, players_df_lineup, match_info , df_opta_events, opta_match_info, \
        df_time_possession  = [None for i in range(7)] 

        # Get processed files 
        try:            
            df_track_players = pd.read_csv(processed_folder+'/df_track_players.csv').iloc[:,1:]
            df_player_names_raw = pd.read_csv(processed_folder+'/df_player_names_raw.csv').iloc[:,1:]
            players_df_lineup = pd.read_csv(processed_folder+'/players_df_lineup.csv').iloc[:,1:]
            df_opta_events = pd.read_csv(processed_folder+'/df_opta_events.csv').iloc[:,1:]
            df_time_possession = pd.read_csv(processed_folder+'/df_time_possession.csv').iloc[:,1:]
            with open(processed_folder+'/match_info.json') as f: match_info = json.load(f)
            with open(processed_folder+'/opta_match_info.json') as f: opta_match_info = json.load(f)

        except Exception as e:
            print('Getting raw processed files from local, however failed to load them: {}'.format(e))

        # If processed files don't exsit process raw
        if any(x is None for x in [df_track_players, df_player_names_raw, players_df_lineup, match_info ,
                                        df_opta_events, opta_match_info,
                                        df_time_possession]):
            df_track_players, df_player_names_raw, players_df_lineup, match_info , df_opta_events, opta_match_info, \
            df_time_possession = spectrumTransformer.transform(session, files)
    
            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)

            df_track_players.to_csv(processed_folder+'/df_track_players.csv')
            df_player_names_raw.to_csv(processed_folder+'/df_player_names_raw.csv')
            players_df_lineup.to_csv(processed_folder+'/players_df_lineup.csv')
            df_opta_events.to_csv(processed_folder+'/df_opta_events.csv')
            df_time_possession.to_csv(processed_folder+'/df_time_possession.csv')
            with open(processed_folder+'/match_info.json', 'w') as fp: json.dump(match_info, fp)
            with open(processed_folder+'/opta_match_info.json', 'w') as fp: json.dump(opta_match_info, fp)

        # ===========================================================================================================================

        # Process OPTA infomation
        df_opta_core_stats, df_opta_core_stats_time_possession, df_opta_crosses, df_opta_shots  = [None for i in range(4)]
        
        # Get processed files 
        try:            
            df_opta_core_stats = pd.read_csv(processed_folder+'/df_opta_core_stats.csv').iloc[:,1:]
            df_opta_core_stats_time_possession = pd.read_csv(processed_folder+'/df_opta_core_stats_time_possession.csv').iloc[:,1:]
            df_opta_crosses = pd.read_csv(processed_folder+'/df_opta_crosses.csv').iloc[:,1:]
            df_opta_shots = pd.read_csv(processed_folder+'/df_opta_shots.csv').iloc[:,1:]
            
        except Exception as e:
            print('Getting raw processed files from local, however failed to load them: {}'.format(e))

        # If processed files don't exsit process
        if any(x is None for x in [df_opta_core_stats, df_opta_core_stats_time_possession, df_opta_crosses, df_opta_shots]):

            df_opta_core_stats = optaTransformer.opta_core_stats(df_opta_events, opta_match_info, df_player_names_raw, players_df_lineup, match_info)
            df_opta_core_stats_time_possession = optaTransformer.opta_core_stats_with_time_possession(df_time_possession, df_opta_core_stats)
            df_opta_crosses = optaTransformer.opta_crosses_classification(df_opta_events)
            df_opta_shots = optaTransformer.opta_shots(df_opta_events, df_player_names_raw, opta_match_info)

            if not os.path.exists(processed_folder):
                os.makedirs(processed_folder)

            df_opta_core_stats.to_csv(processed_folder+'/df_opta_core_stats.csv')
            df_opta_core_stats_time_possession.to_csv(processed_folder+'/df_opta_core_stats_time_possession.csv')
            df_opta_crosses.to_csv(processed_folder+'/df_opta_crosses.csv')
            df_opta_shots.to_csv(processed_folder+'/df_opta_shots.csv')
        

       
       
        
