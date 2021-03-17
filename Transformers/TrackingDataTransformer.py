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
    event_data_filtered = None
    width = None
    length = None

    def __init__(self):
        #self.config = config
       self.logger = logging.getLogger('{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))

    def transform(self, df_track_players, df_crosses_label, df_crosses_output,
                  df_second_phase_set_pieces, df_player_names_raw, players_df_lineup,
                  opta_match_info, match_info, df_opta_events,event_type):
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
            event_type (str): [ event_type]
        Returns:
            pd.DataFrame: [tracking data]
           


        """

        #prep
        
        crosses_label = df_crosses_label
        crosses_output = df_crosses_output
        second_phase_set_pieces = df_second_phase_set_pieces
        players_lineup =  players_df_lineup
        match_info_opta =  opta_match_info
        match_information = match_info
        type_event = event_type

        game_id = opta_match_info['match_id']
        game_date = opta_match_info['match_date']

        track_players_df = df_track_players 
        player_names_raw = df_player_names_raw
        event_data = df_opta_events
        
        player_names_raw['@uID'] = 'p' + player_names_raw['player_id'].astype(str)
        player_names_raw['player_name'] = player_names_raw['full_name']
        players_df_lineup['@PlayerRef'] = players_df_lineup['player_id'].astype(str)
        players_df_lineup['@SubPosition'] = players_df_lineup['sub_position']
        players_df_lineup['@Position'] = players_df_lineup['position_in_pitch']
        players_df_lineup['@Status'] = players_df_lineup['status']
        track_players_df['game_id'] = opta_match_info['match_id']

        track_players_df['Is_Ball_Live'] = track_players_df['Is_Ball_Live'].astype(str).str.lower()

        if event_type=='Cross':
            all_crosses_file_filtered = df_crosses_label
            crosses_classified = df_crosses_output
            all_crosses_file_filtered = all_crosses_file_filtered.merge(crosses_classified[['OPTA Event ID', 'Cross Type']], 
            how = 'inner', left_on =['unique_event_id'], right_on =['OPTA Event ID']).drop(['OPTA Event ID'], axis = 1).reset_index(drop = True)

        
        if event_type == 'Set Piece':
            all_crosses_file_filtered = df_second_phase_set_pieces
            #all_crosses_file_filtered = all_crosses_file_filtered[all_crosses_file_filtered.game_id==sub.replace('g','f')].reset_index(drop=True)
            all_crosses_file_filtered['unique_event_id'] = np.where(all_crosses_file_filtered['Relevant OPTA Event ID'].isnull(), 
                all_crosses_file_filtered['OPTA Event ID'], all_crosses_file_filtered['Relevant OPTA Event ID'])
            all_crosses_file_filtered['player_id'] = np.where(all_crosses_file_filtered['Relevant OPTA Event ID'].isnull(), 
                all_crosses_file_filtered['Player ID'], all_crosses_file_filtered['Relevant Player ID'])
            all_crosses_file_filtered['player_id'] = list(map(lambda x: int(x.replace('p','')), all_crosses_file_filtered['player_id']))
            all_crosses_file_filtered['team_id'] = list(map(lambda x: int(x.replace('t','')), all_crosses_file_filtered['Attacking Team ID']))
            all_crosses_file_filtered['game_id'] = list(map(lambda x: int(x.replace('f','')), all_crosses_file_filtered['game_id']))
            all_crosses_file_filtered['Time_in_Seconds'] = np.where(all_crosses_file_filtered['Relevant OPTA Event ID'].isnull(), 
                all_crosses_file_filtered['min']*60 + all_crosses_file_filtered['sec'], 
                all_crosses_file_filtered['Relevant min']*60 + all_crosses_file_filtered['Relevant sec'])

        
        #we need the crosses_file anyways to make sure that set piece crosses are treated exactly in the same way as crosses
        crosses_file = df_crosses_label

        

        all_crosses_file_filtered['Time_in_Seconds'] = np.where(all_crosses_file_filtered['period_id']==1, all_crosses_file_filtered['Time_in_Seconds'], all_crosses_file_filtered['Time_in_Seconds'] - 45*60.0)

        track_players_df = track_players_df[track_players_df.Period_ID<=4].reset_index(drop=True) #make sure any penalty shootout is excluded, if present.


        #tracking_path = 'C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\987792.jsonl'
        #tracking_path = 'C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\tracks.jsonl'
        #read tracking metadata to attach game id info
        #tracking_metadata_path = 'C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\987792_metadata.json'
        #tracking_metadata_path = 'C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\meta.json'

        #track_players_df, player_names_df = tracking_data_manipulation(tracking_path, tracking_metadata_path)

        #match_results_path = 'K:\\TK Work\\2018-19\\DVMS\\Weekly Data\\Round 19 All Data\\srml-8-2018-f987778-matchresults.xml'
        #match_results_path = 'K:\\TK Work\\2018-19\\DVMS\\Weekly Data\\Round 21 All Data\\srml-8-2018-f987792-matchresults.xml'


        referee_id = match_info['referee_id']
        referee_name = match_info['referee_name']
        venue = match_info['venue']
        home_formation = match_info['home_formation']
        away_formation = match_info['away_formation']
        length = match_info['pitchLength']
        width = match_info['pitchWidth']
        self.length = match_info['pitchLength']
        self.width = match_info['pitchWidth']

        player_names_df = player_names_raw.copy()
        player_names_df['player_id'] = [int(x.replace('p', '')) for x in player_names_df['@uID']]
        player_names_df = player_names_df[['player_id', 'player_name']]

        track_players_df['Time_rounded_down'] = np.floor(track_players_df['Time'])
        track_players_df['Time_rounded_up'] = np.ceil(track_players_df['Time'])


        event_data = event_data[event_data.period_id <= 4].reset_index(drop=True)

        #event_data = pd.read_excel('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\event_data.xlsx')
        if event_data.period_id.max() == 2:
            event_data['min_updated'] = np.where(event_data['period_id']==2, event_data['min'] - 45.0, event_data['min'])
            event_data['Time_in_Seconds'] = event_data['min_updated']*60.0 + list(map(float, event_data['sec']))
            event_data['Total_Seconds_in_Period_timestamp'] = None
            event_data['Total_Seconds_in_Period_timestamp'] = np.where(event_data.period_id==1, (pd.to_datetime(event_data.timestamp) - 
                pd.to_datetime(event_data.timestamp[(event_data.type_id==32) & (event_data.period_id==1)].unique()[0])), (pd.to_datetime(event_data.timestamp) - 
                pd.to_datetime(event_data.timestamp[(event_data.type_id==32) & (event_data.period_id==2)].unique()[0])))
        if event_data.period_id.max() == 4:
            event_data['min_updated'] = np.where(event_data['period_id']==2, event_data['min'] - 45.0, 
                np.where(event_data['period_id']==3, event_data['min'] - 90.0, 
                    np.where(event_data['period_id']==4, event_data['min'] - 105.0, event_data['min'])))
            event_data['Time_in_Seconds'] = event_data['min_updated']*60.0 + list(map(float, event_data['sec']))
            event_data['Total_Seconds_in_Period_timestamp'] = None
            event_data['Total_Seconds_in_Period_timestamp'] = np.where(event_data.period_id==1, (pd.to_datetime(event_data.timestamp) - 
                pd.to_datetime(event_data.timestamp[(event_data.type_id==32) & (event_data.period_id==1)].unique()[0])), 
            np.where(event_data.period_id==2, (pd.to_datetime(event_data.timestamp) - 
                pd.to_datetime(event_data.timestamp[(event_data.type_id==32) & (event_data.period_id==2)].unique()[0])), 
            np.where(event_data.period_id==3, (pd.to_datetime(event_data.timestamp) - 
                pd.to_datetime(event_data.timestamp[(event_data.type_id==32) & (event_data.period_id==3)].unique()[0])), 
            (pd.to_datetime(event_data.timestamp) - 
                pd.to_datetime(event_data.timestamp[(event_data.type_id==32) & (event_data.period_id==4)].unique()[0])))))
        event_data['total_seconds_in_period'] = [x.total_seconds() for x in event_data.Total_Seconds_in_Period_timestamp]
        event_data_filtered = event_data[(event_data.period_id==1) | (event_data.period_id==2) | (event_data.period_id==3) | (event_data.period_id==4)][['game_id', 'game_date', 'competition_name', 'match_day', 'season_id', 'unique_event_id', 'away_team_id', 'away_team_name', 'home_team_id', 'home_team_name', 'event_id', 'type_id', 'period_id', 'outcome', 'x', 'y', 'team_id', 'player_id', 'qualifier_id', 'keypass', 'assist', 'value', 'Time_in_Seconds', 'total_seconds_in_period']]

        #add total seconds in period column to the open play crosses file using a merge
        event_data_open_play_crosses_final = all_crosses_file_filtered.merge(event_data[['unique_event_id', 'total_seconds_in_period', 'home_team_id', 'away_team_id', 'outcome', 'keypass', 'assist', 'x', 'y', 'event_id', 'type_id']].reset_index(drop = True), how = 'inner').drop_duplicates(['unique_event_id'])

        # event_data_filtered['x_updated_period_1'] = 0
        # event_data_filtered['x_updated_period_2'] = 0
        # event_data_filtered['x_updated'] = 0
        # event_data_filtered['y_updated_period_1'] = 0
        # event_data_filtered['y_updated_period_2'] = 0
        # event_data_filtered['y_updated'] = 0

        # event_data_filtered['x_updated_period_1'] = np.where(event_data_filtered['team_id']==which_team_from_left_to_right(period=1), event_data_filtered['x'], 100.0 - event_data_filtered['x'])
        # event_data_filtered['x_updated_period_2'] = np.where(event_data_filtered['team_id']==which_team_from_left_to_right(period=2), event_data_filtered['x'], 100.0 - event_data_filtered['x'])
        # event_data_filtered['x_updated'] = np.where(event_data_filtered['period_id']==1, event_data_filtered['x_updated_period_1'], event_data_filtered['x_updated_period_2'])
        # event_data_filtered['y_updated_period_1'] = np.where(event_data_filtered['team_id']==which_team_from_left_to_right(period=1), event_data_filtered['y'], 100.0 - event_data_filtered['y'])
        # event_data_filtered['y_updated_period_2'] = np.where(event_data_filtered['team_id']==which_team_from_left_to_right(period=2), event_data_filtered['y'], 100.0 - event_data_filtered['y'])
        # event_data_filtered['y_updated'] = np.where(event_data_filtered['period_id']==1, event_data_filtered['y_updated_period_1'], event_data_filtered['y_updated_period_2'])

        #sort event data chronologically to make sure they are well sorted
        event_data_filtered = event_data_filtered.sort_values(['game_id', 'period_id', 'Time_in_Seconds', 'total_seconds_in_period'])

        #add the column for left-to-right and 'right-to-left' at period/team level
        #event_data_filtered['Direction'] 

        #final conversion of x,y coordinates. We use the same criterion as the tracking data in here
        #it could well be the case that opta coordinates and trackng coordinates are of inverted signs once normalised depending on the reference side taken into consideration

        #get pitch size info from metadata file
        #metadata_path = 'K:\\TK Work\\2018-19\\DVMS\\Weekly Data\\Round 21 All Data\\987792_metadata.xml'
        #metadata_path = 'K:\\TK Work\\2018-19\\DVMS\\Weekly Data\\Round 19 All Data\\987778_metadata.xml'

        #metadata_path = 'C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\987792_metadata.json'
        #metadata_path = 'C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\meta.json'


        #we need to convert event data coordinates to 'tracking data' coordinates to check for consistency.
        #event_data_filtered['x_new'] = length*event_data_filtered['x_updated']/100.0 - 0.5*length
        #event_data_filtered['y_new'] = width*event_data_filtered['y_updated']/100.0 - 0.5*width

        event_data_filtered = event_data_filtered[['game_id', 'game_date', 'competition_name', 'match_day', 'season_id', 'unique_event_id', 'away_team_id', 'away_team_name', 'home_team_id', 'home_team_name', 'event_id', 'type_id', 'period_id', 'outcome', 'team_id', 'player_id', 'qualifier_id', 'keypass', 'assist', 'value', 'x', 'y', 'Time_in_Seconds', 'total_seconds_in_period']]

        for ev in event_data_open_play_crosses_final.unique_event_id:
            if event_data_open_play_crosses_final.Time_in_Seconds.loc[event_data_open_play_crosses_final.unique_event_id==ev].iloc[0] != event_data_filtered.Time_in_Seconds.loc[event_data_filtered.unique_event_id==ev].iloc[0]:
                event_data_open_play_crosses_final.loc[event_data_open_play_crosses_final.unique_event_id==ev, 'Time_in_Seconds'] = event_data_filtered.Time_in_Seconds.loc[event_data_filtered.unique_event_id==ev].iloc[0] 
        event_data_crosses = event_data_open_play_crosses_final[['game_id', 'unique_event_id', 'away_team_id', 'home_team_id', 'event_id', 'type_id', 'period_id', 'team_id', 'Time_in_Seconds', 'total_seconds_in_period']].reset_index(drop=True)
        event_data_crosses['cross_id'] = range(1, event_data_crosses.shape[0]+1)
        event_data_crosses = event_data_crosses.sort_values(['game_id', 'period_id', 'Time_in_Seconds'])
        event_data_after_crosses = self.get_events_after_crosses(event_data_filtered, event_data_open_play_crosses_final)
        event_data_after_crosses = event_data_after_crosses.sort_values(['game_id', 'period_id', 'Time_in_Seconds', 'total_seconds_in_period'])
        event_data_before_crosses = self.get_events_before_crosses(event_data_filtered, event_data_open_play_crosses_final)
        event_data_before_crosses = event_data_before_crosses.sort_values(['game_id', 'period_id', 'Time_in_Seconds', 'total_seconds_in_period'])
        event_data_crosses_and_before_crosses_and_after_crosses = event_data_crosses.append(event_data_after_crosses).append(event_data_before_crosses).sort_values(['game_id', 'period_id', 'Time_in_Seconds'])

        #inner join with our event data and inspect
        final_event_data = event_data_filtered.merge(event_data_crosses_and_before_crosses_and_after_crosses).sort_values(['cross_id', 'Time_in_Seconds', 'total_seconds_in_period'])
        final_event_data_only_crosses = event_data_filtered.merge(event_data_crosses).sort_values(['cross_id', 'Time_in_Seconds', 'total_seconds_in_period'])
        final_event_data_only_events_after_crosses = event_data_filtered.merge(event_data_after_crosses).sort_values(['cross_id', 'Time_in_Seconds', 'total_seconds_in_period'])
        final_event_data_only_events_before_crosses = event_data_filtered.merge(event_data_before_crosses).sort_values(['cross_id', 'Time_in_Seconds', 'total_seconds_in_period'])
        #export for inspection
        # final_event_data.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\event data crosses and previous and following events.csv', header = True, index = False, sep = ',', decimal = '.')
        # final_event_data_only_events_after_crosses.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\event data events following crosses.csv', header = True, index = False, sep = ',', decimal = '.')
        # final_event_data_only_events_before_crosses.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\event data events preceding crosses.csv', header = True, index = False, sep = ',', decimal = '.')
        # final_event_data_only_crosses.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Bournemouth v Watford 2-1-19\\Second Spectrum\\json\\event data crosses.csv', header = True, index = False, sep = ',', decimal = '.')

        # final_event_data.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\event data crosses and previous and following events.csv', header = True, index = False, sep = ',', decimal = '.')
        # final_event_data_only_events_after_crosses.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\event data events following crosses.csv', header = True, index = False, sep = ',', decimal = '.')
        # final_event_data_only_events_before_crosses.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\event data events preceding crosses.csv', header = True, index = False, sep = ',', decimal = '.')
        # final_event_data_only_crosses.to_csv('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\Tracking provider files\\Man Utd v Huddersfield 26-01-19\\Second Spectrum\\json\\event data crosses.csv', header = True, index = False, sep = ',', decimal = '.')

        player_names_raw['@uID'] = list(map(self.remove_first_letter, player_names_raw['@uID']))
        player_names_raw['@uID'] = player_names_raw['@uID'].astype(int)
        #players_df_lineup['@PlayerRef'] = list(map(self.remove_first_letter, players_df_lineup['@PlayerRef']))
        players_df_lineup['@PlayerRef'] = players_df_lineup['@PlayerRef'].astype(int)
        players_df_lineup['position_in_pitch'] = np.where(players_df_lineup['@SubPosition'].notnull(), players_df_lineup['@SubPosition'], players_df_lineup['@Position'])
        #players_df_lineup = players_df_lineup.drop(['@Captain', '@Formation_Place', '@ShirtNumber', '@Position', '@SubPosition', 'team_id'], axis = 1)
        #player_names_raw = player_names_raw.drop(['@Position', 'PersonName'], axis = 1)


        #import type and qualifiers definitions
        event_type_id = pd.read_excel(os.path.join('scripts_from_fed', 'f24 - ID definitions.xlsx'))   
        qualifier_type_id = pd.read_excel(os.path.join('scripts_from_fed', 'f24 - ID definitions.xlsx'), sheet_name = 'qualifier types')

        if sum(track_players_df.game_id.isnull()) == 0:
            track_players_df.game_id = track_players_df.game_id.astype(int) 
        # merged_data = track_players_df.merge(final_event_data, how = 'left', left_on = ['game_id', 'Period_ID', 'Time_rounded_down'],
        #      right_on = ['game_id', 'period_id', 'Time_in_Seconds']).sort_values(['game_id', 'Period_ID', 'Frame_ID', 'Time', 'event_id']).drop(['period_id', 'Time_in_Seconds'], axis = 1).merge(players_df_lineup, 
        #      how = 'inner', left_on = ['Player_ID'], right_on = ['@PlayerRef']).drop(['@PlayerRef'], axis = 1).merge(player_names_raw, how = 'inner', left_on = ['Player_ID'],
        #      right_on = ['@uID']).drop(['@uID'], axis = 1)

        merged_data = track_players_df
        time_live_first_half = merged_data[(merged_data.Is_Ball_Live=='true') & (merged_data.Period_ID==1)]['Time'].iloc[0]
        time_live_second_half = merged_data[(merged_data.Is_Ball_Live=='true') & (merged_data.Period_ID==2)]['Time'].iloc[0]
        time_live_third_period = None 
        time_live_fourth_period = None
        if track_players_df.Period_ID.max() > 2:
            time_live_third_period = merged_data[(merged_data.Is_Ball_Live=='true') & (merged_data.Period_ID==3)]['Time'].iloc[0]
            time_live_fourth_period = merged_data[(merged_data.Is_Ball_Live=='true') & (merged_data.Period_ID==4)]['Time'].iloc[0]

        #check whether tracking data and OPTA data are recorded from same side of pitch, else switch all coordinates signs
        avg_x_pos_start_home = track_players_df.x_Player.loc[(track_players_df.Is_Home_Away=='home') & (track_players_df.Frame_ID==track_players_df.Frame_ID.loc[(track_players_df.Period_ID==1) & (track_players_df.Is_Ball_Live=='true')].unique()[0])].mean()
        #track_players_df.x_Player.loc[(track_players_df.Is_Home_Away=='home') & (track_players_df.Frame_ID==track_players_df.Frame_ID.loc[(track_players_df.Period_ID==2) & (track_players_df.Is_Ball_Live=='true')].unique()[0])].mean()
        team_left_to_right_first_half = self.which_team_from_left_to_right(event_data_filtered, 1)
        if track_players_df.Period_ID.max() > 2:
            avg_x_pos_start_third_period_home = track_players_df.x_Player.loc[(track_players_df.Is_Home_Away=='home') & (track_players_df.Frame_ID==track_players_df.Frame_ID.loc[(track_players_df.Period_ID==3) & (track_players_df.Is_Ball_Live=='true')].unique()[0])].mean()
            team_left_to_right_third_period = self.which_team_from_left_to_right(event_data_filtered, 3)

        
        if track_players_df.Period_ID.max() == 2:
            if ((avg_x_pos_start_home < 0) & (team_left_to_right_first_half == event_data_filtered.home_team_id.unique()[0])) | ((avg_x_pos_start_home > 0) & (team_left_to_right_first_half != event_data_filtered.home_team_id.unique()[0])):
                merged_data['x_Player_consistent'] = merged_data['x_Player']
                merged_data['y_Player_consistent'] = merged_data['y_Player']
                merged_data['x_Ball_consistent'] = merged_data['x_Ball']
                merged_data['y_Ball_consistent'] = merged_data['y_Ball']
            else:
                merged_data['x_Player_consistent'] = - merged_data['x_Player']
                merged_data['y_Player_consistent'] = - merged_data['y_Player']
                merged_data['x_Ball_consistent'] = - merged_data['x_Ball']
                merged_data['y_Ball_consistent'] = - merged_data['y_Ball']

        if track_players_df.Period_ID.max() > 2:
            merged_data['x_Player_consistent'] = np.nan
            merged_data['y_Player_consistent'] = np.nan
            merged_data['x_Ball_consistent'] = np.nan
            merged_data['y_Ball_consistent'] = np.nan
            
            if ((avg_x_pos_start_home < 0) & (team_left_to_right_first_half == event_data_filtered.home_team_id.unique()[0])) | ((avg_x_pos_start_home > 0) & (team_left_to_right_first_half != event_data_filtered.home_team_id.unique()[0])):
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'x_Player_consistent'] = merged_data[merged_data['Period_ID'].isin([1,2])]['x_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'y_Player_consistent'] = merged_data[merged_data['Period_ID'].isin([1,2])]['y_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'x_Ball_consistent'] = merged_data[merged_data['Period_ID'].isin([1,2])]['x_Ball']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'y_Ball_consistent'] = merged_data[merged_data['Period_ID'].isin([1,2])]['y_Ball']#.tolist()
            else:
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'x_Player_consistent'] = - merged_data[merged_data['Period_ID'].isin([1,2])]['x_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'y_Player_consistent'] = - merged_data[merged_data['Period_ID'].isin([1,2])]['y_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'x_Ball_consistent'] = - merged_data[merged_data['Period_ID'].isin([1,2])]['x_Ball']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([1,2]), 'y_Ball_consistent'] = - merged_data[merged_data['Period_ID'].isin([1,2])]['y_Ball']#.tolist()

            if ((avg_x_pos_start_third_period_home < 0) & (team_left_to_right_third_period == event_data_filtered.home_team_id.unique()[0])) | ((avg_x_pos_start_third_period_home > 0) & (team_left_to_right_third_period != event_data_filtered.home_team_id.unique()[0])):
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'x_Player_consistent'] = merged_data[merged_data['Period_ID'].isin([3,4])]['x_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'y_Player_consistent'] = merged_data[merged_data['Period_ID'].isin([3,4])]['y_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'x_Ball_consistent'] = merged_data[merged_data['Period_ID'].isin([3,4])]['x_Ball']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'y_Ball_consistent'] = merged_data[merged_data['Period_ID'].isin([3,4])]['y_Ball']#.tolist()
            else:
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'x_Player_consistent'] = - merged_data[merged_data['Period_ID'].isin([3,4])]['x_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'y_Player_consistent'] = - merged_data[merged_data['Period_ID'].isin([3,4])]['y_Player']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'x_Ball_consistent'] = - merged_data[merged_data['Period_ID'].isin([3,4])]['x_Ball']#.tolist()
                merged_data.loc[merged_data['Period_ID'].isin([3,4]), 'y_Ball_consistent'] = - merged_data[merged_data['Period_ID'].isin([3,4])]['y_Ball']#.tolist()


        #this bit is used for testing - if we decide to take a 2 secs window wrapped around the opta time for the event, we can use the height of the ball history to get the exact moment in which the cross starts
        #merged_data[['x_new', 'y_new', 'x_Player', 'y_Player', 'z_Player', 'x_Ball', 'y_Ball', 'z_Ball', 'Player_ID', 'player_id', 'Period_ID', 'Time', 'Time_rounded_down', 'Time_rounded_up']][((merged_data.Time_rounded_down.isin([2366.0, 2367.0])) | (merged_data.Time_rounded_up.isin([2368.0]))) & (merged_data.Period_ID==2) & (merged_data.Player_ID==43250)][['x_new', 'y_new', 'x_Player', 'y_Player', 'z_Player', 'x_Ball', 'y_Ball', 'z_Ball']]


        merged_data['distance_from_ball'] = np.sqrt((merged_data.x_Ball - merged_data.x_Player)**2 + (merged_data.y_Ball - merged_data.y_Player)**2)

        #merged_data[(merged_data.Is_Ball_Live=='true') & (merged_data.Period_ID==1)]['Time'].iloc[0]

        #now, use the merged data to set up a loop to create the players in box file at period/event id/time level

        #we ideally want to have an event/player granularity, where player level gives the players in the box, from both teams.
        #This structure can allow to flexibly aggregate up to either team or specific player level.

        #game_ids = list(map(int, event_data_open_play_crosses_final.id.tolist()))

        #[any_goal, goal_qualifiers, scoring_player_name, goal_assisting_player_name, any_shot, shot_qualifiers, shooting_player_name, shot_assisting_player_name, any_error, error_qualifiers, error_player_name, any_foul_in_box, foul_qualifiers, foul_player_name, foul_player_attacking_defending, foul_player_outcome, any_aerial_in_box, aerial_qualifiers, aerial_player_name, aerial_player_attacking_defending, aerial_player_outcome]

        
        event_ids = list(map(int, event_data_open_play_crosses_final.unique_event_id.tolist()))

        data_export = []
        #cross_id = np.where(np.array(event_ids) == min(event_ids))[0][0]

        for cross_id in range(len(event_ids)):

            current_period = int(event_data_open_play_crosses_final.period_id.iloc[cross_id])
            current_player = int(event_data_open_play_crosses_final.player_id.iloc[cross_id])
            if current_player not in merged_data.Player_ID.unique().tolist():
                print ('Skipping Event ID {} as the player is not in the tracking data.'.format(event_ids[cross_id]))
                continue
            current_player_name = player_names_raw.loc[player_names_raw['@uID']==current_player].player_name.iloc[0]
            current_player_status = players_df_lineup['@Status'].loc[players_df_lineup['@PlayerRef']==current_player].iloc[0]
            current_player_pos_in_pitch = players_df_lineup['position_in_pitch'].loc[players_df_lineup['@PlayerRef']==current_player].iloc[0]
            current_team_home_away = merged_data.Is_Home_Away[(merged_data.Player_ID==current_player)].unique()[0]
            current_team_id = int(event_data_open_play_crosses_final.team_id.iloc[cross_id])
            both_team_ids = [event_data_open_play_crosses_final.home_team_id.iloc[cross_id], event_data_open_play_crosses_final.away_team_id.iloc[cross_id]]
            opposing_team_id = [None if x==current_team_id else x for x in both_team_ids]
            opposing_team_id = [x for x in opposing_team_id if x][0]
            current_team = np.where(current_team_id == event_data_filtered[(event_data_filtered.unique_event_id==event_ids[cross_id])].home_team_id.unique()[0], event_data_filtered.home_team_name.unique()[0], event_data_filtered.away_team_name.unique()[0]).tolist()
            opposing_team = np.where(current_team_id == event_data_filtered[(event_data_filtered.unique_event_id==event_ids[cross_id])].home_team_id.unique()[0], event_data_filtered.away_team_name.unique()[0], event_data_filtered.home_team_name.unique()[0]).tolist()
            outcome = event_data_open_play_crosses_final.outcome.iloc[cross_id]
            #chipped = np.where(np.any(final_event_data_only_crosses[(final_event_data_only_crosses.event_id==event_ids[cross_id]) & (final_event_data_only_crosses.period_id==current_period) & (final_event_data_only_crosses.team_id==current_team_id)].qualifier_id==155.0),1,0)
            keypass = event_data_open_play_crosses_final.fillna(0).keypass.iloc[cross_id]
            assist = event_data_open_play_crosses_final.fillna(0).assist.iloc[cross_id]
            x_crosser = event_data_open_play_crosses_final.x.iloc[cross_id]
            y_crosser = event_data_open_play_crosses_final.y.iloc[cross_id]

            x_final_ball = None 
            y_final_ball = None 
            length_cross = None
            if event_type == 'Set Piece':
                if (event_data_open_play_crosses_final['Set Piece Type'].iloc[cross_id] != 'Free Kick Shot') & (event_data_open_play_crosses_final['type_id'].iloc[cross_id] not in [13,14,15,16]):
                    x_final_ball = float(final_event_data_only_crosses[(final_event_data_only_crosses.unique_event_id==event_ids[cross_id]) & (final_event_data_only_crosses.qualifier_id==140)].value.iloc[0])
                    y_final_ball = float(final_event_data_only_crosses[(final_event_data_only_crosses.unique_event_id==event_ids[cross_id]) & (final_event_data_only_crosses.qualifier_id==141)].value.iloc[0])
                    length_cross = np.round(np.sqrt((x_crosser/100.0*length - x_final_ball/100.0*length)**2 + (y_crosser/100.0*width - y_final_ball/100.0*width)**2),2)
            if event_type == 'Cross':
                x_final_ball = float(final_event_data_only_crosses[(final_event_data_only_crosses.unique_event_id==event_ids[cross_id]) & (final_event_data_only_crosses.qualifier_id==140)].value.iloc[0])
                y_final_ball = float(final_event_data_only_crosses[(final_event_data_only_crosses.unique_event_id==event_ids[cross_id]) & (final_event_data_only_crosses.qualifier_id==141)].value.iloc[0])
                length_cross = np.round(np.sqrt((x_crosser/100.0*length - x_final_ball/100.0*length)**2 + (y_crosser/100.0*width - y_final_ball/100.0*width)**2),2)
            #opta_time = float(event_data_open_play_crosses_final.total_seconds_in_period[(event_data_open_play_crosses_final.unique_event_id==event_ids[cross_id])])
            #opta_time_following_event = min(event_data_after_crosses.total_seconds_in_period[(event_data_after_crosses.cross_id==cross_id+1)])
            #opta_time_previous_event = max(event_data_before_crosses.total_seconds_in_period[(event_data_before_crosses.cross_id==cross_id+1)])
            time_offset = float(np.where(current_period==1, time_live_first_half, 
                np.where(current_period==2, time_live_second_half, 
                    np.where(current_period==3, time_live_third_period, time_live_fourth_period))))
            opta_time = float(event_data_open_play_crosses_final.Time_in_Seconds[(event_data_open_play_crosses_final.unique_event_id==event_ids[cross_id])]) + time_offset
            
            try: 
                opta_time_following_event = min(event_data_after_crosses.Time_in_Seconds[(event_data_after_crosses.cross_id==cross_id+1)]) + time_offset
            except ValueError:
                opta_time_following_event = opta_time + 10.0 #+ time_offset
            
            try:
                opta_time_previous_event = max(event_data_before_crosses.Time_in_Seconds[(event_data_before_crosses.cross_id==cross_id+1)]) + time_offset
            except ValueError:
                opta_time_previous_event = opta_time - 10.0 #+ time_offset

            

            #we take here a quite large window - we will restrict it afterwards
            #window_start = opta_time_previous_event - 2.0
            #window_end = opta_time_following_event + 1.0
            window_start = opta_time - 5.0
            window_end = opta_time + 5.0
            merged_data_event = merged_data[(merged_data.Time >= window_start) & (merged_data.Time <= window_end) & (merged_data.Period_ID==current_period)].reset_index(drop = True)


        #this method to transform coordinates can actually work well to take into account both teams - we are transferring everything to the perspective of the attacking team
            if self.which_team_from_left_to_right(event_data_filtered, current_period) == current_team_id:
                merged_data_event.loc[:,'x_Player_percent'] = 100*(merged_data_event.loc[:,'x_Player_consistent'] + 0.5*length)/length
                merged_data_event.loc[:,'y_Player_percent'] = 100*(merged_data_event.loc[:,'y_Player_consistent'] + 0.5*width)/width
                merged_data_event.loc[:,'x_Ball_percent'] = 100*(merged_data_event.loc[:,'x_Ball_consistent'] + 0.5*length)/length
                merged_data_event.loc[:,'y_Ball_percent'] = 100*(merged_data_event.loc[:,'y_Ball_consistent'] + 0.5*width)/width
            else:
                merged_data_event.loc[:,'x_Player_percent'] = 100*(1.0 - (merged_data_event.loc[:,'x_Player_consistent'] + 0.5*length)/length)
                merged_data_event.loc[:,'y_Player_percent'] = 100*(1.0 - (merged_data_event.loc[:,'y_Player_consistent'] + 0.5*width)/width)
                merged_data_event.loc[:,'x_Ball_percent'] = 100*(1.0 - (merged_data_event.loc[:,'x_Ball_consistent'] + 0.5*length)/length)
                merged_data_event.loc[:,'y_Ball_percent'] = 100*(1.0 - (merged_data_event.loc[:,'y_Ball_consistent'] + 0.5*width)/width)

            merged_data_event_subset_players_large_window = merged_data_event[(merged_data_event.Is_Ball_Live=='true')].reset_index(drop = True)

            if merged_data_event_subset_players_large_window.shape[0] == 0:
                print ('Skipping Event ID {} as there are issues with tracking data.'.format(event_ids[cross_id]))
                continue
            #merged_data_event_subset_players_large_window = merged_data_event.copy()
            #merged_data_event_subset_players_large_window = merged_data_event[(~merged_data_event.Player_ID.isin(players_df_lineup.loc[players_df_lineup.position_in_pitch=='Goalkeeper']['@PlayerRef'].tolist()))].reset_index(drop = True)
            #(merged_data_event.Is_Ball_Live=='true') & 
            #now we extract the info we want to get out for the event


            #work out height and speed figures for the ball - due to lack of precise temporal and spatial consistency, we try to approximate the path using closest distance with opta coordinates
            #nearly impossible to automate ball height and speed precisely for crosses as there is lack of consistency in time and space between opta and tracking data. As a result, we comment this part
            #out and focus on creating sequences and getting players in box figures, as well as crosser and ball locations.

            #def distance_coordinates_start(x, y):
            #    return np.sqrt((x/100*length - x_crosser/100*length)**2 + (y/100*width - y_crosser/100*width)**2)

            #def distance_coordinates_end(x, y):
            #    return np.sqrt((x/100*length - x_final_ball/100*length)**2 + (y/100*width - y_final_ball/100*width)**2)


            event_data_after_crosses_event = event_data_after_crosses[event_data_after_crosses.cross_id==cross_id+1]

            dat_for_ball = merged_data_event_subset_players_large_window[['Period_ID', 'Time', 'x_Player_percent', 'y_Player_percent', 'x_Ball_percent', 'y_Ball_percent', 'z_Ball', 'Speed_Ball']][merged_data_event_subset_players_large_window.Player_ID == current_player].drop_duplicates()
            #dat_for_ball['distance_start'] = list(map(distance_coordinates_start, dat_for_ball.x_Player_percent, dat_for_ball.y_Player_percent))
            #dat_for_ball['distance_end'] = list(map(distance_coordinates_end, dat_for_ball.x_Ball_percent, dat_for_ball.y_Ball_percent))
            dat_for_ball['distance_ball_from_centre_y'] = list(map(self.distance_coordinates_centre_y, dat_for_ball.y_Ball_percent))

            # from matplotlib import pyplot as plt 
            # plt.plot(dat_for_ball['x_Ball_percent']*length/100.0, dat_for_ball['y_Ball_percent']*width/100.0)
            # plt.xlim((0, length))
            # plt.ylim((0, width))
            # plt.show()
            
            #logic for start of crossing - take the maximum time having ball within 1 m from the crosser, under some additional time constraints as well
            #estimated_start_time_cross = dat_for_ball.Time.iloc[np.where(dat_for_ball.distance_start==min(dat_for_ball.distance_start))[0][0]]
            #estimated_start_time_cross = max(dat_for_ball.Time.loc[(dat_for_ball.distance_start <= 1) & (dat_for_ball.Time < opta_time_following_event)])
            #estimated_start_time_cross = max(merged_data_event_subset_players_large_window.Time.loc[(merged_data_event_subset_players_large_window.distance_from_ball <= 2) & (merged_data_event_subset_players_large_window.Player_ID==current_player) & (merged_data_event_subset_players_large_window.Time < opta_time_following_event) & (merged_data_event_subset_players_large_window.Is_Home_Away == merged_data_event_subset_players_large_window.Last_Touched)])
            #estimated_end_time_cross = dat_for_ball.Time.loc[(dat_for_ball.distance_ball_from_centre_y==min(dat_for_ball.distance_ball_from_centre_y.loc[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.Time <= opta_time_following_event)])) & (dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.Time <= opta_time_following_event)].iloc[0]
            #estimated_start_time_cross = max(time_cross_df.Time.loc[(time_cross_df.Player_ID==current_player) & (time_cross_df.Time <=  min(time_cross_df.Time.loc[time_cross_df.Player_ID==event_data_filtered.player_id.loc[event_data_filtered.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].iloc[0]]))])
            #if event_data_filtered.player_id.loc[event_data_filtered.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].unique()[0] is not None:
            #estimated_end_time_cross = min(time_cross_df.Time.loc[(time_cross_df.Time > estimated_start_time_cross) & (track_players_df_sub.Player_ID!=current_player)])
            #else:
            #    estimated_end_time_cross = opta_time_following_event + min(track_players_df.Time.loc[(track_players_df.Is_Ball_Live=='true') & (track_players_df.Period_ID==current_period)])

            #before looking at right after cross info, we try to get whether the cross was first touch or not.
                

            #THIS IS THE BLOCK THAT CAN CAUSE VALUEERROR STOP, SO WE JUST MAKE SURE THAT PROBLEMATIC EVENTS ARE SKIPPED FOR THE TIME BEING
            #try:
                #get min distance from ball of the players belongig to the team in possession
            closest_player_to_ball_in_poss_grouped = merged_data_event_subset_players_large_window.groupby(['Period_ID', 'Time'])
            closest_player_to_ball_in_poss = []
            for index, df in closest_player_to_ball_in_poss_grouped:
                #if df.Last_Touched.iloc[0] == current_team_home_away:
                #    closest_player_to_ball_in_poss.append(df[(df.distance_from_ball==min(df.distance_from_ball[df.Is_Home_Away==current_team_home_away])) & (df.Last_Touched==current_team_home_away)].reset_index(drop = True))
                closest_player_to_ball_in_poss.append(df[(df.distance_from_ball==min(df[df.Is_Home_Away==current_team_home_away].distance_from_ball))].reset_index(drop = True))
            # if len(closest_player_to_ball_in_poss) == 0:
            #     for index, df in closest_player_to_ball_in_poss_grouped:
            #         closest_player_to_ball_in_poss.append(df[(df.distance_from_ball==min(df.distance_from_ball[df.Is_Home_Away==current_team_home_away])) & (df.Is_Home_Away==current_team_home_away)].reset_index(drop = True))

            closest_player_to_ball_in_poss = pd.concat(closest_player_to_ball_in_poss)
            #closest_player_to_ball_in_poss = closest_player_to_ball_in_poss[(closest_player_to_ball_in_poss.x_Ball_percent <= 100.0) & (closest_player_to_ball_in_poss.y_Ball_percent <= 100.0) & (closest_player_to_ball_in_poss.y_Ball_percent >= 0)].reset_index(drop = True)
            #(closest_player_to_ball_in_poss.distance_from_ball <=2) & 

            closest_player_to_ball_in_poss_crosser = closest_player_to_ball_in_poss[(closest_player_to_ball_in_poss.Player_ID==current_player) & (closest_player_to_ball_in_poss.x_Ball_percent <= 100.0) & (closest_player_to_ball_in_poss.y_Ball_percent <= 100.0) & (closest_player_to_ball_in_poss.y_Ball_percent >= 0)].reset_index(drop = True)

            #if np.all(closest_player_to_ball_in_poss_crosser.Last_Touched != closest_player_to_ball_in_poss_crosser.Is_Home_Away.iloc[0]):
            #    crosser_in_possession = merged_data_event_subset_players_large_window.Time.loc[(merged_data_event_subset_players_large_window.distance_from_ball <= 2) & (merged_data_event_subset_players_large_window.Player_ID==current_player) & (merged_data_event_subset_players_large_window.Time <= opta_time_following_event)]
            
            #else:
            #crosser_in_possession = closest_player_to_ball_in_poss_crosser.Time.loc[(closest_player_to_ball_in_poss_crosser.Time <= opta_time_following_event)]
            #take the last sequence of possession before the ball is touched. 
            # time_in_possession_before_cross = 0.04
            # for time in range(2, crosser_in_possession.shape[0]):
            #     if np.round(crosser_in_possession.iloc[-time+1] - crosser_in_possession.iloc[-time], 2) == 0.04:
            #         time_in_possession_before_cross += 0.04
            #     else:
            #         break 
            # time_in_possession_before_cross = np.round(time_in_possession_before_cross, 2)
            # first_touch_cross = 0
            # if time_in_possession_before_cross < 1:
            #     first_touch_cross = 1

            #estimated_start_time_cross = crosser_in_possession.max()
            eps = 1
            
            while (eps < 5):
                try:
                    estimated_start_time_cross = closest_player_to_ball_in_poss_crosser[(closest_player_to_ball_in_poss_crosser.distance_from_ball==min(closest_player_to_ball_in_poss_crosser.distance_from_ball[(closest_player_to_ball_in_poss_crosser.Time >= opta_time - eps) & (closest_player_to_ball_in_poss_crosser.Time <= opta_time + eps)]))]['Time'].iloc[0]
                    break
                except ValueError:
                    eps += 1
            if eps >= 5:
                print ('Skipping Event ID {} as time misalignment is too big.'.format(event_ids[cross_id]))
                continue
            #estimated_start_time_cross = crosser_in_possession.iloc[-1] - time_in_possession_before_cross - 0.04
            try:
                if ('Cross Type' in list(all_crosses_file_filtered)) | (event_ids[cross_id] in crosses_file.unique_event_id.tolist()):
                    if event_data_after_crosses_event.shape[0] == 0:
                        estimated_end_time_cross = dat_for_ball[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.distance_ball_from_centre_y == min(dat_for_ball.distance_ball_from_centre_y[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)])) & (dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)]['Time'].iloc[0]        
                    elif final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].type_id.iloc[0] in [1,2,3,4,7,10,11,13,14,15,16,8,12,41,44,45,49,50,51,52,54,56,59,61,67,74]: 
                        if (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 44) | (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 4): #in case we have an aerial duel as straight subsequent event, we need to make sure to include the relevant player, i.e. the player winning the duel
                            if (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 4) & ((10 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].qualifier_id.tolist()) | (10 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1]].qualifier_id.tolist()) | (132 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].qualifier_id.tolist()) | (132 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1]].qualifier_id.tolist())):
                                estimated_end_time_cross = merged_data_event_subset_players_large_window[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==0)].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==0)].player_id.iloc[0])]))]['Time'].iloc[0]
                            elif (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 4) & ((313 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].qualifier_id.tolist()) | (313 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1]].qualifier_id.tolist())):
                                estimated_end_time_cross = dat_for_ball[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.distance_ball_from_centre_y == min(dat_for_ball.distance_ball_from_centre_y[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)])) & (dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)]['Time'].iloc[0]
                            else:
                                estimated_end_time_cross = merged_data_event_subset_players_large_window[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==1)].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==1)].player_id.iloc[0])]))]['Time'].iloc[0]
                        else:
                            estimated_end_time_cross = merged_data_event_subset_players_large_window[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].player_id.iloc[0])]))]['Time'].iloc[0]
                    elif final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].type_id.iloc[0] in [5,6]: 
                        estimated_end_time_cross = dat_for_ball[(dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)]['Time'].max()
                    elif (event_data_open_play_crosses_final[event_data_open_play_crosses_final.unique_event_id==event_ids[cross_id]].type_id.iloc[0]==2):
                        estimated_end_time_cross = merged_data_event_subset_players_large_window[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Player_ID==int(final_event_data[(final_event_data.unique_event_id==event_ids[cross_id]) & (final_event_data.qualifier_id==7)]['value'].iloc[0])) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(((merged_data_event_subset_players_large_window.y_Ball_percent >= 21.1) & (merged_data_event_subset_players_large_window.y_Ball_percent <= 78.9)) | (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) == np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==int(final_event_data[(final_event_data.unique_event_id==event_ids[cross_id]) & (final_event_data.qualifier_id==7)]['value'].iloc[0]))]))]['Time'].iloc[0]
                    else:
                        estimated_end_time_cross = dat_for_ball[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.distance_ball_from_centre_y == min(dat_for_ball.distance_ball_from_centre_y[(dat_for_ball.Time > estimated_start_time_cross) & (dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)])) & (dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)]['Time'].iloc[0]
                if ('Set Piece Type' in list(all_crosses_file_filtered)) & (event_ids[cross_id] not in crosses_file.unique_event_id.tolist()):
                    if event_data_after_crosses_event.shape[0] == 0:
                        estimated_end_time_cross = estimated_start_time_cross + 0.04
                    elif final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].type_id.iloc[0] in [1,2,3,4,7,10,11,13,14,15,16,8,12,41,44,45,49,50,51,52,54,56,59,61,67,74]: 
                        if (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 44) | (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 4): #in case we have an aerial duel as straight subsequent event, we need to make sure to include the relevant player, i.e. the player winning the duel
                            if (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 4) & ((10 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].qualifier_id.tolist()) | (10 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1]].qualifier_id.tolist()) | (132 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].qualifier_id.tolist()) | (132 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1]].qualifier_id.tolist())):
                                estimated_end_time_cross = merged_data_event_subset_players_large_window[(merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==0)].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==0)].player_id.iloc[0])]))]['Time'].iloc[0]
                            elif (final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 4) & ((313 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].qualifier_id.tolist()) | (313 in final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1]].qualifier_id.tolist())):
                                estimated_end_time_cross = estimated_start_time_cross + 0.04
                            else:
                                estimated_end_time_cross = merged_data_event_subset_players_large_window[(merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==1)].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[((final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]) | (final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[1])) & (final_event_data.outcome==1)].player_id.iloc[0])]))]['Time'].iloc[0]
                        else:
                            estimated_end_time_cross = merged_data_event_subset_players_large_window[(merged_data_event_subset_players_large_window.Player_ID==final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].player_id.iloc[0])]))]['Time'].iloc[0]
                    elif final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].type_id.iloc[0] in [5,6]: 
                        estimated_end_time_cross = dat_for_ball[(dat_for_ball.x_Ball_percent <= 100.0) & (dat_for_ball.y_Ball_percent >= 0) & (dat_for_ball.y_Ball_percent <= 100)]['Time'].max()
                    elif (event_data_open_play_crosses_final[event_data_open_play_crosses_final.unique_event_id==event_ids[cross_id]].type_id.iloc[0]==2):
                        estimated_end_time_cross = merged_data_event_subset_players_large_window[(merged_data_event_subset_players_large_window.Player_ID==int(final_event_data[(final_event_data.unique_event_id==event_ids[cross_id]) & (final_event_data.qualifier_id==7)]['value'].iloc[0])) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==int(final_event_data[(final_event_data.unique_event_id==event_ids[cross_id]) & (final_event_data.qualifier_id==7)]['value'].iloc[0]))]))]['Time'].iloc[0]
                    else:
                        estimated_end_time_cross = estimated_start_time_cross + 0.04
            except ValueError:
                print ('Skipping Event ID {} as estimated end time of the cross raises an error.'.format(event_ids[cross_id]))
                continue
            except IndexError:
                print ('Skipping Event ID {} as estimated end time of the cross raises an error.'.format(event_ids[cross_id]))
                continue

            dat_for_ball_restricted = dat_for_ball[(dat_for_ball.Time >= estimated_start_time_cross) & (dat_for_ball.Time <= estimated_end_time_cross)].sort_values(['Time'])
            
            if event_data_after_crosses_event.shape[0] > 0:
                intercepted = 0
                if final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 8:
                    intercepted = 1
                blocked = 0
                if final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 74:
                    blocked = 1
                cleared = 0
                if final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 12:
                    cleared = 1
                ball_out = 0
                if final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].type_id.iloc[0] == 5:
                    ball_out = 1 
            else:
                intercepted = 0
                blocked = 0
                cleared = 0
                ball_out = 0
                #dat_for_ball_restricted = dat_for_ball[(dat_for_ball.Time >= estimated_start_time_cross) & (dat_for_ball.Time <= dat_for_ball[((dat_for_ball.x_Ball_percent > 100.0) | (dat_for_ball.y_Ball_percent > 100.0) | (dat_for_ball.y_Ball_percent < 0)) & (dat_for_ball.Time > estimated_start_time_cross)]['Time'].iloc[0])].sort_values(['Time'])
            #if (((y_final_ball > 78.9) & (y_final_ball < 100.0)) | ((y_final_ball < 21.1) & (y_final_ball > 0))) & (np.sign(y_final_ball - 50.0) != np.sign(y_crosser - 50.0)):
            #    dat_for_ball_restricted = dat_for_ball[(dat_for_ball.Time >= estimated_start_time_cross) & (dat_for_ball.Time <= merged_data_event_subset_players_large_window[(((merged_data_event_subset_players_large_window.y_Ball_percent < 21.1) | (merged_data_event_subset_players_large_window.y_Ball_percent > 78.9)) & (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) != np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].player_id.iloc[0]) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.distance_from_ball==min(merged_data_event_subset_players_large_window.distance_from_ball[(((merged_data_event_subset_players_large_window.y_Ball_percent < 21.1) | (merged_data_event_subset_players_large_window.y_Ball_percent > 78.9)) & (np.sign(merged_data_event_subset_players_large_window.y_Ball_percent - 50.0) != np.sign(y_crosser - 50.0))) & (merged_data_event_subset_players_large_window.Time > estimated_start_time_cross) & (merged_data_event_subset_players_large_window.Player_ID==final_event_data[final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0]].player_id.iloc[0])]))]['Time'].iloc[0])].sort_values(['Time'])
            #estimated_end_time_cross = dat_for_ball.Time.loc[(dat_for_ball.distance_ball_from_centre_y==min(dat_for_ball.distance_ball_from_centre_y.loc[(dat_for_ball.Time > estimated_start_time_cross)]))].max()
            #estimated_end_time_cross = dat_for_ball.Time.loc[(dat_for_ball.distance_ball_from_centre_y==min(dat_for_ball.distance_ball_from_centre_y.loc[(dat_for_ball.Time >= opta_time)]))].max()
            # except ValueError:
            #     estimated_end_time_cross = np.round(crosser_in_possession.max() - 0.6, 2)
            #     estimated_start_time_cross = np.round(estimated_end_time_cross - 0.6, 2)

            #max_height_ball = np.round(max(dat_for_ball_restricted.z_Ball), 2)
            #speed_ball_at_max_height = np.round(np.mean(dat_for_ball_restricted.Speed_Ball[dat_for_ball.z_Ball==max_height_ball]),2)


            # from matplotlib import pyplot as plt 
            # plt.plot(dat_for_ball_restricted['x_Ball_percent']*length/100.0, dat_for_ball_restricted['y_Ball_percent']*width/100.0)
            # plt.xlim((0, length))
            # plt.ylim((0, width))
            # plt.show()

            start_window = estimated_start_time_cross
            end_window = estimated_end_time_cross
                #window_duration = np.round(end_window - start_window + 0.04, 2)

            
            # & ((~merged_data_event_subset_players_large_window.Player_ID.isin(players_df_lineup.loc[players_df_lineup.position_in_pitch=='Goalkeeper']['@PlayerRef'].tolist())) | (merged_data_event_subset_players_large_window.Is_Home_Away == current_team_home_away))

            merged_data_event_subset_players = merged_data_event_subset_players_large_window[(merged_data_event_subset_players_large_window.Time >= start_window) & (merged_data_event_subset_players_large_window.Time <= end_window)].drop_duplicates(['Frame_ID', 'Player_ID']).sort_values(['Time'])
            merged_data_event_subset_players = merged_data_event_subset_players.merge(merged_data_event_subset_players[merged_data_event_subset_players.Player_ID==current_player][['Time', 'x_Player_percent', 'y_Player_percent']], 
                how = 'inner', left_on = ['Time'], right_on = ['Time'], suffixes = ('', '_crosser')).reset_index(drop = True)
            x_crosser_tracking = merged_data_event_subset_players.x_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player)].mean()
            y_crosser_tracking = merged_data_event_subset_players.y_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player)].mean()
            dist_crosser_from_centre_of_goal = np.round(np.sqrt((x_crosser_tracking/100.0*length - length)**2 + (y_crosser_tracking/100.0*width - 0.5*width)**2), 2)
            #dist_crosser_from_centre_of_goal_tracking = np.round(np.sqrt((x_crosser_tracking/100.0*length - length)**2 + (y_crosser_tracking/100.0*width - 0.5*width)**2), 2)
            angle_crosser_from_centre_of_goal = np.round(180/np.pi*np.arccos((length - x_crosser_tracking/100*length)/dist_crosser_from_centre_of_goal), 2)


            
            merged_data_event_subset_players['distance_opponents_from_crosser'] = list(map(self.distance_coordinates_start2, merged_data_event_subset_players.x_Player_percent, merged_data_event_subset_players.y_Player_percent, merged_data_event_subset_players.x_Player_percent_crosser, merged_data_event_subset_players.y_Player_percent_crosser))
            #opponents to crosser info
            x_crosser_tracking = np.round(x_crosser_tracking, 2)
            y_crosser_tracking = np.round(y_crosser_tracking, 2)
            opponents_in_contrast_df = merged_data_event_subset_players[(merged_data_event_subset_players.Is_Home_Away!=current_team_home_away) & (merged_data_event_subset_players.distance_opponents_from_crosser <= 2)]
            number_opponents_in_contrast = opponents_in_contrast_df.Player_ID.unique().shape[0]
            opponents_in_contrast_ids = ', '.join(['p' + str(x) for x in opponents_in_contrast_df.Player_ID.unique().tolist()])
            opponents_in_contrast_names = ', '.join([player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in opponents_in_contrast_df.Player_ID.unique().tolist()])
            #opponents_in_contrast_x_attack_persp = ', '.join([str(np.round(opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].x_Player_percent.mean(), 2)) for x in opponents_in_contrast_df.Player_ID.unique().tolist()]) 
            #opponents_in_contrast_y_attack_persp = ', '.join([str(np.round(opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].y_Player_percent.mean(), 2)) for x in opponents_in_contrast_df.Player_ID.unique().tolist()]) 
            #opponents_in_contrast_x_defend_persp = ', '.join([str(np.round(100.0 - x, 2)) for x in opponents_in_contrast_df.x_Player_percent]) 
            #opponents_in_contrast_y_defend_persp = ', '.join([str(np.round(100.0 - x, 2)) for x in opponents_in_contrast_df.y_Player_percent]) 
            opponents_in_contrast_frames = ', '.join([str(len(opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].Frame_ID)) for x in opponents_in_contrast_df.Player_ID.unique().tolist()])
            opponents_in_contrast_distance = ', '.join([str(np.round(opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].distance_opponents_from_crosser.mean(), 2)) for x in opponents_in_contrast_df.Player_ID.unique().tolist()])
            opponents_in_contrast_distance_from_centre_of_goal = ', '.join([str(np.round((np.sqrt((opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].x_Player_percent/100.0*length - length)**2 + (opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].y_Player_percent/100.0*width - 0.5*width)**2)).mean(), 2)) for x in opponents_in_contrast_df.Player_ID.unique().tolist()])
            angle_opponents_in_contrast_from_centre_of_goal = ', '.join([str(np.round(180/np.pi*np.arccos((opponents_in_contrast_df[opponents_in_contrast_df.Player_ID==x].x_Player_percent.mean()/100.0*length - length)/float(opponents_in_contrast_distance_from_centre_of_goal.split(',')[np.where(opponents_in_contrast_df.Player_ID.unique()==x)[0][0]])), 2)) for x in opponents_in_contrast_df.Player_ID.unique().tolist()])
            

            cross_opposition_df = pd.DataFrame([[number_opponents_in_contrast, opponents_in_contrast_ids, opponents_in_contrast_names, 
                opponents_in_contrast_frames]], columns = ['Number Of Opponents Within 2 Meters From The Crosser',
                'Player IDS Of Opponents Within 2 Meters From The Crosser', 'Player Names Of Opponents Within 2 Meters From The Crosser',
                'Number Of Frames Being Within 2 Meters From The Crosser'])
            
            #this includes only strictly in box players per frame, so we are missing something for players not being in box for the whole duration
            merged_data_event_subset_players_in_box_restricted = merged_data_event_subset_players[(merged_data_event_subset_players.x_Player_percent >= 83.0) & (merged_data_event_subset_players.y_Player_percent >= 21.1) & (merged_data_event_subset_players.y_Player_percent <= 78.9)]
            merged_data_event_subset_players_not_in_box_restricted = merged_data_event_subset_players[(merged_data_event_subset_players.x_Player_percent < 83.0) | (merged_data_event_subset_players.y_Player_percent < 21.1) | (merged_data_event_subset_players.y_Player_percent > 78.9)]
            #which part of the area the attacking players get in from - left, center, right 
            #player_entry = []
            attacking_players = list(map(int, merged_data_event_subset_players_in_box_restricted.Player_ID[(merged_data_event_subset_players_in_box_restricted.Player_ID != current_player) & (merged_data_event_subset_players_in_box_restricted.Is_Home_Away==current_team_home_away)].unique().tolist()))
            defending_players = list(map(int, merged_data_event_subset_players_in_box_restricted.Player_ID[(merged_data_event_subset_players_in_box_restricted.Is_Home_Away != current_team_home_away)].unique().tolist()))

            attacking_players_not_in_box = list(map(int, merged_data_event_subset_players_not_in_box_restricted.Player_ID[(merged_data_event_subset_players_not_in_box_restricted.Player_ID != current_player) & (merged_data_event_subset_players_not_in_box_restricted.Is_Home_Away==current_team_home_away)].unique().tolist()))
            defending_players_not_in_box = list(map(int, merged_data_event_subset_players_not_in_box_restricted.Player_ID[(merged_data_event_subset_players_not_in_box_restricted.Is_Home_Away != current_team_home_away)].unique().tolist()))
            
            #this will include all the considered players in every frame
            merged_data_event_subset_players_in_box = merged_data_event_subset_players[(merged_data_event_subset_players.Player_ID.isin(attacking_players)) | (merged_data_event_subset_players.Player_ID.isin(defending_players))]

            merged_data_event_subset_players_not_in_box = merged_data_event_subset_players[(merged_data_event_subset_players.Player_ID.isin(attacking_players_not_in_box)) | (merged_data_event_subset_players.Player_ID.isin(defending_players_not_in_box))]

            #get total frames in window, as well as in each split
            start_frame = int(merged_data_event_subset_players.Frame_ID.min())
            end_frame = int(merged_data_event_subset_players.Frame_ID.max())
            frames = merged_data_event_subset_players[(merged_data_event_subset_players.Frame_ID > start_frame) & (merged_data_event_subset_players.Frame_ID < end_frame)].Frame_ID.unique().tolist()
            tot_frames = len(frames)
            if tot_frames == 0:
                frames_first_part = [-999]
                frames_second_part = [-999]
                frames_third_part = [-999]
                tot_frames_first_part = 0
                tot_frames_second_part = 0
                tot_frames_third_part = 0
            elif tot_frames < 3:
                frames_first_part = [frames[0]]
                tot_frames_first_part = 1
                if tot_frames == 2:
                    frames_second_part = [frames[1]]
                    tot_frames_second_part = 1
                else:
                    frames_second_part = [-999]
                    tot_frames_second_part = 0
                frames_third_part = [-999]
                tot_frames_third_part = 0
            else:
                if divmod(tot_frames, 3)[-1] == 0:
                    tot_frames_first_part = int(tot_frames/3)
                    tot_frames_second_part = int(tot_frames/3)
                    tot_frames_third_part = int(tot_frames/3)
                elif divmod(tot_frames, 3)[-1] == 1:
                    tot_frames_first_part = int(np.ceil(tot_frames/3))
                    tot_frames_second_part = int(np.floor(tot_frames/3))
                    tot_frames_third_part = int(np.floor(tot_frames/3))
                else:
                    tot_frames_first_part = int(np.ceil(tot_frames/3))
                    tot_frames_second_part = int(np.ceil(tot_frames/3))
                    tot_frames_third_part = int(np.floor(tot_frames/3))    

                frames_first_part = frames[:tot_frames_first_part]
                frames_second_part = frames[tot_frames_first_part:(tot_frames_first_part + tot_frames_second_part)]
                frames_third_part = frames[(tot_frames_first_part + tot_frames_second_part):]        

            x_crosser_tracking_first_part = np.round(merged_data_event_subset_players.x_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_first_part))].mean(), 2)
            y_crosser_tracking_first_part = np.round(merged_data_event_subset_players.y_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_first_part))].mean(), 2)
            x_crosser_tracking_second_part = np.round(merged_data_event_subset_players.x_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2)
            y_crosser_tracking_second_part = np.round(merged_data_event_subset_players.y_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2)
            x_crosser_tracking_third_part = np.round(merged_data_event_subset_players.x_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2)
            y_crosser_tracking_third_part = np.round(merged_data_event_subset_players.y_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2)
            x_crosser_tracking_start = np.round(merged_data_event_subset_players[(merged_data_event_subset_players.Player_ID==current_player)].x_Player_percent.iloc[0], 2)
            y_crosser_tracking_start = np.round(merged_data_event_subset_players[(merged_data_event_subset_players.Player_ID==current_player)].y_Player_percent.iloc[0], 2)
            x_crosser_tracking_end = np.round(merged_data_event_subset_players[(merged_data_event_subset_players.Player_ID==current_player)].x_Player_percent.iloc[-1], 2)
            y_crosser_tracking_end = np.round(merged_data_event_subset_players[(merged_data_event_subset_players.Player_ID==current_player)].y_Player_percent.iloc[-1], 2)

            #let's create an alternative way to obtain a summarised path for each player - downsampling based on bucketing with a largest triangle reasoning
            if tot_frames_first_part + tot_frames_second_part + tot_frames_third_part >= 3:
                x_crosser_tracking_downsampled = [x_crosser_tracking_start]
                y_crosser_tracking_downsampled = [y_crosser_tracking_start]
                frame_crosser_tracking_downsampled = [start_frame]
                temporary_x = [x_crosser_tracking_second_part, x_crosser_tracking_third_part, x_crosser_tracking_end]
                temporary_y = [y_crosser_tracking_second_part, y_crosser_tracking_third_part, y_crosser_tracking_end]
                temporary_frame = [np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2), np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2), end_frame]
                list_frames = [frames_first_part, frames_second_part, frames_third_part]
                frame_crosser_tracking_first_part_downsampled, frame_crosser_tracking_second_part_downsampled, frame_crosser_tracking_third_part_downsampled, x_crosser_tracking_first_part_downsampled, x_crosser_tracking_second_part_downsampled, x_crosser_tracking_third_part_downsampled, y_crosser_tracking_first_part_downsampled, y_crosser_tracking_second_part_downsampled, y_crosser_tracking_third_part_downsampled = self.largest_triangle_sampling(x_crosser_tracking_downsampled, y_crosser_tracking_downsampled, frame_crosser_tracking_downsampled, temporary_x, temporary_y, temporary_frame, list_frames, current_player, merged_data_event_subset_players)

            else:
                x_crosser_tracking_first_part_downsampled = np.nan 
                y_crosser_tracking_first_part_downsampled = np.nan
                x_crosser_tracking_second_part_downsampled = np.nan 
                y_crosser_tracking_second_part_downsampled = np.nan
                x_crosser_tracking_third_part_downsampled = np.nan 
                y_crosser_tracking_third_part_downsampled = np.nan
                frame_crosser_tracking_first_part_downsampled = np.nan 
                frame_crosser_tracking_second_part_downsampled = np.nan
                frame_crosser_tracking_third_part_downsampled = np.nan


            ###avg positions for players being at least for one frame in box
            avg_x_position_attacking = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            avg_x_position_defending = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]
            avg_y_position_attacking = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            avg_y_position_defending = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]

            #three splits averages
            avg_x_position_attacking_first_part = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in attacking_players]
            avg_x_position_defending_first_part = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in defending_players]
            avg_y_position_attacking_first_part = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in attacking_players]
            avg_y_position_defending_first_part = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in defending_players]

            avg_x_position_attacking_second_part = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in attacking_players]
            avg_x_position_defending_second_part = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in defending_players]
            avg_y_position_attacking_second_part = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in attacking_players]
            avg_y_position_defending_second_part = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in defending_players]

            avg_x_position_attacking_third_part = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in attacking_players]
            avg_x_position_defending_third_part = [np.round(np.mean(merged_data_event_subset_players_in_box.x_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in defending_players]
            avg_y_position_attacking_third_part = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in attacking_players]
            avg_y_position_defending_third_part = [np.round(np.mean(merged_data_event_subset_players_in_box.y_Player_percent[(merged_data_event_subset_players_in_box.Player_ID==x) & (merged_data_event_subset_players_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in defending_players]

            #three splits downsampled
            downsampled_x_position_attacking_first_part = []
            downsampled_y_position_attacking_first_part = []
            downsampled_x_position_attacking_second_part = []
            downsampled_y_position_attacking_second_part = []
            downsampled_x_position_attacking_third_part = []
            downsampled_y_position_attacking_third_part = []
            downsampled_frame_attacking_first_part = []
            downsampled_frame_attacking_second_part = []
            downsampled_frame_attacking_third_part = []


            if (len(attacking_players) > 0) & (tot_frames >= 3):
                for att_player_index in range(len(attacking_players)):
                    x_downsampled = [np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==attacking_players[att_player_index]].iloc[0], 2)]
                    y_downsampled = [np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==attacking_players[att_player_index]].iloc[0], 2)]
                    frame_downsampled = [start_frame]
                    temporary_x = [avg_x_position_attacking_second_part[att_player_index], avg_x_position_attacking_third_part[att_player_index], np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==attacking_players[att_player_index]].iloc[-1], 2)]
                    temporary_y = [avg_y_position_attacking_second_part[att_player_index], avg_y_position_attacking_third_part[att_player_index], np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==attacking_players[att_player_index]].iloc[-1], 2)]
                    temporary_frame = [np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==attacking_players[att_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2), np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==attacking_players[att_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2), end_frame]
                    list_frames = [frames_first_part, frames_second_part, frames_third_part]
                    frame_first_part_downsampled, frame_second_part_downsampled, frame_third_part_downsampled, x_first_part_downsampled, x_second_part_downsampled, x_third_part_downsampled, y_first_part_downsampled, y_second_part_downsampled, y_third_part_downsampled = self.largest_triangle_sampling(x_downsampled, y_downsampled, frame_downsampled, temporary_x, temporary_y, temporary_frame, list_frames, attacking_players[att_player_index], merged_data_event_subset_players)
                    downsampled_x_position_attacking_first_part.append(x_first_part_downsampled)
                    downsampled_y_position_attacking_first_part.append(y_first_part_downsampled)
                    downsampled_x_position_attacking_second_part.append(x_second_part_downsampled)
                    downsampled_y_position_attacking_second_part.append(y_second_part_downsampled)
                    downsampled_x_position_attacking_third_part.append(x_third_part_downsampled)
                    downsampled_y_position_attacking_third_part.append(y_third_part_downsampled)
                    downsampled_frame_attacking_first_part.append(frame_first_part_downsampled)
                    downsampled_frame_attacking_second_part.append(frame_second_part_downsampled)
                    downsampled_frame_attacking_third_part.append(frame_third_part_downsampled)
            else:
                if len(attacking_players) > 0:
                    downsampled_x_position_attacking_first_part = [np.nan]*len(attacking_players)
                    downsampled_y_position_attacking_first_part = [np.nan]*len(attacking_players)
                    downsampled_x_position_attacking_second_part = [np.nan]*len(attacking_players)
                    downsampled_y_position_attacking_second_part = [np.nan]*len(attacking_players)
                    downsampled_x_position_attacking_third_part = [np.nan]*len(attacking_players)
                    downsampled_y_position_attacking_third_part = [np.nan]*len(attacking_players)
                    downsampled_frame_attacking_first_part = [np.nan]*len(attacking_players)
                    downsampled_frame_attacking_second_part = [np.nan]*len(attacking_players)
                    downsampled_frame_attacking_third_part = [np.nan]*len(attacking_players)            


            downsampled_x_position_defending_first_part = []
            downsampled_y_position_defending_first_part = []
            downsampled_x_position_defending_second_part = []
            downsampled_y_position_defending_second_part = []
            downsampled_x_position_defending_third_part = []
            downsampled_y_position_defending_third_part = []
            downsampled_frame_defending_first_part = []
            downsampled_frame_defending_second_part = []
            downsampled_frame_defending_third_part = []

            if (len(defending_players) > 0) & (tot_frames >= 3):
                for def_player_index in range(len(defending_players)):
                    x_downsampled = [np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==defending_players[def_player_index]].iloc[0], 2)]
                    y_downsampled = [np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==defending_players[def_player_index]].iloc[0], 2)]
                    frame_downsampled = [start_frame]
                    temporary_x = [avg_x_position_defending_second_part[def_player_index], avg_x_position_defending_third_part[def_player_index], np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==defending_players[def_player_index]].iloc[-1], 2)]
                    temporary_y = [avg_y_position_defending_second_part[def_player_index], avg_y_position_defending_third_part[def_player_index], np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==defending_players[def_player_index]].iloc[-1], 2)]
                    temporary_frame = [np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==defending_players[def_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2), np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==defending_players[def_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2), end_frame]
                    list_frames = [frames_first_part, frames_second_part, frames_third_part]
                    frame_first_part_downsampled, frame_second_part_downsampled, frame_third_part_downsampled, x_first_part_downsampled, x_second_part_downsampled, x_third_part_downsampled, y_first_part_downsampled, y_second_part_downsampled, y_third_part_downsampled = self.largest_triangle_sampling(x_downsampled, y_downsampled, frame_downsampled, temporary_x, temporary_y, temporary_frame, list_frames, defending_players[def_player_index], merged_data_event_subset_players)
                    downsampled_x_position_defending_first_part.append(x_first_part_downsampled)
                    downsampled_y_position_defending_first_part.append(y_first_part_downsampled)
                    downsampled_x_position_defending_second_part.append(x_second_part_downsampled)
                    downsampled_y_position_defending_second_part.append(y_second_part_downsampled)
                    downsampled_x_position_defending_third_part.append(x_third_part_downsampled)
                    downsampled_y_position_defending_third_part.append(y_third_part_downsampled)
                    downsampled_frame_defending_first_part.append(frame_first_part_downsampled)
                    downsampled_frame_defending_second_part.append(frame_second_part_downsampled)
                    downsampled_frame_defending_third_part.append(frame_third_part_downsampled)
            else:
                if len(defending_players) > 0:
                    downsampled_x_position_defending_first_part = [np.nan]*len(defending_players)
                    downsampled_y_position_defending_first_part = [np.nan]*len(defending_players)
                    downsampled_x_position_defending_second_part = [np.nan]*len(defending_players)
                    downsampled_y_position_defending_second_part = [np.nan]*len(defending_players)
                    downsampled_x_position_defending_third_part = [np.nan]*len(defending_players)
                    downsampled_y_position_defending_third_part = [np.nan]*len(defending_players)
                    downsampled_frame_defending_first_part = [np.nan]*len(defending_players)
                    downsampled_frame_defending_second_part = [np.nan]*len(defending_players)
                    downsampled_frame_defending_third_part = [np.nan]*len(defending_players)    


            #avg positions for players being never in box
            avg_x_position_attacking_not_in_box = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x]), 2) for x in attacking_players_not_in_box]
            avg_x_position_defending_not_in_box = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x]), 2) for x in defending_players_not_in_box]
            avg_y_position_attacking_not_in_box = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x]), 2) for x in attacking_players_not_in_box]
            avg_y_position_defending_not_in_box = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x]), 2) for x in defending_players_not_in_box]

            avg_x_position_attacking_not_in_box_first_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in attacking_players_not_in_box]
            avg_x_position_defending_not_in_box_first_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in defending_players_not_in_box]
            avg_y_position_attacking_not_in_box_first_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in attacking_players_not_in_box]
            avg_y_position_defending_not_in_box_first_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_first_part))]), 2) for x in defending_players_not_in_box]

            avg_x_position_attacking_not_in_box_second_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in attacking_players_not_in_box]
            avg_x_position_defending_not_in_box_second_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in defending_players_not_in_box]
            avg_y_position_attacking_not_in_box_second_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in attacking_players_not_in_box]
            avg_y_position_defending_not_in_box_second_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_second_part))]), 2) for x in defending_players_not_in_box]

            avg_x_position_attacking_not_in_box_third_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in attacking_players_not_in_box]
            avg_x_position_defending_not_in_box_third_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.x_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in defending_players_not_in_box]
            avg_y_position_attacking_not_in_box_third_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in attacking_players_not_in_box]
            avg_y_position_defending_not_in_box_third_part = [np.round(np.mean(merged_data_event_subset_players_not_in_box.y_Player_percent[(merged_data_event_subset_players_not_in_box.Player_ID==x) & (merged_data_event_subset_players_not_in_box.Frame_ID.isin(frames_third_part))]), 2) for x in defending_players_not_in_box]

            #three splits downsampled
            downsampled_x_position_attacking_first_part_no_box = []
            downsampled_y_position_attacking_first_part_no_box = []
            downsampled_x_position_attacking_second_part_no_box = []
            downsampled_y_position_attacking_second_part_no_box = []
            downsampled_x_position_attacking_third_part_no_box = []
            downsampled_y_position_attacking_third_part_no_box = []
            downsampled_frame_attacking_first_part_no_box = []
            downsampled_frame_attacking_second_part_no_box = []
            downsampled_frame_attacking_third_part_no_box = []

            if (len(attacking_players_not_in_box) > 0) & (tot_frames >= 3):
                for att_player_index in range(len(attacking_players_not_in_box)):
                    x_downsampled = [np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==attacking_players_not_in_box[att_player_index]].iloc[0], 2)]
                    y_downsampled = [np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==attacking_players_not_in_box[att_player_index]].iloc[0], 2)]
                    frame_downsampled = [start_frame]
                    temporary_x = [avg_x_position_attacking_not_in_box_second_part[att_player_index], avg_x_position_attacking_not_in_box_third_part[att_player_index], np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==attacking_players_not_in_box[att_player_index]].iloc[-1], 2)]
                    temporary_y = [avg_y_position_attacking_not_in_box_second_part[att_player_index], avg_y_position_attacking_not_in_box_third_part[att_player_index], np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==attacking_players_not_in_box[att_player_index]].iloc[-1], 2)]
                    temporary_frame = [np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==attacking_players_not_in_box[att_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2), np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==attacking_players_not_in_box[att_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2), end_frame]
                    list_frames = [frames_first_part, frames_second_part, frames_third_part]
                    frame_first_part_downsampled, frame_second_part_downsampled, frame_third_part_downsampled, x_first_part_downsampled, x_second_part_downsampled, x_third_part_downsampled, y_first_part_downsampled, y_second_part_downsampled, y_third_part_downsampled = self.largest_triangle_sampling(x_downsampled, y_downsampled, frame_downsampled, temporary_x, temporary_y, temporary_frame, list_frames, attacking_players_not_in_box[att_player_index], merged_data_event_subset_players)
                    downsampled_x_position_attacking_first_part_no_box.append(x_first_part_downsampled)
                    downsampled_y_position_attacking_first_part_no_box.append(y_first_part_downsampled)
                    downsampled_x_position_attacking_second_part_no_box.append(x_second_part_downsampled)
                    downsampled_y_position_attacking_second_part_no_box.append(y_second_part_downsampled)
                    downsampled_x_position_attacking_third_part_no_box.append(x_third_part_downsampled)
                    downsampled_y_position_attacking_third_part_no_box.append(y_third_part_downsampled)
                    downsampled_frame_attacking_first_part_no_box.append(frame_first_part_downsampled)
                    downsampled_frame_attacking_second_part_no_box.append(frame_second_part_downsampled)
                    downsampled_frame_attacking_third_part_no_box.append(frame_third_part_downsampled)
            else:
                if len(attacking_players_not_in_box) > 0:
                    downsampled_x_position_attacking_first_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_y_position_attacking_first_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_x_position_attacking_second_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_y_position_attacking_second_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_x_position_attacking_third_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_y_position_attacking_third_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_frame_attacking_first_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_frame_attacking_second_part_no_box = [np.nan]*len(attacking_players_not_in_box)
                    downsampled_frame_attacking_third_part_no_box = [np.nan]*len(attacking_players_not_in_box)    


            downsampled_x_position_defending_first_part_no_box = []
            downsampled_y_position_defending_first_part_no_box = []
            downsampled_x_position_defending_second_part_no_box = []
            downsampled_y_position_defending_second_part_no_box = []
            downsampled_x_position_defending_third_part_no_box = []
            downsampled_y_position_defending_third_part_no_box = []
            downsampled_frame_defending_first_part_no_box = []
            downsampled_frame_defending_second_part_no_box = []
            downsampled_frame_defending_third_part_no_box = []

            if (len(defending_players_not_in_box) > 0) & (tot_frames >= 3):
                for def_player_index in range(len(defending_players_not_in_box)):
                    x_downsampled = [np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==defending_players_not_in_box[def_player_index]].iloc[0], 2)]
                    y_downsampled = [np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==defending_players_not_in_box[def_player_index]].iloc[0], 2)]
                    frame_downsampled = [start_frame]
                    temporary_x = [avg_x_position_defending_not_in_box_second_part[def_player_index], avg_x_position_defending_not_in_box_third_part[def_player_index], np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==defending_players_not_in_box[def_player_index]].iloc[-1], 2)]
                    temporary_y = [avg_y_position_defending_not_in_box_second_part[def_player_index], avg_y_position_defending_not_in_box_third_part[def_player_index], np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==defending_players_not_in_box[def_player_index]].iloc[-1], 2)]
                    temporary_frame = [np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==defending_players_not_in_box[def_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_second_part))].mean(), 2), np.round(merged_data_event_subset_players.Frame_ID.loc[(merged_data_event_subset_players.Player_ID==defending_players_not_in_box[def_player_index]) & (merged_data_event_subset_players.Frame_ID.isin(frames_third_part))].mean(), 2), end_frame]
                    list_frames = [frames_first_part, frames_second_part, frames_third_part]
                    frame_first_part_downsampled, frame_second_part_downsampled, frame_third_part_downsampled, x_first_part_downsampled, x_second_part_downsampled, x_third_part_downsampled, y_first_part_downsampled, y_second_part_downsampled, y_third_part_downsampled = self.largest_triangle_sampling(x_downsampled, y_downsampled, frame_downsampled, temporary_x, temporary_y, temporary_frame, list_frames, defending_players_not_in_box[def_player_index], merged_data_event_subset_players)
                    downsampled_x_position_defending_first_part_no_box.append(x_first_part_downsampled)
                    downsampled_y_position_defending_first_part_no_box.append(y_first_part_downsampled)
                    downsampled_x_position_defending_second_part_no_box.append(x_second_part_downsampled)
                    downsampled_y_position_defending_second_part_no_box.append(y_second_part_downsampled)
                    downsampled_x_position_defending_third_part_no_box.append(x_third_part_downsampled)
                    downsampled_y_position_defending_third_part_no_box.append(y_third_part_downsampled)
                    downsampled_frame_defending_first_part_no_box.append(frame_first_part_downsampled)
                    downsampled_frame_defending_second_part_no_box.append(frame_second_part_downsampled)
                    downsampled_frame_defending_third_part_no_box.append(frame_third_part_downsampled)
            else:
                if len(defending_players_not_in_box) > 0:
                    downsampled_x_position_defending_first_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_y_position_defending_first_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_x_position_defending_second_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_y_position_defending_second_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_x_position_defending_third_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_y_position_defending_third_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_frame_defending_first_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_frame_defending_second_part_no_box = [np.nan]*len(defending_players_not_in_box)
                    downsampled_frame_defending_third_part_no_box = [np.nan]*len(defending_players_not_in_box)    


            #update players set in the box based on their average position - if on average they are outside of the box then we exclude them.
            #potential v2 - we could consider to slightly update the rule such that if the player is on average outside the box but is inside the box at the final frame(s), then we still consider him as being in box
            attacking_player_in_box = [int((avg_x_position_attacking[i] >= 83.0) & (avg_y_position_attacking[i] >= 21.1) & (avg_y_position_attacking[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box = [int((avg_x_position_defending[i] >= 83.0) & (avg_y_position_defending[i] >= 21.1) & (avg_y_position_defending[i] <= 78.9)) for i in range(len(defending_players))]
            attacking_player_in_box_first_part = [int((avg_x_position_attacking_first_part[i] >= 83.0) & (avg_y_position_attacking_first_part[i] >= 21.1) & (avg_y_position_attacking_first_part[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box_first_part = [int((avg_x_position_defending_first_part[i] >= 83.0) & (avg_y_position_defending_first_part[i] >= 21.1) & (avg_y_position_defending_first_part[i] <= 78.9)) for i in range(len(defending_players))]
            attacking_player_in_box_second_part = [int((avg_x_position_attacking_second_part[i] >= 83.0) & (avg_y_position_attacking_second_part[i] >= 21.1) & (avg_y_position_attacking_second_part[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box_second_part = [int((avg_x_position_defending_second_part[i] >= 83.0) & (avg_y_position_defending_second_part[i] >= 21.1) & (avg_y_position_defending_second_part[i] <= 78.9)) for i in range(len(defending_players))]
            attacking_player_in_box_third_part = [int((avg_x_position_attacking_third_part[i] >= 83.0) & (avg_y_position_attacking_third_part[i] >= 21.1) & (avg_y_position_attacking_third_part[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box_third_part = [int((avg_x_position_defending_third_part[i] >= 83.0) & (avg_y_position_defending_third_part[i] >= 21.1) & (avg_y_position_defending_third_part[i] <= 78.9)) for i in range(len(defending_players))]
            attacking_player_in_box_first_part_downsampled = [int((downsampled_x_position_attacking_first_part[i] >= 83.0) & (downsampled_y_position_attacking_first_part[i] >= 21.1) & (downsampled_y_position_attacking_first_part[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box_first_part_downsampled = [int((downsampled_x_position_defending_first_part[i] >= 83.0) & (downsampled_y_position_defending_first_part[i] >= 21.1) & (downsampled_y_position_defending_first_part[i] <= 78.9)) for i in range(len(defending_players))]
            attacking_player_in_box_second_part_downsampled = [int((downsampled_x_position_attacking_second_part[i] >= 83.0) & (downsampled_y_position_attacking_second_part[i] >= 21.1) & (downsampled_y_position_attacking_second_part[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box_second_part_downsampled = [int((downsampled_x_position_defending_second_part[i] >= 83.0) & (downsampled_y_position_defending_second_part[i] >= 21.1) & (downsampled_y_position_defending_second_part[i] <= 78.9)) for i in range(len(defending_players))]
            attacking_player_in_box_third_part_downsampled = [int((downsampled_x_position_attacking_third_part[i] >= 83.0) & (downsampled_y_position_attacking_third_part[i] >= 21.1) & (downsampled_y_position_attacking_third_part[i] <= 78.9)) for i in range(len(attacking_players))]
            defending_player_in_box_third_part_downsampled = [int((downsampled_x_position_defending_third_part[i] >= 83.0) & (downsampled_y_position_defending_third_part[i] >= 21.1) & (downsampled_y_position_defending_third_part[i] <= 78.9)) for i in range(len(defending_players))]

            attacking_players_avg = list(np.array(attacking_players)[np.array(attacking_player_in_box)==1])
            defending_players_avg = list(np.array(defending_players)[np.array(defending_player_in_box)==1])
            attacking_players_first_part = list(np.array(attacking_players)[np.array(attacking_player_in_box_first_part)==1])
            defending_players_first_part = list(np.array(defending_players)[np.array(defending_player_in_box_first_part)==1])
            attacking_players_second_part = list(np.array(attacking_players)[np.array(attacking_player_in_box_second_part)==1])
            defending_players_second_part = list(np.array(defending_players)[np.array(defending_player_in_box_second_part)==1])
            attacking_players_third_part = list(np.array(attacking_players)[np.array(attacking_player_in_box_third_part)==1])
            defending_players_third_part = list(np.array(defending_players)[np.array(defending_player_in_box_third_part)==1])
            attacking_players_first_part_downsampled = list(np.array(attacking_players)[np.array(attacking_player_in_box_first_part_downsampled)==1])
            defending_players_first_part_downsampled = list(np.array(defending_players)[np.array(defending_player_in_box_first_part_downsampled)==1])
            attacking_players_second_part_downsampled = list(np.array(attacking_players)[np.array(attacking_player_in_box_second_part_downsampled)==1])
            defending_players_second_part_downsampled = list(np.array(defending_players)[np.array(defending_player_in_box_second_part_downsampled)==1])
            attacking_players_third_part_downsampled = list(np.array(attacking_players)[np.array(attacking_player_in_box_third_part_downsampled)==1])
            defending_players_third_part_downsampled = list(np.array(defending_players)[np.array(defending_player_in_box_third_part_downsampled)==1])


            attacking_players_names = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_avg]
            defending_players_names = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_avg]
            attacking_players_names_first_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_first_part]
            defending_players_names_first_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_first_part]
            attacking_players_names_second_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_second_part]
            defending_players_names_second_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_second_part]
            attacking_players_names_third_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_third_part]
            defending_players_names_third_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_third_part]
            attacking_players_names_first_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_first_part_downsampled]
            defending_players_names_first_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_first_part_downsampled]
            attacking_players_names_second_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_second_part_downsampled]
            defending_players_names_second_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_second_part_downsampled]
            attacking_players_names_third_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_third_part_downsampled]
            defending_players_names_third_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_third_part_downsampled]



            attacking_player_not_in_box = [int((avg_x_position_attacking_not_in_box[i] < 83.0) | (avg_y_position_attacking_not_in_box[i] < 21.1) | (avg_y_position_attacking_not_in_box[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box = [int((avg_x_position_defending_not_in_box[i] < 83.0) | (avg_y_position_defending_not_in_box[i] < 21.1) | (avg_y_position_defending_not_in_box[i] > 78.9)) for i in range(len(defending_players_not_in_box))]
            attacking_player_not_in_box_first_part = [int((avg_x_position_attacking_not_in_box_first_part[i] < 83.0) | (avg_y_position_attacking_not_in_box_first_part[i] < 21.1) | (avg_y_position_attacking_not_in_box_first_part[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box_first_part = [int((avg_x_position_defending_not_in_box_first_part[i] < 83.0) | (avg_y_position_defending_not_in_box_first_part[i] < 21.1) | (avg_y_position_defending_not_in_box_first_part[i] > 78.9)) for i in range(len(defending_players_not_in_box))]
            attacking_player_not_in_box_second_part = [int((avg_x_position_attacking_not_in_box_second_part[i] < 83.0) | (avg_y_position_attacking_not_in_box_second_part[i] < 21.1) | (avg_y_position_attacking_not_in_box_second_part[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box_second_part = [int((avg_x_position_defending_not_in_box_second_part[i] < 83.0) | (avg_y_position_defending_not_in_box_second_part[i] < 21.1) | (avg_y_position_defending_not_in_box_second_part[i] > 78.9)) for i in range(len(defending_players_not_in_box))]
            attacking_player_not_in_box_third_part = [int((avg_x_position_attacking_not_in_box_third_part[i] < 83.0) | (avg_y_position_attacking_not_in_box_third_part[i] < 21.1) | (avg_y_position_attacking_not_in_box_third_part[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box_third_part = [int((avg_x_position_defending_not_in_box_third_part[i] < 83.0) | (avg_y_position_defending_not_in_box_third_part[i] < 21.1) | (avg_y_position_defending_not_in_box_third_part[i] > 78.9)) for i in range(len(defending_players_not_in_box))]
            attacking_player_not_in_box_first_part_downsampled = [int((downsampled_x_position_attacking_first_part_no_box[i] < 83.0) | (downsampled_y_position_attacking_first_part_no_box[i] < 21.1) | (downsampled_y_position_attacking_first_part_no_box[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box_first_part_downsampled = [int((downsampled_x_position_defending_first_part_no_box[i] < 83.0) | (downsampled_y_position_defending_first_part_no_box[i] < 21.1) | (downsampled_y_position_defending_first_part_no_box[i] > 78.9)) for i in range(len(defending_players_not_in_box))]
            attacking_player_not_in_box_second_part_downsampled = [int((downsampled_x_position_attacking_second_part_no_box[i] < 83.0) | (downsampled_y_position_attacking_second_part_no_box[i] < 21.1) | (downsampled_y_position_attacking_second_part_no_box[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box_second_part_downsampled = [int((downsampled_x_position_defending_second_part_no_box[i] < 83.0) | (downsampled_y_position_defending_second_part_no_box[i] < 21.1) | (downsampled_y_position_defending_second_part_no_box[i] > 78.9)) for i in range(len(defending_players_not_in_box))]
            attacking_player_not_in_box_third_part_downsampled = [int((downsampled_x_position_attacking_third_part_no_box[i] < 83.0) | (downsampled_y_position_attacking_third_part_no_box[i] < 21.1) | (downsampled_y_position_attacking_third_part_no_box[i] > 78.9)) for i in range(len(attacking_players_not_in_box))]
            defending_player_not_in_box_third_part_downsampled = [int((downsampled_x_position_defending_third_part_no_box[i] < 83.0) | (downsampled_y_position_defending_third_part_no_box[i] < 21.1) | (downsampled_y_position_defending_third_part_no_box[i] > 78.9)) for i in range(len(defending_players_not_in_box))]


            attacking_players_not_in_box_avg = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box)==1])
            defending_players_not_in_box_avg = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box)==1])
            attacking_players_not_in_box_first_part = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box_first_part)==1])
            defending_players_not_in_box_first_part = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box_first_part)==1])
            attacking_players_not_in_box_second_part = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box_second_part)==1])
            defending_players_not_in_box_second_part = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box_second_part)==1])
            attacking_players_not_in_box_third_part = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box_third_part)==1])
            defending_players_not_in_box_third_part = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box_third_part)==1])
            attacking_players_not_in_box_first_part_downsampled = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box_first_part_downsampled)==1])
            defending_players_not_in_box_first_part_downsampled = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box_first_part_downsampled)==1])
            attacking_players_not_in_box_second_part_downsampled = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box_second_part_downsampled)==1])
            defending_players_not_in_box_second_part_downsampled = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box_second_part_downsampled)==1])
            attacking_players_not_in_box_third_part_downsampled = list(np.array(attacking_players_not_in_box)[np.array(attacking_player_not_in_box_third_part_downsampled)==1])
            defending_players_not_in_box_third_part_downsampled = list(np.array(defending_players_not_in_box)[np.array(defending_player_not_in_box_third_part_downsampled)==1])


            attacking_players_names_not_in_box = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_avg]
            defending_players_names_not_in_box = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_avg]
            attacking_players_names_not_in_box_first_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_first_part]
            defending_players_names_not_in_box_first_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_first_part]
            attacking_players_names_not_in_box_second_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_second_part]
            defending_players_names_not_in_box_second_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_second_part]
            attacking_players_names_not_in_box_third_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_third_part]
            defending_players_names_not_in_box_third_part = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_third_part]
            attacking_players_names_not_in_box_first_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_first_part_downsampled]
            defending_players_names_not_in_box_first_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_first_part_downsampled]
            attacking_players_names_not_in_box_second_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_second_part_downsampled]
            defending_players_names_not_in_box_second_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_second_part_downsampled]
            attacking_players_names_not_in_box_third_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in attacking_players_not_in_box_third_part_downsampled]
            defending_players_names_not_in_box_third_part_downsampled = [player_names_df.player_name[player_names_df.player_id==x].iloc[0] for x in defending_players_not_in_box_third_part_downsampled]

            

            avg_dist_from_centre_of_goal_attacking = [np.round(np.mean(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in attacking_players]
            max_dist_from_centre_of_goal_attacking = [np.round(max(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in attacking_players]
            min_dist_from_centre_of_goal_attacking = [np.round(min(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in attacking_players]
            avg_dist_from_centre_of_goal_defending = [np.round(np.mean(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in defending_players]
            max_dist_from_centre_of_goal_defending = [np.round(max(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in defending_players]
            min_dist_from_centre_of_goal_defending = [np.round(min(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in defending_players]
            #avg_dist_from_ball_attacking = [np.round(np.mean(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            #max_dist_from_ball_attacking = [np.round(max(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            #min_dist_from_ball_attacking = [np.round(min(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            #avg_dist_from_ball_defending = [np.round(np.mean(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]
            #max_dist_from_ball_defending = [np.round(max(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]
            #min_dist_from_ball_defending = [np.round(min(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]

            avg_x_position_attacking = list(np.array(avg_x_position_attacking)[np.array(attacking_player_in_box)==1])
            avg_x_position_defending = list(np.array(avg_x_position_defending)[np.array(defending_player_in_box)==1])
            avg_y_position_attacking = list(np.array(avg_y_position_attacking)[np.array(attacking_player_in_box)==1])
            avg_y_position_defending = list(np.array(avg_y_position_defending)[np.array(defending_player_in_box)==1])
            avg_x_position_attacking_first_part = list(np.array(avg_x_position_attacking_first_part)[np.array(attacking_player_in_box_first_part)==1])
            avg_x_position_defending_first_part = list(np.array(avg_x_position_defending_first_part)[np.array(defending_player_in_box_first_part)==1])
            avg_y_position_attacking_first_part = list(np.array(avg_y_position_attacking_first_part)[np.array(attacking_player_in_box_first_part)==1])
            avg_y_position_defending_first_part = list(np.array(avg_y_position_defending_first_part)[np.array(defending_player_in_box_first_part)==1])
            avg_x_position_attacking_second_part = list(np.array(avg_x_position_attacking_second_part)[np.array(attacking_player_in_box_second_part)==1])
            avg_x_position_defending_second_part = list(np.array(avg_x_position_defending_second_part)[np.array(defending_player_in_box_second_part)==1])
            avg_y_position_attacking_second_part = list(np.array(avg_y_position_attacking_second_part)[np.array(attacking_player_in_box_second_part)==1])
            avg_y_position_defending_second_part = list(np.array(avg_y_position_defending_second_part)[np.array(defending_player_in_box_second_part)==1])
            avg_x_position_attacking_third_part = list(np.array(avg_x_position_attacking_third_part)[np.array(attacking_player_in_box_third_part)==1])
            avg_x_position_defending_third_part = list(np.array(avg_x_position_defending_third_part)[np.array(defending_player_in_box_third_part)==1])
            avg_y_position_attacking_third_part = list(np.array(avg_y_position_attacking_third_part)[np.array(attacking_player_in_box_third_part)==1])
            avg_y_position_defending_third_part = list(np.array(avg_y_position_defending_third_part)[np.array(defending_player_in_box_third_part)==1])
            avg_x_position_attacking_first_part_downsampled = list(np.array(downsampled_x_position_attacking_first_part)[np.array(attacking_player_in_box_first_part_downsampled)==1])
            avg_x_position_defending_first_part_downsampled = list(np.array(downsampled_x_position_defending_first_part)[np.array(defending_player_in_box_first_part_downsampled)==1])
            avg_y_position_attacking_first_part_downsampled = list(np.array(downsampled_y_position_attacking_first_part)[np.array(attacking_player_in_box_first_part_downsampled)==1])
            avg_y_position_defending_first_part_downsampled = list(np.array(downsampled_y_position_defending_first_part)[np.array(defending_player_in_box_first_part_downsampled)==1])
            avg_x_position_attacking_second_part_downsampled = list(np.array(downsampled_x_position_attacking_second_part)[np.array(attacking_player_in_box_second_part_downsampled)==1])
            avg_x_position_defending_second_part_downsampled = list(np.array(downsampled_x_position_defending_second_part)[np.array(defending_player_in_box_second_part_downsampled)==1])
            avg_y_position_attacking_second_part_downsampled = list(np.array(downsampled_y_position_attacking_second_part)[np.array(attacking_player_in_box_second_part_downsampled)==1])
            avg_y_position_defending_second_part_downsampled = list(np.array(downsampled_y_position_defending_second_part)[np.array(defending_player_in_box_second_part_downsampled)==1])
            avg_x_position_attacking_third_part_downsampled = list(np.array(downsampled_x_position_attacking_third_part)[np.array(attacking_player_in_box_third_part_downsampled)==1])
            avg_x_position_defending_third_part_downsampled = list(np.array(downsampled_x_position_defending_third_part)[np.array(defending_player_in_box_third_part_downsampled)==1])
            avg_y_position_attacking_third_part_downsampled = list(np.array(downsampled_y_position_attacking_third_part)[np.array(attacking_player_in_box_third_part_downsampled)==1])
            avg_y_position_defending_third_part_downsampled = list(np.array(downsampled_y_position_defending_third_part)[np.array(defending_player_in_box_third_part_downsampled)==1])
            frame_attacking_first_part_downsampled = list(np.array(downsampled_frame_attacking_first_part)[np.array(attacking_player_in_box_first_part_downsampled)==1])
            frame_attacking_second_part_downsampled = list(np.array(downsampled_frame_attacking_second_part)[np.array(attacking_player_in_box_second_part_downsampled)==1])
            frame_attacking_third_part_downsampled = list(np.array(downsampled_frame_attacking_third_part)[np.array(attacking_player_in_box_third_part_downsampled)==1])
            frame_defending_first_part_downsampled = list(np.array(downsampled_frame_defending_first_part)[np.array(defending_player_in_box_first_part_downsampled)==1])
            frame_defending_second_part_downsampled = list(np.array(downsampled_frame_defending_second_part)[np.array(defending_player_in_box_second_part_downsampled)==1])
            frame_defending_third_part_downsampled = list(np.array(downsampled_frame_defending_third_part)[np.array(defending_player_in_box_third_part_downsampled)==1])

            start_x_position_attacking = [np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[0], 2) for x in attacking_players_avg]
            end_x_position_attacking = [np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[-1], 2) for x in attacking_players_avg]

            start_x_position_defending = [np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[0], 2) for x in defending_players_avg]
            end_x_position_defending = [np.round(merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[-1], 2) for x in defending_players_avg]
            
            start_y_position_attacking = [np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[0], 2) for x in attacking_players_avg]
            end_y_position_attacking = [np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[-1], 2) for x in attacking_players_avg]
            
            start_y_position_defending = [np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[0], 2) for x in defending_players_avg]
            end_y_position_defending = [np.round(merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x].iloc[-1], 2) for x in defending_players_avg]



            avg_dist_from_centre_of_goal_attacking_not_in_box = [np.round(np.mean(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in attacking_players]
            max_dist_from_centre_of_goal_attacking_not_in_box = [np.round(max(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in attacking_players]
            min_dist_from_centre_of_goal_attacking_not_in_box = [np.round(min(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in attacking_players]
            avg_dist_from_centre_of_goal_defending_not_in_box = [np.round(np.mean(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in defending_players]
            max_dist_from_centre_of_goal_defending_not_in_box = [np.round(max(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in defending_players]
            min_dist_from_centre_of_goal_defending_not_in_box = [np.round(min(np.sqrt((merged_data_event_subset_players_in_box.x_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*length - length)**2 + (merged_data_event_subset_players_in_box.y_Player_percent[merged_data_event_subset_players_in_box.Player_ID==x]/100.0*width - 0.5*width)**2)), 2) for x in defending_players]
            #avg_dist_from_ball_attacking = [np.round(np.mean(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            #max_dist_from_ball_attacking = [np.round(max(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            #min_dist_from_ball_attacking = [np.round(min(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in attacking_players]
            #avg_dist_from_ball_defending = [np.round(np.mean(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]
            #max_dist_from_ball_defending = [np.round(max(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]
            #min_dist_from_ball_defending = [np.round(min(merged_data_event_subset_players_in_box.distance_from_ball[merged_data_event_subset_players_in_box.Player_ID==x]), 2) for x in defending_players]

            avg_x_position_attacking_not_in_box = list(np.array(avg_x_position_attacking_not_in_box)[np.array(attacking_player_not_in_box)==1])
            avg_x_position_defending_not_in_box = list(np.array(avg_x_position_defending_not_in_box)[np.array(defending_player_not_in_box)==1])
            avg_y_position_attacking_not_in_box = list(np.array(avg_y_position_attacking_not_in_box)[np.array(attacking_player_not_in_box)==1])
            avg_y_position_defending_not_in_box = list(np.array(avg_y_position_defending_not_in_box)[np.array(defending_player_not_in_box)==1])
            avg_x_position_attacking_not_in_box_first_part = list(np.array(avg_x_position_attacking_not_in_box_first_part)[np.array(attacking_player_not_in_box_first_part)==1])
            avg_x_position_defending_not_in_box_first_part = list(np.array(avg_x_position_defending_not_in_box_first_part)[np.array(defending_player_not_in_box_first_part)==1])
            avg_y_position_attacking_not_in_box_first_part = list(np.array(avg_y_position_attacking_not_in_box_first_part)[np.array(attacking_player_not_in_box_first_part)==1])
            avg_y_position_defending_not_in_box_first_part = list(np.array(avg_y_position_defending_not_in_box_first_part)[np.array(defending_player_not_in_box_first_part)==1])
            avg_x_position_attacking_not_in_box_second_part = list(np.array(avg_x_position_attacking_not_in_box_second_part)[np.array(attacking_player_not_in_box_second_part)==1])
            avg_x_position_defending_not_in_box_second_part = list(np.array(avg_x_position_defending_not_in_box_second_part)[np.array(defending_player_not_in_box_second_part)==1])
            avg_y_position_attacking_not_in_box_second_part = list(np.array(avg_y_position_attacking_not_in_box_second_part)[np.array(attacking_player_not_in_box_second_part)==1])
            avg_y_position_defending_not_in_box_second_part = list(np.array(avg_y_position_defending_not_in_box_second_part)[np.array(defending_player_not_in_box_second_part)==1])
            avg_x_position_attacking_not_in_box_third_part = list(np.array(avg_x_position_attacking_not_in_box_third_part)[np.array(attacking_player_not_in_box_third_part)==1])
            avg_x_position_defending_not_in_box_third_part = list(np.array(avg_x_position_defending_not_in_box_third_part)[np.array(defending_player_not_in_box_third_part)==1])
            avg_y_position_attacking_not_in_box_third_part = list(np.array(avg_y_position_attacking_not_in_box_third_part)[np.array(attacking_player_not_in_box_third_part)==1])
            avg_y_position_defending_not_in_box_third_part = list(np.array(avg_y_position_defending_not_in_box_third_part)[np.array(defending_player_not_in_box_third_part)==1])
            avg_x_position_attacking_not_in_box_first_part_downsampled = list(np.array(downsampled_x_position_attacking_first_part_no_box)[np.array(attacking_player_not_in_box_first_part_downsampled)==1])
            avg_x_position_defending_not_in_box_first_part_downsampled = list(np.array(downsampled_x_position_defending_first_part_no_box)[np.array(defending_player_not_in_box_first_part_downsampled)==1])
            avg_y_position_attacking_not_in_box_first_part_downsampled = list(np.array(downsampled_y_position_attacking_first_part_no_box)[np.array(attacking_player_not_in_box_first_part_downsampled)==1])
            avg_y_position_defending_not_in_box_first_part_downsampled = list(np.array(downsampled_y_position_defending_first_part_no_box)[np.array(defending_player_not_in_box_first_part_downsampled)==1])
            avg_x_position_attacking_not_in_box_second_part_downsampled = list(np.array(downsampled_x_position_attacking_second_part_no_box)[np.array(attacking_player_not_in_box_second_part_downsampled)==1])
            avg_x_position_defending_not_in_box_second_part_downsampled = list(np.array(downsampled_x_position_defending_second_part_no_box)[np.array(defending_player_not_in_box_second_part_downsampled)==1])
            avg_y_position_attacking_not_in_box_second_part_downsampled = list(np.array(downsampled_y_position_attacking_second_part_no_box)[np.array(attacking_player_not_in_box_second_part_downsampled)==1])
            avg_y_position_defending_not_in_box_second_part_downsampled = list(np.array(downsampled_y_position_defending_second_part_no_box)[np.array(defending_player_not_in_box_second_part_downsampled)==1])
            avg_x_position_attacking_not_in_box_third_part_downsampled = list(np.array(downsampled_x_position_attacking_third_part_no_box)[np.array(attacking_player_not_in_box_third_part_downsampled)==1])
            avg_x_position_defending_not_in_box_third_part_downsampled = list(np.array(downsampled_x_position_defending_third_part_no_box)[np.array(defending_player_not_in_box_third_part_downsampled)==1])
            avg_y_position_attacking_not_in_box_third_part_downsampled = list(np.array(downsampled_y_position_attacking_third_part_no_box)[np.array(attacking_player_not_in_box_third_part_downsampled)==1])
            avg_y_position_defending_not_in_box_third_part_downsampled = list(np.array(downsampled_y_position_defending_third_part_no_box)[np.array(defending_player_not_in_box_third_part_downsampled)==1])
            frame_attacking_first_part_downsampled_no_box = list(np.array(downsampled_frame_attacking_first_part_no_box)[np.array(attacking_player_not_in_box_first_part_downsampled)==1])
            frame_attacking_second_part_downsampled_no_box = list(np.array(downsampled_frame_attacking_second_part_no_box)[np.array(attacking_player_not_in_box_second_part_downsampled)==1])
            frame_attacking_third_part_downsampled_no_box = list(np.array(downsampled_frame_attacking_third_part_no_box)[np.array(attacking_player_not_in_box_third_part_downsampled)==1])
            frame_defending_first_part_downsampled_no_box = list(np.array(downsampled_frame_defending_first_part_no_box)[np.array(defending_player_not_in_box_first_part_downsampled)==1])
            frame_defending_second_part_downsampled_no_box = list(np.array(downsampled_frame_defending_second_part_no_box)[np.array(defending_player_not_in_box_second_part_downsampled)==1])
            frame_defending_third_part_downsampled_no_box = list(np.array(downsampled_frame_defending_third_part_no_box)[np.array(defending_player_not_in_box_third_part_downsampled)==1])

            
            start_x_position_attacking_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[0], 2) for x in attacking_players_not_in_box_avg]
            end_x_position_attacking_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[-1], 2) for x in attacking_players_not_in_box_avg]
            
            start_x_position_defending_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[0], 2) for x in defending_players_not_in_box_avg]
            end_x_position_defending_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.x_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[-1], 2) for x in defending_players_not_in_box_avg]
            
            start_y_position_attacking_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[0], 2) for x in attacking_players_not_in_box_avg]
            end_y_position_attacking_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[-1], 2) for x in attacking_players_not_in_box_avg]
            
            start_y_position_defending_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[0], 2) for x in defending_players_not_in_box_avg]
            end_y_position_defending_not_in_box = [np.round(merged_data_event_subset_players_not_in_box.y_Player_percent[merged_data_event_subset_players_not_in_box.Player_ID==x].iloc[-1], 2) for x in defending_players_not_in_box_avg]

            row = []
            #in here we consider three different ways: 1) players within 3m, 2) players within the triangle formed by crosser, player in box and centre of goal and 3)both of 1) and 2) simultaneously
            if len(attacking_players) > 0:
                for p in attacking_players:
                    number_close_players = []
                    distance_closest_player = []
                    for time_player in merged_data_event_subset_players.Time.loc[merged_data_event_subset_players.Player_ID==p].unique().tolist():
                        merged_data_event_subset_players_time = merged_data_event_subset_players[(merged_data_event_subset_players.Time==time_player)].drop_duplicates(['Player_ID'])
                        distances = np.asarray(np.sqrt((merged_data_event_subset_players_time.x_Player.loc[merged_data_event_subset_players_time.Is_Home_Away!=current_team_home_away] - merged_data_event_subset_players_time.x_Player.loc[merged_data_event_subset_players_time.Player_ID==p].iloc[0])**2 + (merged_data_event_subset_players_time.y_Player.loc[merged_data_event_subset_players_time.Is_Home_Away!=current_team_home_away] - merged_data_event_subset_players_time.y_Player.loc[merged_data_event_subset_players_time.Player_ID==p].iloc[0])**2).tolist())
                        players = np.asarray(merged_data_event_subset_players_time.Player_ID.loc[merged_data_event_subset_players_time.Is_Home_Away!=current_team_home_away])
                        players_close = len(players[distances <= 3.0])
                        number_close_players.append(players_close)
                        distance_closest_player.append(np.min(distances)) #need to modify the code as there can be cases where a player gets in and out of the area more than once. Visualise cross id 8 to get the idea.
                    avg_number_close_players = np.round(np.mean(number_close_players),2)
                    avg_distance_closest_player = np.round(np.mean(distance_closest_player), 2)
                    row.append([avg_number_close_players, avg_distance_closest_player])
            else:
                avg_number_close_players = None
                avg_distance_closest_player = None
                row.append([avg_number_close_players, avg_distance_closest_player])

            avg_number_close_players, avg_distance_closest_player = ['; '.join([str(x[j]) for x in row]) for j in range(len(row[0]))]


            data_box = ['; '.join(['p' + str(x) for x in attacking_players_avg]), '; '.join(attacking_players_names), '; '.join(['p' + str(x) for x in defending_players_avg]), '; '.join(defending_players_names),
                #'; '.join(['p' + str(x) for x in attacking_players_first_part]), '; '.join(attacking_players_names_first_part), '; '.join(['p' + str(x) for x in defending_players_first_part]), '; '.join(defending_players_names_first_part),
                #'; '.join(['p' + str(x) for x in attacking_players_second_part]), '; '.join(attacking_players_names_second_part), '; '.join(['p' + str(x) for x in defending_players_second_part]), '; '.join(defending_players_names_second_part),
                #'; '.join(['p' + str(x) for x in attacking_players_third_part]), '; '.join(attacking_players_names_third_part), '; '.join(['p' + str(x) for x in defending_players_third_part]), '; '.join(defending_players_names_third_part),
                '; '.join(['p' + str(x) for x in attacking_players_first_part_downsampled]), '; '.join(attacking_players_names_first_part_downsampled), '; '.join(['p' + str(x) for x in defending_players_first_part_downsampled]), '; '.join(defending_players_names_first_part_downsampled),
                '; '.join(['p' + str(x) for x in attacking_players_second_part_downsampled]), '; '.join(attacking_players_names_second_part_downsampled), '; '.join(['p' + str(x) for x in defending_players_second_part_downsampled]), '; '.join(defending_players_names_second_part_downsampled),
                '; '.join(['p' + str(x) for x in attacking_players_third_part_downsampled]), '; '.join(attacking_players_names_third_part_downsampled), '; '.join(['p' + str(x) for x in defending_players_third_part_downsampled]), '; '.join(defending_players_names_third_part_downsampled),
                '; '.join([str(x) for x in avg_dist_from_centre_of_goal_attacking]), '; '.join([str(x) for x in max_dist_from_centre_of_goal_attacking]), '; '.join([str(x) for x in min_dist_from_centre_of_goal_attacking]),
                '; '.join([str(x) for x in avg_dist_from_centre_of_goal_defending]), '; '.join([str(x) for x in max_dist_from_centre_of_goal_defending]), '; '.join([str(x) for x in min_dist_from_centre_of_goal_defending]),
                #'; '.join([str(x) for x in avg_dist_from_ball_attacking]), '; '.join([str(x) for x in max_dist_from_ball_attacking]), '; '.join([str(x) for x in min_dist_from_ball_attacking]),
                #'; '.join([str(x) for x in avg_dist_from_ball_defending]), '; '.join([str(x) for x in max_dist_from_ball_defending]), '; '.join([str(x) for x in min_dist_from_ball_defending]),
                '; '.join([str(x) for x in avg_x_position_attacking]), '; '.join([str(x) for x in avg_y_position_attacking]), 
                #'; '.join([str(x) for x in avg_x_position_attacking_first_part]), '; '.join([str(x) for x in avg_y_position_attacking_first_part]),
                #'; '.join([str(x) for x in avg_x_position_attacking_second_part]), '; '.join([str(x) for x in avg_y_position_attacking_second_part]),
                #'; '.join([str(x) for x in avg_x_position_attacking_third_part]), '; '.join([str(x) for x in avg_y_position_attacking_third_part]),
                '; '.join([str(x) for x in avg_x_position_attacking_first_part_downsampled]), '; '.join([str(x) for x in avg_y_position_attacking_first_part_downsampled]),
                '; '.join([str(x) for x in avg_x_position_attacking_second_part_downsampled]), '; '.join([str(x) for x in avg_y_position_attacking_second_part_downsampled]),
                '; '.join([str(x) for x in avg_x_position_attacking_third_part_downsampled]), '; '.join([str(x) for x in avg_y_position_attacking_third_part_downsampled]),
                '; '.join([str(x) for x in start_x_position_attacking]), '; '.join([str(x) for x in start_y_position_attacking]), 
                '; '.join([str(x) for x in end_x_position_attacking]), '; '.join([str(x) for x in end_y_position_attacking]),
                '; '.join([str(x) for x in avg_x_position_defending]), '; '.join([str(x) for x in avg_y_position_defending]), 
                #'; '.join([str(x) for x in avg_x_position_defending_first_part]), '; '.join([str(x) for x in avg_y_position_defending_first_part]),
                #'; '.join([str(x) for x in avg_x_position_defending_second_part]), '; '.join([str(x) for x in avg_y_position_defending_second_part]),
                #'; '.join([str(x) for x in avg_x_position_defending_third_part]), '; '.join([str(x) for x in avg_y_position_defending_third_part]),
                '; '.join([str(x) for x in avg_x_position_defending_first_part_downsampled]), '; '.join([str(x) for x in avg_y_position_defending_first_part_downsampled]),
                '; '.join([str(x) for x in avg_x_position_defending_second_part_downsampled]), '; '.join([str(x) for x in avg_y_position_defending_second_part_downsampled]),
                '; '.join([str(x) for x in avg_x_position_defending_third_part_downsampled]), '; '.join([str(x) for x in avg_y_position_defending_third_part_downsampled]),
                '; '.join([str(x) for x in start_x_position_defending]), '; '.join([str(x) for x in start_y_position_defending]), 
                '; '.join([str(x) for x in end_x_position_defending]), '; '.join([str(x) for x in end_y_position_defending]), 
                ';'.join([str(x) for x in frame_attacking_first_part_downsampled]), ';'.join([str(x) for x in frame_attacking_second_part_downsampled]), ';'.join([str(x) for x in frame_attacking_third_part_downsampled]),
                ';'.join([str(x) for x in frame_defending_first_part_downsampled]), ';'.join([str(x) for x in frame_defending_second_part_downsampled]), ';'.join([str(x) for x in frame_defending_third_part_downsampled]),
                avg_number_close_players, avg_distance_closest_player]

            data_outside_box = ['; '.join(['p' + str(x) for x in attacking_players_not_in_box_avg]), '; '.join(attacking_players_names_not_in_box), '; '.join(['p' + str(x) for x in defending_players_not_in_box_avg]), '; '.join(defending_players_names_not_in_box),
                #'; '.join(['p' + str(x) for x in attacking_players_not_in_box_first_part]), '; '.join(attacking_players_names_not_in_box_first_part), '; '.join(['p' + str(x) for x in defending_players_not_in_box_first_part]), '; '.join(defending_players_names_not_in_box_first_part),
                #'; '.join(['p' + str(x) for x in attacking_players_not_in_box_second_part]), '; '.join(attacking_players_names_not_in_box_second_part), '; '.join(['p' + str(x) for x in defending_players_not_in_box_second_part]), '; '.join(defending_players_names_not_in_box_second_part),
                #'; '.join(['p' + str(x) for x in attacking_players_not_in_box_third_part]), '; '.join(attacking_players_names_not_in_box_third_part), '; '.join(['p' + str(x) for x in defending_players_not_in_box_third_part]), '; '.join(defending_players_names_not_in_box_third_part),
                '; '.join(['p' + str(x) for x in attacking_players_not_in_box_first_part_downsampled]), '; '.join(attacking_players_names_not_in_box_first_part_downsampled), '; '.join(['p' + str(x) for x in defending_players_not_in_box_first_part_downsampled]), '; '.join(defending_players_names_not_in_box_first_part_downsampled),
                '; '.join(['p' + str(x) for x in attacking_players_not_in_box_second_part_downsampled]), '; '.join(attacking_players_names_not_in_box_second_part_downsampled), '; '.join(['p' + str(x) for x in defending_players_not_in_box_second_part_downsampled]), '; '.join(defending_players_names_not_in_box_second_part_downsampled),
                '; '.join(['p' + str(x) for x in attacking_players_not_in_box_third_part_downsampled]), '; '.join(attacking_players_names_not_in_box_third_part_downsampled), '; '.join(['p' + str(x) for x in defending_players_not_in_box_third_part_downsampled]), '; '.join(defending_players_names_not_in_box_third_part_downsampled),
                '; '.join([str(x) for x in avg_dist_from_centre_of_goal_attacking_not_in_box]), '; '.join([str(x) for x in max_dist_from_centre_of_goal_attacking_not_in_box]), '; '.join([str(x) for x in min_dist_from_centre_of_goal_attacking_not_in_box]),
                '; '.join([str(x) for x in avg_dist_from_centre_of_goal_defending_not_in_box]), '; '.join([str(x) for x in max_dist_from_centre_of_goal_defending_not_in_box]), '; '.join([str(x) for x in min_dist_from_centre_of_goal_defending_not_in_box]),
                #'; '.join([str(x) for x in avg_dist_from_ball_attacking]), '; '.join([str(x) for x in max_dist_from_ball_attacking]), '; '.join([str(x) for x in min_dist_from_ball_attacking]),
                #'; '.join([str(x) for x in avg_dist_from_ball_defending]), '; '.join([str(x) for x in max_dist_from_ball_defending]), '; '.join([str(x) for x in min_dist_from_ball_defending]),
                '; '.join([str(x) for x in avg_x_position_attacking_not_in_box]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box]), 
                #'; '.join([str(x) for x in avg_x_position_attacking_not_in_box_first_part]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box_first_part]), 
                #'; '.join([str(x) for x in avg_x_position_attacking_not_in_box_second_part]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box_second_part]), 
                #'; '.join([str(x) for x in avg_x_position_attacking_not_in_box_third_part]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box_third_part]), 
                '; '.join([str(x) for x in avg_x_position_attacking_not_in_box_first_part_downsampled]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box_first_part_downsampled]), 
                '; '.join([str(x) for x in avg_x_position_attacking_not_in_box_second_part_downsampled]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box_second_part_downsampled]), 
                '; '.join([str(x) for x in avg_x_position_attacking_not_in_box_third_part_downsampled]), '; '.join([str(x) for x in avg_y_position_attacking_not_in_box_third_part_downsampled]), 
                '; '.join([str(x) for x in start_x_position_attacking_not_in_box]), '; '.join([str(x) for x in start_y_position_attacking_not_in_box]), 
                '; '.join([str(x) for x in end_x_position_attacking_not_in_box]), '; '.join([str(x) for x in end_y_position_attacking_not_in_box]),
                '; '.join([str(x) for x in avg_x_position_defending_not_in_box]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box]), 
                #'; '.join([str(x) for x in avg_x_position_defending_not_in_box_first_part]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box_first_part]), 
                #'; '.join([str(x) for x in avg_x_position_defending_not_in_box_second_part]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box_second_part]), 
                #'; '.join([str(x) for x in avg_x_position_defending_not_in_box_third_part]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box_third_part]), 
                '; '.join([str(x) for x in avg_x_position_defending_not_in_box_first_part_downsampled]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box_first_part_downsampled]), 
                '; '.join([str(x) for x in avg_x_position_defending_not_in_box_second_part_downsampled]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box_second_part_downsampled]), 
                '; '.join([str(x) for x in avg_x_position_defending_not_in_box_third_part_downsampled]), '; '.join([str(x) for x in avg_y_position_defending_not_in_box_third_part_downsampled]), 
                '; '.join([str(x) for x in start_x_position_defending_not_in_box]), '; '.join([str(x) for x in start_y_position_defending_not_in_box]), 
                '; '.join([str(x) for x in end_x_position_defending_not_in_box]), '; '.join([str(x) for x in end_y_position_defending_not_in_box]),
                ';'.join([str(x) for x in frame_attacking_first_part_downsampled_no_box]), ';'.join([str(x) for x in frame_attacking_second_part_downsampled_no_box]), ';'.join([str(x) for x in frame_attacking_third_part_downsampled_no_box]),
                ';'.join([str(x) for x in frame_defending_first_part_downsampled_no_box]), ';'.join([str(x) for x in frame_defending_second_part_downsampled_no_box]), ';'.join([str(x) for x in frame_defending_third_part_downsampled_no_box])]
        

            data_box_df = pd.DataFrame([data_box], columns = ['Attacking Player IDs In Box', 'Attacking Player Names In Box', 
                'Defending Player IDs In Box', 'Defending Player Names In Box', 'Attacking Player IDs In Box First Part', 'Attacking Player Names In Box First Part', 
                'Defending Player IDs In Box First Part', 'Defending Player Names In Box First Part', 'Attacking Player IDs In Box Second Part', 'Attacking Player Names In Box Second Part', 
                'Defending Player IDs In Box Second Part', 'Defending Player Names In Box Second Part', 'Attacking Player IDs In Box Third Part', 'Attacking Player Names In Box Third Part', 
                'Defending Player IDs In Box Third Part', 'Defending Player Names In Box Third Part', 'Attacking Players Average Distance From Centre Of Goal', 
                'Attacking Players Max Distance From Centre Of Goal', 'Attacking Players Min Distance From Centre Of Goal',
                'Defending Players Average Distance From Centre Of Goal', 
                'Defending Players Max Distance From Centre Of Goal', 'Defending Players Min Distance From Centre Of Goal',
                #'Attacking Players Average Distance From Ball', 'Attacking Players Max Distance From Ball', 'Attacking Players Min Distance From Ball',
                #'Defending Players Average Distance From Ball', 'Defending Players Max Distance From Ball', 'Defending Players Min Distance From Ball',
                'Attacking Players Average X Position', 'Attacking Players Average Y Position', 'Attacking Players Average X Position First Part', 'Attacking Players Average Y Position First Part', 'Attacking Players Average X Position Second Part', 'Attacking Players Average Y Position Second Part', 'Attacking Players Average X Position Third Part', 'Attacking Players Average Y Position Third Part', 
                'Attacking Players Starting X Position', 'Attacking Players Starting Y Position', 
                'Attacking Players Ending X Position', 'Attacking Players Ending Y Position',
                'Defending Players Average X Position', 'Defending Players Average Y Position', 'Defending Players Average X Position First Part', 'Defending Players Average Y Position First Part', 'Defending Players Average X Position Second Part', 'Defending Players Average Y Position Second Part', 'Defending Players Average X Position Third Part', 'Defending Players Average Y Position Third Part', 
                'Defending Players Starting X Position', 'Defending Players Starting Y Position', 
                'Defending Players Ending X Position', 'Defending Players Ending Y Position', 
                'Attacking Frames First Part Downsampled', 'Attacking Frames Second Part Downsampled', 'Attacking Frames Third Part Downsampled',
                'Defending Frames First Part Downsampled', 'Defending Frames Second Part Downsampled', 'Defending Frames Third Part Downsampled',
                'Average Number Of Defending Players Within 3 Meters From Attacking Players', 'Average distance Of Closest Defender to Attacking Players'])
            #players_df = pd.DataFrame(player_entry, columns = ['Player_ID', 'Player_Name', 'Player_Team_id', 'Player_Team_Name', 'Attacking_or_Defending', 'Avg_Dist_from_Centre_of_Goal', 'Start_Dist_From_Centre_of_Goal', 'Closest_Dist_From_Centre_of_Goal', 'Avg_Number_Players_within_3m', 'Avg_Dist_Closest_Player', 'Already_in_Box', 'Time_of_Entry', 'Exit_from_Box', 'Time_of_Exit', 'x_entry', 'y_entry', 'box_entry_part', 'Speed_of_Entry', 'time_spent_in_box', 'time_spent_in_box_relative_to_duration_of_window', 'time_spent_in_box_at_high_speed', 'time_spent_in_box_at_high_speed_relative_to_duration_of_window', 'time_spent_in_box_at_high_speed_relative_to_time_spent_in_box', 'window_duration', 'Number_Attacking_Players', 'Number_Defending_Players_no_GK'])

            data_outside_box_df = pd.DataFrame([data_outside_box], columns = ['Attacking Player IDs Outside Box', 'Attacking Player Names Outside Box', 
                'Defending Player IDs Outside Box', 'Defending Player Names Outside Box', 'Attacking Player IDs Outside Box First Part', 'Attacking Player Names Outside Box First Part', 
                'Defending Player IDs Outside Box First Part', 'Defending Player Names Outside Box First Part', 'Attacking Player IDs Outside Box Second Part', 'Attacking Player Names Outside Box Second Part', 
                'Defending Player IDs Outside Box Second Part', 'Defending Player Names Outside Box Second Part', 'Attacking Player IDs Outside Box Third Part', 'Attacking Player Names Outside Box Third Part', 
                'Defending Player IDs Outside Box Third Part', 'Defending Player Names Outside Box Third Part', 'Attacking Players Average Distance From Centre Of Goal Outside Box', 
                'Attacking Players Max Distance From Centre Of Goal Outside Box', 'Attacking Players Min Distance From Centre Of Goal Outside Box',
                'Defending Players Average Distance From Centre Of Goal Outside Box', 
                'Defending Players Max Distance From Centre Of Goal Outside Box', 'Defending Players Min Distance From Centre Of Goal Outside Box',
                #'Attacking Players Average Distance From Ball', 'Attacking Players Max Distance From Ball', 'Attacking Players Min Distance From Ball',
                #'Defending Players Average Distance From Ball', 'Defending Players Max Distance From Ball', 'Defending Players Min Distance From Ball',
                'Attacking Players Average X Position Outside Box', 'Attacking Players Average Y Position Outside Box', 'Attacking Players Average X Position Outside Box First Part', 'Attacking Players Average Y Position Outside Box First Part', 'Attacking Players Average X Position Outside Box Second Part', 'Attacking Players Average Y Position Outside Box Second Part', 'Attacking Players Average X Position Outside Box Third Part', 'Attacking Players Average Y Position Outside Box Third Part', 
                'Attacking Players Starting X Position Outside Box', 'Attacking Players Starting Y Position Outside Box', 
                'Attacking Players Ending X Position Outside Box', 'Attacking Players Ending Y Position Outside Box',
                'Defending Players Average X Position Outside Box', 'Defending Players Average Y Position Outside Box', 'Defending Players Average X Position Outside Box First Part', 'Defending Players Average Y Position Outside Box First Part', 'Defending Players Average X Position Outside Box Second Part', 'Defending Players Average Y Position Outside Box Second Part', 'Defending Players Average X Position Outside Box Third Part', 'Defending Players Average Y Position Outside Box Third Part',  
                'Defending Players Starting X Position Outside Box', 'Defending Players Starting Y Position Outside Box', 
                'Defending Players Ending X Position Outside Box', 'Defending Players Ending Y Position Outside Box',
                'Attacking Frames First Part Downsampled Outside Box', 'Attacking Frames Second Part Downsampled Outside Box', 'Attacking Frames Third Part Downsampled Outside Box',
                'Defending Frames First Part Downsampled Outside Box', 'Defending Frames Second Part Downsampled Outside Box', 'Defending Frames Third Part Downsampled Outside Box'
                #,
                #'Average Number Of Defending Players Within 3 Meters From Attacking Players', 
                #'Average distance Of Closest Defender to Attacking Players'
                ])


            #use type and qualifier ids explanations to add more context
            #cross_qualifiers = ', '.join([x.encode('utf-8') for x in qualifier_type_id.name[qualifier_type_id.qualifier_id.isin(final_event_data_only_crosses[(final_event_data_only_crosses.cross_id==cross_id+1) & (~final_event_data_only_crosses.qualifier_id.isin([140,141,212,213,233,56]))].qualifier_id.sort_values())].tolist()])
            cross_qualifiers = ', '.join([x for x in qualifier_type_id.name[qualifier_type_id.qualifier_id.isin(final_event_data_only_crosses[(final_event_data_only_crosses.cross_id==cross_id+1) & (final_event_data_only_crosses.value.isnull())].qualifier_id.sort_values())].tolist()])



            #get the information we need from the main dataset
            #merged_data_event_subset_players_unique = merged_data_event_subset_players[merged_data_event_subset_players.Player_ID!=current_player][['Period_ID', 'Is_Home_Away', 'Player_ID', '@Status', 'position_in_pitch']].drop_duplicates()
            
            #both_sources = merged_data_event_subset_players_unique.merge(data_box_df, how = 'inner')
            #both_sources['merger'] = 1
            data_box_df['merger'] = 1
            data_outside_box_df['merger'] = 1
            #after_cross_events_df['merger'] = 1
            #row_to_export['merger'] = 1
            cross_opposition_df['merger'] = 1
            both_sources = data_box_df.merge(data_outside_box_df).merge(cross_opposition_df).drop(['merger'], axis = 1)
            #.merge(row_to_export)
            #.merge(after_cross_events_df)

            #we add the other columns we need
            both_sources['Game ID'] = 'f' + str(game_id)
            date_game = pd.to_datetime(game_date)
            both_sources['Date'] = str(date_game.date())
            #fixture = home_team_name + ' v ' + away_team_name
            #both_sources['fixture'] = fixture
            #both_sources['Competition ID'] = event_data_filtered.competition_id.unique()[0]
            both_sources['Competition Name'] = event_data_filtered.competition_name.unique()[0]
            both_sources['Team ID'] = 't' + str(current_team_id)
            both_sources['Team Name'] = current_team
            both_sources['Opposition Team ID'] = opposing_team_id
            both_sources['Opposition Team Name'] = opposing_team
            both_sources['OPTA Event ID'] = int(event_data_open_play_crosses_final.unique_event_id.iloc[cross_id])
            both_sources['OPTA Qualifiers'] = cross_qualifiers
            opta_min, opta_sec = divmod(opta_time, 60)
            #opta_sec = np.round(opta_sec, 3)
            #opta_sec, opta_ms = str(opta_sec).split('.')
            opta_min = str(int(np.where(current_period==2, int(opta_min) + 45, 
                np.where(current_period==3, int(opta_min) + 90, 
                    np.where(current_period==4, int(opta_min) + 105, int(opta_min))))))
            opta_sec = str(int(opta_sec))
            opta_min = opta_min.zfill(3 - len(opta_min))
            opta_sec = opta_sec.zfill(3 - len(opta_sec))
            #while len(opta_ms) < 3:
                #opta_ms += '0'
            #opta_ms = int(opta_ms)
            both_sources['Period ID'] = current_period
            both_sources['Time'] = '{}:{}'.format(opta_min, opta_sec)
            both_sources['Time In Seconds'] = opta_time - time_offset
            #both_sources['Start_Window'] = start_window
            #both_sources['End_Window'] = end_window
            both_sources['Outcome'] = outcome
            #both_sources['chipped'] = chipped
            both_sources['Player Crosser ID'] = 'p' + str(current_player)
            both_sources['Player Crosser Name'] = current_player_name
            both_sources['Player Crosser Status'] = current_player_status
            #both_sources['Time In Possession Before Cross'] = time_in_possession_before_cross
            #both_sources['First Touch Cross'] = first_touch_cross
            #both_sources['player_crosser_position_in_pitch'] = current_player_pos_in_pitch
            both_sources['X Crosser OPTA'] = x_crosser
            both_sources['Y Crosser OPTA'] = y_crosser
            both_sources['X Crosser Second Spectrum'] = x_crosser_tracking
            both_sources['Y Crosser Second Spectrum'] = y_crosser_tracking
            both_sources['X Crosser Second Spectrum First Part Downsampled'] = x_crosser_tracking_first_part_downsampled
            both_sources['Y Crosser Second Spectrum First Part Downsampled'] = y_crosser_tracking_first_part_downsampled
            both_sources['X Crosser Second Spectrum Second Part Downsampled'] = x_crosser_tracking_second_part_downsampled
            both_sources['Y Crosser Second Spectrum Second Part Downsampled'] = y_crosser_tracking_second_part_downsampled
            both_sources['X Crosser Second Spectrum Third Part Downsampled'] = x_crosser_tracking_third_part_downsampled
            both_sources['Y Crosser Second Spectrum Third Part Downsampled'] = y_crosser_tracking_third_part_downsampled
            both_sources['X Crosser Second Spectrum Start'] = x_crosser_tracking_start
            both_sources['Y Crosser Second Spectrum Start'] = y_crosser_tracking_start
            both_sources['X Crosser Second Spectrum End'] = x_crosser_tracking_end
            both_sources['Y Crosser Second Spectrum End'] = y_crosser_tracking_end
            both_sources['Frame Crosser First Part Downsampled'] = frame_crosser_tracking_first_part_downsampled
            both_sources['Frame Crosser Second Part Downsampled'] = frame_crosser_tracking_second_part_downsampled
            both_sources['Frame Crosser Third Part Downsampled'] = frame_crosser_tracking_third_part_downsampled
            both_sources['Distance Of Crosser From Centre Of Goal'] = dist_crosser_from_centre_of_goal
            both_sources['Angle Of Crosser From Centre Of Goal'] = angle_crosser_from_centre_of_goal

            both_sources['Left/Right'] = np.where(y_crosser < 50.0, 'right', 'left').tolist()
            both_sources['Wide/Centred'] = np.where((y_crosser < 21.1/2.0) | (y_crosser > 78.9 + (100.0 - 78.9)/2.0), 'far wide', np.where((y_crosser < 21.1) | (y_crosser > 78.9), 'centred wide', 'centred')).tolist()
            both_sources['Cross Category'] = np.where(x_crosser < 83, 'early', np.where(x_crosser >= 94.2, 'deep', 'lateral')).tolist()
            both_sources['Inside/Outside Box'] = np.where((x_crosser >= 83.0) & (y_crosser >= 21.1) & (y_crosser <= 78.9), 'inside', 'outside').tolist()
            both_sources['X Ball Final'] = x_final_ball
            both_sources['Y Ball Final'] = y_final_ball
            both_sources['X Ball Final Second Spectrum'] = dat_for_ball_restricted.x_Ball_percent.iloc[-1]
            both_sources['Y Ball Final Second Spectrum'] = dat_for_ball_restricted.y_Ball_percent.iloc[-1]
            
            both_sources['Final Ball Before/After GK Area'] = None 
            both_sources['Final Ball Which Part Of The Area'] = None 
            both_sources['Same Side Of Cross'] = None 
            if x_final_ball is not None:
                both_sources['Final Ball Before/After GK Area'] = np.where(x_final_ball >= 94.2, 'after', 'before')
                both_sources['Final Ball Which Part Of The Area'] = np.where((y_final_ball >= 21.1) & (y_final_ball < 36.8), 'right', np.where((y_final_ball <= 78.9) & (y_final_ball >= 63.2), 'left', np.where((y_final_ball < 63.2) & (y_final_ball >= 36.8), 'centre', 'out')))
                both_sources['Same Side Of Cross'] = np.where(both_sources['Final Ball Which Part Of The Area']==both_sources['Left/Right'], 'same', np.where((both_sources['Final Ball Which Part Of The Area']=='left') | (both_sources['Final Ball Which Part Of The Area']=='right'), 'opposite', both_sources['Final Ball Which Part Of The Area']))
            #both_sources['opta_time_of_cross'] = opta_time
            #both_sources['opta_time_next_event'] = opta_time_following_event
            both_sources['Keypass'] = keypass
            both_sources['Assist'] = assist
            #both_sources['Number Of Attacking Players In Box'] = len(attacking_players_avg)
            #both_sources['Number Of Defending Players In Box'] = len(defending_players_avg)
            #both_sources['Number Of Attacking Players In Box First Part'] = len(attacking_players_first_part)
            #both_sources['Number Of Defending Players In Box First Part'] = len(defending_players_first_part)
            #both_sources['Number Of Attacking Players In Box Second Part'] = len(attacking_players_second_part)
            #both_sources['Number Of Defending Players In Box Second Part'] = len(defending_players_second_part)
            #both_sources['Number Of Attacking Players In Box Third Part'] = len(attacking_players_third_part)
            #both_sources['Number Of Defending Players In Box Third Part'] = len(defending_players_third_part)
            both_sources['Start Window'] = start_window
            both_sources['End Window'] = end_window
            both_sources['Start Frame'] = start_frame
            both_sources['End Frame'] = end_frame
            both_sources['Total Frames In Window'] = tot_frames + 2
            both_sources['Total Frames In Window First Part'] = tot_frames_first_part
            both_sources['Total Frames In Window Second Part'] = tot_frames_second_part
            both_sources['Total Frames In Window Third Part'] = tot_frames_third_part
            #both_sources['End Window Ball'] = dat_for_ball_restricted.Time.max()

            #both_sources['estimated_tracking_time_of_cross'] = estimated_start_time_cross
            #both_sources['estimated_tracking_time_of_end_cross'] = estimated_end_time_cross
            both_sources['avg_height_ball'] = dat_for_ball_restricted.z_Ball.mean()
            #both_sources['median_height_ball'] = dat_for_ball_restricted.z_Ball.median()
            #both_sources['sd_height_ball'] = dat_for_ball_restricted.z_Ball.std()
            both_sources['min_height_ball'] = dat_for_ball_restricted.z_Ball.min()
            both_sources['max_height_ball'] = dat_for_ball_restricted.z_Ball.max()
            both_sources['start_height_ball'] = dat_for_ball_restricted.z_Ball.iloc[0]
            both_sources['end_height_ball'] = dat_for_ball_restricted.z_Ball.iloc[-1]
            #both_sources['low_or_high_cross'] = np.where(max_height_ball > 1.5, 'high', 'low').tolist()
            #both_sources['height_ball_at_start'] = dat_for_ball_restricted.z_Ball.iloc[0]
            #both_sources['height_ball_at_end'] = dat_for_ball_restricted.z_Ball.iloc[dat_for_ball_restricted.shape[0]-1]
            #both_sources['x_ball_max_height'] = dat_for_ball_restricted[dat_for_ball_restricted.z_Ball==dat_for_ball_restricted.z_Ball.max()].x_Ball_percent.mean()
            #both_sources['y_ball_max_height'] = dat_for_ball_restricted[dat_for_ball_restricted.z_Ball==dat_for_ball_restricted.z_Ball.max()].y_Ball_percent.mean()
            both_sources['avg_speed_ball_at_max_height'] = dat_for_ball_restricted.Speed_Ball[dat_for_ball_restricted.z_Ball==dat_for_ball_restricted.z_Ball.max()].mean()
            #both_sources['median_speed_ball'] = dat_for_ball_restricted.Speed_Ball.median()
            #both_sources['sd_speed_ball'] = dat_for_ball_restricted.Speed_Ball.std()
            both_sources['avg_speed_ball'] = dat_for_ball_restricted.Speed_Ball.mean()
            both_sources['min_speed_ball'] = dat_for_ball_restricted.Speed_Ball.min()
            both_sources['max_speed_ball'] = dat_for_ball_restricted.Speed_Ball.max()
            both_sources['speed_ball_at_start'] = dat_for_ball_restricted.Speed_Ball.iloc[0]
            both_sources['speed_ball_at_end'] = dat_for_ball_restricted.Speed_Ball.iloc[-1]
            #both_sources['x_ball_max_speed'] = dat_for_ball_restricted[dat_for_ball_restricted.Speed_Ball==dat_for_ball_restricted.Speed_Ball.max()].x_Ball_percent.mean()
            #both_sources['y_ball_max_speed'] = dat_for_ball_restricted[dat_for_ball_restricted.Speed_Ball==dat_for_ball_restricted.Speed_Ball.max()].y_Ball_percent.mean()
            #both_sources['cross_id'] = cross_id + 1
            both_sources['ball_out'] = ball_out
            both_sources['intercepted'] = intercepted
            both_sources['blocked'] = blocked
            both_sources['cleared'] = cleared
            if event_data_after_crosses_event.shape[0] > 0:  
                both_sources['Type ID After Cross'] = final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].type_id.iloc[0]
                both_sources['OPTA Event ID After Cross'] = final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].unique_event_id.iloc[0]
                if final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].type_id.iloc[0] in [4, 44]:
                    both_sources['OPTA Event ID After Cross'] = ', '.join([str(int(final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].unique_event_id.iloc[0])), 
                        str(int(final_event_data[(final_event_data.unique_event_id==event_data_after_crosses_event.unique_event_id.iloc[0])].unique_event_id.iloc[0]))])
            else:
                both_sources['Type ID After Cross'] = None
                both_sources['OPTA Event ID After Cross'] = None

            
            if event_type == 'Cross':
                both_sources['Cross Type'] = event_data_open_play_crosses_final['Cross Type'].iloc[cross_id]

            if event_type == 'Set Piece':
                both_sources['Set Piece Type'] = event_data_open_play_crosses_final['Set Piece Type'].iloc[cross_id]


            data_export.append(both_sources)
            data_export = pd.concat(data_export).sort_values(['Period ID', 'Time In Seconds'])

            return data_export


    @staticmethod
    def which_team_from_left_to_right(event_data_filtered, period):
        """[summary]

        Args:
            period (int): [description]

        Returns:
            str: [description]


        """
        return int(event_data_filtered['team_id'][(event_data_filtered['value']=='Left to Right') & (event_data_filtered['period_id']==period)])

    @staticmethod
    def get_events_after_crosses(data_event_full, data_event_crosses):
        """[summary]

       Args:
           data_event_full (pd.DataFrame): [description]
           data_event_crosses (pd.DataFrame): [description]

       Returns:
           pd.DataFrame: [description]


       """
        x = pd.DataFrame(columns = ['game_id', 'unique_event_id', 'away_team_id', 'home_team_id', 'event_id', 'type_id', 'period_id', 'team_id', 'Time_in_Seconds', 'total_seconds_in_period', 'cross_id'])
        for i in range(data_event_crosses.shape[0]):
            next_game = data_event_crosses.game_id.iloc[i]
            next_period = data_event_crosses.period_id.iloc[i]
            time_cross = data_event_crosses.Time_in_Seconds.iloc[i] 
            #we use the rounded down time of event here because we do not discard combined events just because of minimal time differences (say 10 ms)
            where_relevant_pass = np.where(data_event_full.unique_event_id==data_event_crosses.unique_event_id.iloc[i])[0][-1]
            dat = data_event_full[(data_event_full.game_id==next_game) & ((data_event_full.Time_in_Seconds - time_cross) >= 0) & (data_event_full.period_id==next_period) & (~data_event_full.unique_event_id.isin(data_event_full.unique_event_id.iloc[:(where_relevant_pass+1)].unique()))][(data_event_full[(data_event_full.game_id==next_game) & (~data_event_full.unique_event_id.isin(data_event_full.unique_event_id.iloc[:(where_relevant_pass+1)].unique())) & ((data_event_full.Time_in_Seconds - time_cross) >= 0) & (data_event_full.period_id==next_period)].Time_in_Seconds) <= time_cross + 10.0][['game_id', 'unique_event_id', 'away_team_id', 'home_team_id', 'event_id', 'type_id', 'period_id', 'team_id', 'Time_in_Seconds', 'total_seconds_in_period']].drop_duplicates()
            dat['cross_id'] = i + 1
            dat = dat[(dat.unique_event_id != data_event_crosses.unique_event_id.iloc[i]) & (~dat.type_id.isin([21,34,35,36,38,39,40,43,53,55,58,63,69,70,71,77,79,83])) & (dat.total_seconds_in_period >= data_event_crosses.total_seconds_in_period.iloc[i])]
            #next_event = map(int, data_event_full.event_id[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)][(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross) == min(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross)].unique())
            #next_team_id = map(int, data_event_full.team_id[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)][(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross) == min(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross)].unique())
            x = x.append(dat)

        #df = pd.DataFrame(x, columns = ['event_id', 'period_id', 'team_id'])
        x.event_id = x.event_id.astype(int)
        x.period_id = x.period_id.astype(int)
        x.team_id = x.team_id.astype(int)
        x.game_id = x.game_id.astype(int)
        #x.unique_event_id = x.unique_event_id.astype(int)
        x.away_team_id = x.away_team_id.astype(int)
        x.home_team_id = x.home_team_id.astype(int)
        return x


    @staticmethod
    def get_events_before_crosses(data_event_full, data_event_crosses):
        """[summary]

        Args:
            data_event_full (pd.DataFrame): [description]
            data_event_crosses (pd.DataFrame): [description]

        Returns:
            pd.DataFrame: [description]


        """

        x = pd.DataFrame(columns = ['game_id', 'unique_event_id', 'away_team_id', 'home_team_id', 'event_id', 'type_id', 'period_id', 'team_id', 'Time_in_Seconds', 'total_seconds_in_period', 'cross_id'])
        for i in range(data_event_crosses.shape[0]):
            next_game = data_event_crosses.game_id.iloc[i]
            next_period = data_event_crosses.period_id.iloc[i]
            time_cross = data_event_crosses.Time_in_Seconds.iloc[i] 
            #we use the rounded down time of event here because we do not discard combined events just because of minimal time differences (say 10 ms)
            where_relevant_pass = np.where(data_event_full.unique_event_id==data_event_crosses.unique_event_id.iloc[i])[0][-1]
            dat = data_event_full[(data_event_full.game_id==next_game) & ((data_event_full.Time_in_Seconds - time_cross) <= 0) & (data_event_full.period_id==next_period) & (~data_event_full.unique_event_id.isin(data_event_full.unique_event_id.iloc[(where_relevant_pass):].unique()))][(data_event_full[(data_event_full.game_id==next_game) & (~data_event_full.unique_event_id.isin(data_event_full.unique_event_id.iloc[(where_relevant_pass):].unique())) & ((data_event_full.Time_in_Seconds - time_cross) <= 0) & (data_event_full.period_id==next_period)].Time_in_Seconds) >= time_cross - 10.0][['game_id', 'unique_event_id', 'away_team_id', 'home_team_id', 'event_id', 'type_id', 'period_id', 'team_id', 'Time_in_Seconds', 'total_seconds_in_period']].drop_duplicates()
            dat['cross_id'] = i + 1
            dat = dat[(dat.unique_event_id != data_event_crosses.unique_event_id.iloc[i]) & (~dat.type_id.isin([21,34,35,36,38,39,40,43,53,55,58,63,69,70,71,77,79,83])) & (dat.total_seconds_in_period <= data_event_crosses.total_seconds_in_period.iloc[i])]
            #next_event = map(int, data_event_full.event_id[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)][(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross) == min(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross)].unique())
            #next_team_id = map(int, data_event_full.team_id[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)][(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross) == min(data_event_full[((data_event_full.Time_in_Seconds - time_cross) > 0) & (data_event_full.period_id==next_period)].Time_in_Seconds - time_cross)].unique())
            x = x.append(dat)

        #df = pd.DataFrame(x, columns = ['event_id', 'period_id', 'team_id'])
        x.event_id = x.event_id.astype(int)
        x.period_id = x.period_id.astype(int)
        x.team_id = x.team_id.astype(int)
        x.game_id = x.game_id.astype(int)
        #x.unique_event_id = x.unique_event_id.astype(int)
        x.away_team_id = x.away_team_id.astype(int)
        x.home_team_id = x.home_team_id.astype(int)
        return x


    @staticmethod
    def remove_first_letter (x):
        """[summary]

        Args:
            x (str): [description]

        Returns:
            str: [description]

        """

        return x[1:]


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

        #take into consideration two sides
        #a: v1v2
        #b: v1v3
        #v1 is the common vertex on which we compute the angle
        norm_a = np.sqrt(sum((v1 - v2)**2))
        norm_b = np.sqrt(sum((v1 - v3)**2))
        dot_product_a_b = sum((v1 - v2)*(v1 - v3))
        cos_theta = dot_product_a_b/(norm_a*norm_b) #we don't care about the sign of the cosine as since the angle is < 180 we are sure that the sin is positive
        #theta = math.acos(cos_theta)
        #sin_theta = math.sin(theta*np.pi/180) 
        sin_theta = np.sqrt((1 - cos_theta**2)) #can't be negative otherwise the angle would be larger than 180 degrees

        return (0.5*norm_a*norm_b*sin_theta)



    def largest_triangle_sampling (self, x_path, y_path, frame_path, temporary_x, temporary_y, temporary_frame,
                                   list_frames, current_player, merged_data_event_subset_players):
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

        for f in range(len(list_frames)):
            previous_point = np.array([x_path[f], 
                y_path[f], frame_path[f]])
            next_temporary_point = np.array([temporary_x[f], temporary_y[f], temporary_frame[f]])

            x_coordinates = merged_data_event_subset_players.x_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(list_frames[f]))].tolist()
            y_coordinates = merged_data_event_subset_players.y_Player_percent.loc[(merged_data_event_subset_players.Player_ID==current_player) & (merged_data_event_subset_players.Frame_ID.isin(list_frames[f]))].tolist()
            frames_coordinates = list_frames[f]

            #initialise coordinates and area
            stored_x = 0
            stored_y = 0
            stored_frame = 0
            max_triangle_area = 0

            for index in range(len(frames_coordinates)):
                current_area = self.area_triangle(previous_point, next_temporary_point, np.array([x_coordinates[index], y_coordinates[index], frames_coordinates[index]]))
                if current_area > max_triangle_area:
                    stored_x = np.round(x_coordinates[index], 2)
                    stored_y = np.round(y_coordinates[index], 2)
                    stored_frame = frames_coordinates[index]
                    max_triangle_area = current_area

            x_path.append(stored_x)
            y_path.append(stored_y)
            frame_path.append(stored_frame)

        x_first_part_downsampled, x_second_part_downsampled, x_third_part_downsampled = x_path[1:]
        y_first_part_downsampled, y_second_part_downsampled, y_third_part_downsampled = y_path[1:]
        frame_first_part_downsampled, frame_second_part_downsampled, frame_third_part_downsampled = frame_path[1:]


        return (frame_first_part_downsampled, frame_second_part_downsampled, frame_third_part_downsampled, x_first_part_downsampled, x_second_part_downsampled, x_third_part_downsampled, y_first_part_downsampled, y_second_part_downsampled, y_third_part_downsampled)



    def distance_coordinates_centre_y(self, y):
        """[summary]

        Args:
            y (): [description]

        Returns:
            float: [description]

        """
        return abs(y/100*self.width - 0.5*self.width)



    def distance_coordinates_start2(self, x, y, xcrosser, ycrosser):
        return np.sqrt((x/100*self.length - xcrosser/100*self.length)**2 + (y/100*self.width - ycrosser/100*self.width)**2)

