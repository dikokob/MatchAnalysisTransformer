import pandas as pd
import numpy as np
import logging
import os
import json
import time
import xmltodict


class SetPieceClassificationTansformer:
    config = None
    logger = None
    data_source = None
    match_date = None

    def __init__(self):
        #self.config = config
        self.logger = logging.getLogger('{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))


    @staticmethod
    def get_opta_squad_data(path_squads: str = None, num_of_squads=20) -> pd.DataFrame:
        """

        Returns:
            pd.DataFrame: [description]

        """

        # always assume to run from root
        if path_squads is None:
            path_squads = os.path.join("scripts_from_fed", "srml-8-2019-squads.xml")

        try:
            with open(path_squads, encoding = 'utf-8') as fd:
                opta_squads = xmltodict.parse(fd.read())
        except UnicodeDecodeError:
            with open(path_squads, encoding = 'latin-1') as fd:
                opta_squads = xmltodict.parse(fd.read())

        list_squads = []

        if num_of_squads is None:
            num_of_squads = len(opta_squads['SoccerFeed']['SoccerDocument']['Team'])

        for i in range(num_of_squads):
            # potentially, change from for loop to generator built the above load functionality
            #
            data_players_in = pd.DataFrame(opta_squads['SoccerFeed']['SoccerDocument']['Team'][i]['Player'])
            if '@loan' in data_players_in.columns:
                data_players_in = data_players_in.drop(['@loan'], axis = 1)
            if 'PlayerChanges' in list(opta_squads['SoccerFeed']['SoccerDocument'].keys()):
                try:
                    data_players_out = pd.DataFrame(opta_squads['SoccerFeed']['SoccerDocument']['PlayerChanges']['Team'][i]['Player'])
                    if '@loan' in data_players_out.columns:
                        data_players_out = data_players_out.drop(['@loan'], axis = 1)
                    data_players = pd.concat([data_players_in, data_players_out]).reset_index(drop=True)
                except:
                    data_players = data_players_in.reset_index(drop=True)
            else:
                data_players = data_players_in.reset_index(drop=True)
            data_players['team_id'] = opta_squads['SoccerFeed']['SoccerDocument']['Team'][i]['@uID']
            list_squads.append(data_players)

        data_squads = pd.concat(list_squads).reset_index(drop=True)
        data_squads = data_squads[pd.Series([type(x) for x in data_squads.Stat])==list].reset_index(drop=True)
        data_squads['preferred_foot'] = [pd.DataFrame(x)['#text'][pd.DataFrame(x)['@Type']=='preferred_foot'].iloc[0] if 'preferred_foot' in pd.DataFrame(x)['@Type'].tolist() else 'Not Available' for x in data_squads.Stat]
        data_squads = data_squads.drop('Stat', axis = 1).drop_duplicates(['@uID']).reset_index(drop=True)
        return data_squads

    def extract_set_piece_statistics(self, df_opta_output_final_freekicks: pd.DataFrame,
                                     df_opta_output_shots_freekicks: pd.DataFrame,
                                     df_opta_output_aerial_duels_freekicks: pd.DataFrame,
                                     df_opta_output_final_corners: pd.DataFrame,
                                     df_opta_output_shots_corners: pd.DataFrame,
                                     df_opta_output_aerial_duels_corners: pd.DataFrame,
                                     opta_match_info: dict) -> \
            (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """[summary]

        Args:
            df_opta_output_final_freekicks (pd.DataFrame): [description]
            df_opta_output_shots_freekicks (pd.DataFrame): [description]
            df_opta_output_aerial_duels_freekicks (pd.DataFrame): [description]
            df_opta_output_final_corners (pd.DataFrame): [description]
            df_opta_output_shots_corners (pd.DataFrame): [description]
            df_opta_output_aerial_duels_corners (pd.DataFrame): [description]

        Returns:
            pd.DataFrame: [description]
            pd.DataFrame: [description]
            pd.DataFrame: [description]

        """

        data_squads = self.get_opta_squad_data()

        list_of_dfs_shots = []
        list_of_dfs_shots.append(df_opta_output_shots_freekicks)
        list_of_dfs_shots.append(df_opta_output_shots_corners)

        list_of_dfs_aerial_duels = []
        list_of_dfs_aerial_duels.append(df_opta_output_aerial_duels_freekicks)
        list_of_dfs_aerial_duels.append(df_opta_output_aerial_duels_corners)


        if df_opta_output_final_corners is not None :
            final_df_freekicks = df_opta_output_final_freekicks
            final_df_corners = df_opta_output_final_corners
            final_df_shots = pd.concat(list_of_dfs_shots)
            final_df_aerial_duels = pd.concat(list_of_dfs_aerial_duels)

            final_df_freekicks['Frontal/Lateral End'] = np.where(final_df_freekicks['Frontal/Lateral End'] == 'End Shot', None, final_df_freekicks['Frontal/Lateral End'])

            final_df_freekicks = final_df_freekicks.rename(columns = {'Same Side':'Ending Side'})

            #final_df_freekicks_pivoted = pd.get_dummies(final_df_freekicks, prefix = '', prefix_sep='', columns=['Start Area Of Pitch', 'Frontal/Lateral Start', 'Frontal/Lateral End']).reset_index(drop = True)


            #final_df_freekicks = final_df_freekicks.merge(final_df_freekicks_pivoted, how = 'inner').sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)

            final_df_set_pieces = pd.concat([final_df_freekicks, final_df_corners], axis = 0, ignore_index = True,
                                            sort = False).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)


            final_df_set_pieces = final_df_set_pieces.merge(data_squads.drop(['Position'], axis =1), how = 'left', left_on = ['Attacking Team ID', 'Player ID'],
                                                            right_on = ['team_id', '@uID']).drop(['@uID', 'team_id', 'Name'], axis = 1).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)
            final_df_set_pieces = final_df_set_pieces.merge(data_squads.drop(['Position'], axis =1), how = 'left', left_on = ['Attacking Team ID', 'Relevant Player ID'],
                                                            right_on = ['team_id', '@uID'], suffixes = ('', '_relevant')).drop(['@uID', 'team_id', 'Name'], axis = 1).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)

            final_df_set_pieces['Actual Delivery Type'] = np.where(final_df_set_pieces['Set Piece Type'] == 'Corner',
                                                                   final_df_set_pieces['Actual Delivery Type'],
                                                                   np.where(final_df_set_pieces['Set Piece Type'].isin(['Free Kick Outside Target Zone', 'Free Kick Shot']), None,
                                                                            np.where(final_df_set_pieces['Side']=='No Side', None,
                                                                                     np.where(final_df_set_pieces['Side'] == final_df_set_pieces['preferred_foot_relevant'],
                                                                                              'Outswing', 'Inswing'))))
            final_df_set_pieces = final_df_set_pieces.rename(columns = {'preferred_foot': 'Preferred Foot',
                                                                        'preferred_foot_relevant': 'Relevant Preferred Foot'})

            final_df_shots = final_df_shots.merge(data_squads.drop(['Position'], axis =1), how = 'left', left_on = ['Shot Team ID', 'Shot Player ID'],
                                                  right_on = ['team_id', '@uID']).drop(['@uID', 'team_id', 'Name'], axis = 1).reset_index(drop=True)
            final_df_shots = final_df_shots.rename(columns = {'preferred_foot': 'Preferred Foot'})

            # Add fixtures and game_id Todo Check with Fed
            final_df_aerial_duels['game_id'] = opta_match_info['match_id']
            final_df_aerial_duels['Fixture'] = opta_match_info['fixture']

            return final_df_set_pieces, final_df_shots, final_df_aerial_duels

    def set_piece_classification(self, df_opta_events: pd.DataFrame, match_info: dict,
                                 opta_match_info: dict, df_opta_crosses: pd.DataFrame,
                                 df_opta_shots: pd.DataFrame,
                                 df_player_names_raw: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """[summary]

        Args:
            df_opta_events (pd.DataFrame): [description]
            match_info (dict): [description]
            opta_match_info (dict): [description]
            df_opta_crosses (pd.DataFrame): [description]
            df_opta_shots (pd.DataFrame): [description]
            df_player_names_raw (pd.DataFrame): [description]

        Returns:
            pd.DataFrame: [description]
            pd.DataFrame: [description]
            pd.DataFrame: [description]

        """

        opta_event_data_df = df_opta_events

        game_id = opta_match_info['match_id']
        away_team_id = opta_match_info['away_team_id']
        away_team_name = opta_match_info['away_team_name']
        home_team_id = opta_match_info['home_team_id']
        home_team_name = opta_match_info['home_team_name']
    
        player_names_raw = df_player_names_raw
    
        data_crosses = df_opta_crosses
        data_shots = df_opta_shots
    
        length_pitch = match_info['pitchLength']
        width_pitch = match_info['pitchWidth'] 
        
        # always assume running from root
        # this should also be data found in data/raw_data/
        event_type_id = pd.read_excel(os.path.join('scripts_from_fed', 'f24 - ID definitions.xlsx'))      
        
        player_names_raw['@uID'] = 'p' + player_names_raw['player_id'].astype(str)
        player_names_raw['player_name'] = player_names_raw['full_name']

        #here teh actual loop starts
        #summary_list = []

        opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']
        fixture = opta_event_data_df['fixture'].iloc[0]
        freekicks_taken = opta_event_data_df.loc[((opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.loc[opta_event_data_df.qualifier_id==5].unique().tolist())) | ((opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.loc[opta_event_data_df.qualifier_id==26].unique().tolist())) & (opta_event_data_df.type_id.isin([13,14,15,16])))) & (opta_event_data_df.period_id != 5) & (opta_event_data_df.type_id != 43)].reset_index(drop = True) #we include all free kicks instances
        freekicks_taken['time_in_seconds'] = freekicks_taken['min']*60.0 + freekicks_taken['sec']
        freekicks_taken['time_in_seconds'] = freekicks_taken['time_in_seconds'].astype(float)

        #further step - remove direct freekick shots that are indeed not direct. We might also do the other way round, i.e. retain the shots with qualifier 26 and remove any freekick taken instance occurring within 3 seconds before that shot
        if np.any((freekicks_taken.type_id.isin([13,14,15,16])) & (freekicks_taken.qualifier_id==26)):
            for ev_shot in freekicks_taken.unique_event_id.loc[(freekicks_taken.type_id.isin([13,14,15,16])) & (freekicks_taken.qualifier_id==26)].tolist():
                time_fk_shot = freekicks_taken[freekicks_taken.unique_event_id==ev_shot]['time_in_seconds'].iloc[0]
                period_fk_shot = freekicks_taken[freekicks_taken.unique_event_id==ev_shot]['period_id'].iloc[0]
                team_fk_shot = freekicks_taken[freekicks_taken.unique_event_id==ev_shot]['team_id'].iloc[0]
                window_of_interest = freekicks_taken[(freekicks_taken.period_id==period_fk_shot) & (freekicks_taken.team_id==team_fk_shot) & (freekicks_taken.time_in_seconds >= time_fk_shot - 4.0) & (freekicks_taken.time_in_seconds <= time_fk_shot) & (freekicks_taken.unique_event_id!=ev_shot)].drop_duplicates(['unique_event_id']).reset_index(drop=True)
                if window_of_interest.shape[0] > 0:
                    freekicks_taken = freekicks_taken[~freekicks_taken.unique_event_id.isin(window_of_interest.unique_event_id)].reset_index(drop=True)


        #| (opta_event_data_df.type_id.isin([1,2,13,14,15,16]))
        #crosses_df = opta_event_data_df.loc[(opta_event_data_df.type_id==1) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.loc[opta_event_data_df.qualifier_id==2].unique().tolist())) & (opta_event_data_df.period_id != 5)] #contains all crosses in a game with related qualifiers

        thresh_pass = 7.5
        thresh_dist_direct = 7.5
        thresh_dist_indirect = 10

        #list_events = []
        list_outside = []
        list_frontal_lateral = []
        list_shots = []
        list_all_shots = []
        list_aerial_duels = []
        for freekick_event_id in freekicks_taken.unique_event_id.unique():
            #type_of_freekick_copy = 'None'
            #freekick_event_id = 2169668585
            #freekick_event_id = 2168453871
            #freekick_event_id = 2175844837
            #freekick_event_id = 206710676
            #freekick_event_id = 956107712
            #freekick_event_id = 2173671927
            #freekick_event_id = 2170928251
            #freekick_event_id = 554521953
            if freekick_event_id == 64356062:
                continue

            #check for the freekick event taken 
            attacking_team_id = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, home_team_id, away_team_id).tolist()
            defending_team_id = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, away_team_id, home_team_id).tolist()
            attacking_team = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, home_team_name, away_team_name).tolist()
            defending_team = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, away_team_name, home_team_name).tolist()
            period_id = int(freekicks_taken['period_id'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0])
            freekick_mins = int(freekicks_taken['min'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0])
            freekick_secs = int(freekicks_taken['sec'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0]) 
            freekick_time_seconds = freekick_mins*60.0 + freekick_secs    
            attacking_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==attacking_team_id) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==defending_team_id) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < period_id) | ((opta_event_data_df.time_in_seconds < freekick_time_seconds) & (opta_event_data_df.period_id==period_id)))]['unique_event_id'].unique())    
            defending_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==defending_team_id) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < period_id) | ((opta_event_data_df.time_in_seconds < freekick_time_seconds) & (opta_event_data_df.period_id==period_id)))]['unique_event_id'].unique())    
            goal_diff_attack_v_defense = attacking_team_goals_up_to_freekick_excluded - defending_team_goals_up_to_freekick_excluded
            game_state = np.sign(goal_diff_attack_v_defense)
            freekick_event_id_event = int(freekicks_taken.event_id.loc[freekicks_taken.unique_event_id==freekick_event_id].unique()[0])
            freekick_type_id = int(freekicks_taken.type_id.loc[freekicks_taken.unique_event_id==freekick_event_id].unique()[0])
            freekick_player_id = 'p' + str(int(freekicks_taken.player_id.loc[freekicks_taken.unique_event_id==freekick_event_id].unique()[0]))
            freekick_player_name = player_names_raw.player_name.loc[player_names_raw['@uID'] == freekick_player_id].iloc[0]
            freekick_qualifier_ids = ', '.join([str(int(x)) for x in freekicks_taken.qualifier_id.loc[freekicks_taken.unique_event_id==freekick_event_id].tolist()])
            freekick_qualifier_values = ', '.join([str(x) for x in freekicks_taken['value'].loc[freekicks_taken.unique_event_id==freekick_event_id].tolist()])
            freekick_x_start = freekicks_taken['x'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0]
            freekick_y_start = freekicks_taken['y'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0]
            rolled = 0 #initialise 
            direct = 0
            #freekick_contains_cross_qualifier = int(2 in [int(x) for x in freekick_qualifier_ids.split(', ')])

            #we can keep track of all the freekicks won in the middle third, frontal final third and lateral final third up to (and included) the one in the event of interest.
            #this allows us to have a clearer picture of how often teams decide to play a freekick in a certain way from a certain area
            #freekicks_taken_middle_third = len(freekicks_taken[(freekicks_taken['x'] >= 100/3.0) & (freekicks_taken['x'] < 100/1.5) & ((freekicks_taken['time_in_seconds'] <= freekick_time_seconds) | (freekicks_taken['period_id'] < period_id)) & (freekicks_taken['team_id']==attacking_team_id)]['unique_event_id'].unique())
            #freekicks_taken_final_third_frontal_before_box = len(freekicks_taken[(freekicks_taken['x'] >= 100/1.5) & (freekicks_taken['x'] < 83.0) & (freekicks_taken['y'] >= 100/3.0) & (freekicks_taken['y'] <= 100/1.5) & ((freekicks_taken['time_in_seconds'] <= freekick_time_seconds) | (freekicks_taken['period_id'] < period_id)) & (freekicks_taken['team_id']==attacking_team_id)]['unique_event_id'].unique())
            #freekicks_taken_final_third_frontal_after_box = len(freekicks_taken[(freekicks_taken['x'] >= 100/1.5) & (freekicks_taken['x'] >= 83.0) & (freekicks_taken['y'] >= 100/3.0) & (freekicks_taken['y'] <= 100/1.5) & ((freekicks_taken['time_in_seconds'] <= freekick_time_seconds) | (freekicks_taken['period_id'] < period_id)) & (freekicks_taken['team_id']==attacking_team_id)]['unique_event_id'].unique())
            #freekicks_taken_final_third_lateral_before_box = len(freekicks_taken[(freekicks_taken['x'] >= 100/1.5) & (freekicks_taken['x'] < 83.0) & ((freekicks_taken['y'] < 100/3.0) | (freekicks_taken['y'] > 100/1.5)) & ((freekicks_taken['time_in_seconds'] <= freekick_time_seconds) | (freekicks_taken['period_id'] < period_id)) & (freekicks_taken['team_id']==attacking_team_id)]['unique_event_id'].unique())
            #freekicks_taken_final_third_lateral_after_box = len(freekicks_taken[(freekicks_taken['x'] >= 100/1.5) & (freekicks_taken['x'] >= 83.0) & ((freekicks_taken['y'] < 100/3.0) | (freekicks_taken['y'] > 100/1.5)) & ((freekicks_taken['time_in_seconds'] <= freekick_time_seconds) | (freekicks_taken['period_id'] < period_id)) & (freekicks_taken['team_id']==attacking_team_id)]['unique_event_id'].unique())

            #we might need to use 2 different criteria to identify the relevant pass depending on whether we are starting from an end zone or not.

            #get window of events happening within, say, 3 seconds since the freekick is taken, just to account free kicks which are not boomed straight away
            if freekick_type_id in [13,14,15,16]:
                if freekick_event_id == 1384166730:
                    time_window = opta_event_data_df[(opta_event_data_df['time_in_seconds'] >= freekick_time_seconds - 3.0) & (opta_event_data_df['time_in_seconds'] <= freekick_time_seconds) & (opta_event_data_df.period_id==period_id)].reset_index(drop = True)
                else:
                    time_window = opta_event_data_df[(opta_event_data_df['time_in_seconds'] >= freekick_time_seconds - 4.0) & (opta_event_data_df['time_in_seconds'] <= freekick_time_seconds) & (opta_event_data_df.period_id==period_id)].reset_index(drop = True)
            else:
                time_window = opta_event_data_df[(opta_event_data_df['time_in_seconds'] >= freekick_time_seconds) & (opta_event_data_df['time_in_seconds'] <= freekick_time_seconds + 4.0) & (opta_event_data_df.period_id==period_id)].reset_index(drop = True)
            time_window_passes = time_window[(time_window.type_id.isin([1,2, 13, 14, 15, 16])) & (time_window.team_id==attacking_team_id)].reset_index(drop=True) #includes both passes and shots
            passes_in_window = []
            for ev in time_window_passes.unique_event_id.unique():
                x_start = time_window_passes[time_window_passes.unique_event_id==ev]['x'].iloc[0]
                y_start = time_window_passes[time_window_passes.unique_event_id==ev]['y'].iloc[0]
                min_event = int(time_window_passes['min'].loc[time_window_passes.unique_event_id==ev].iloc[0])
                sec_event = int(time_window_passes['sec'].loc[time_window_passes.unique_event_id==ev].iloc[0])
                if time_window_passes[time_window_passes.unique_event_id==ev].type_id.iloc[0] in [1,2]:
                    x_end = float(time_window_passes[(time_window_passes.unique_event_id==ev) & (time_window_passes.qualifier_id==140)]['value'].iloc[0])
                    y_end = float(time_window_passes[(time_window_passes.unique_event_id==ev) & (time_window_passes.qualifier_id==141)]['value'].iloc[0])
                    chipped = int((155 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    launch = int((157 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    cross = int(ev in data_crosses.unique_event_id.tolist())
                    overhit_cross = int((345 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    switch_of_play = int((196 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    head_pass = int((3 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    flick_on = int((168 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    length_pass = np.round(np.sqrt((x_start/100.0*length_pitch - x_end/100.0*length_pitch)**2 + (y_start/100.0*width_pitch - y_end/100.0*width_pitch)**2), 2)
                    distance_x_percent = x_end - x_start #we keep the difference with the relevant sign to make sure we penalise passages going backwards
                    qual_26_or_24 = None
                else:
                    x_end = -1
                    y_end = -1
                    chipped = None
                    launch = None
                    cross = None
                    overhit_cross = None
                    switch_of_play = None
                    head_pass = None
                    flick_on = None
                    length_pass = -1
                    distance_x_percent = np.nan
                    qual_26_or_24 = np.where(26 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist(), 26, 
                        np.where(24 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist(), 24, None)).tolist()
                type_id_event = time_window_passes[time_window_passes.unique_event_id==ev].type_id.iloc[0]
                event_id_event = int(time_window_passes[time_window_passes.unique_event_id==ev].event_id.iloc[0])
                passes_in_window.append([x_start, y_start, x_end, y_end, chipped, type_id_event, distance_x_percent, length_pass, qual_26_or_24, ev, launch, cross, overhit_cross, switch_of_play, head_pass, flick_on, event_id_event, min_event, sec_event])
            passes_in_window = np.array(passes_in_window)

            if passes_in_window.shape[0] > 1:
                #as relevant pass we can use the first pass that targets an end zone, assuming it is longer than 5 meters
                if (freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5):
                    index_longest_pass = np.where((((passes_in_window[:,2] >= 100/1.5) & (passes_in_window[:,7] > thresh_pass)) | ((passes_in_window[:,10] == 1) & (passes_in_window[:,7] > thresh_pass)) | ((passes_in_window[:,4] == 1) & (passes_in_window[:,7] > thresh_pass)) | (passes_in_window[:,13]==1)) & (passes_in_window[:,14] != 1) & (passes_in_window[:,15] != 1))
                    if type(index_longest_pass) == tuple:
                        index_longest_pass = index_longest_pass[0]
                    if index_longest_pass.shape[0] > 0:
                        index_longest_pass = index_longest_pass[0]
                        if (((passes_in_window[index_longest_pass,2] >= 75.0) & (passes_in_window[index_longest_pass,7] > thresh_pass)) | ((passes_in_window[index_longest_pass,10] == 1) & (passes_in_window[index_longest_pass,7] > thresh_pass)) | ((passes_in_window[index_longest_pass,4] == 1) & (passes_in_window[index_longest_pass,7] > thresh_pass)) | (passes_in_window[index_longest_pass,13]==1)) & (passes_in_window[index_longest_pass,14] != 1) & (passes_in_window[index_longest_pass,15] != 1):
                            index_longest_pass = np.array([index_longest_pass])
                        else:
                            index_longest_pass = np.array([])

                    #index_longest_pass = np.where(((passes_in_window[:,2] >= 75.0) & (passes_in_window[:,7] > 5.0)) | ((passes_in_window[:,10] == 1) & (passes_in_window[:,7] > 5.0)))
                elif (freekick_x_start >= 100/1.5):
                    if (freekick_y_start < 100/3.0) | (freekick_y_start > 100/1.5):
                        index_longest_pass = np.where((((passes_in_window[:,2] >= 75) & (passes_in_window[:,3] >= 21.1) & (passes_in_window[:,3] <= 78.9)) | ((passes_in_window[:,2] >= 79.0) & ((passes_in_window[:,3] < 21.1) | (passes_in_window[:,3] > 78.9)))) & (passes_in_window[:,7] > thresh_pass) & (passes_in_window[:,14] != 1) & (passes_in_window[:,15] != 1))
                    else:
                        index_longest_pass = np.where((((passes_in_window[:,2] >= 79) & (passes_in_window[:,3] >= 21.1) & (passes_in_window[:,3] <= 78.9)) | ((passes_in_window[:,2] >= 75.0) & ((passes_in_window[:,3] < 21.1) | (passes_in_window[:,3] > 78.9)))) & (passes_in_window[:,7] > thresh_pass) & (passes_in_window[:,14] != 1) & (passes_in_window[:,15] != 1))
                else:
                    continue
                #index_longest_pass = np.where(passes_in_window[:,6] == max(passes_in_window[:,6]))[0] #here by longest we mean longest along the x axis
                if type(index_longest_pass) == tuple:
                    index_longest_pass = index_longest_pass[0]
                #if type(index_longest_pass) == int:
                #    index_longest_pass = np.array(index_longest_pass)
                #if index_longest_pass is None:
                #    index_longest_pass = np.array([])

                if index_longest_pass.shape[0] == 0: #if this occurs, it means there are no (qualifying) passes in the window, hence there can be only shots or non relevant passes
                    index_longest_pass = 0
                    x_start_relevant = -1
                    y_start_relevant = -1
                    x_end_relevant = -1
                    y_end_relevant = -1
                    chipped_relevant = None
                    launch_relevant = None
                    cross_relevant = None
                    overhit_cross_relevant = None
                    switch_of_play_relevant = None
                    freekick_player_id_relevant = None
                    freekick_player_name_relevant = None
                else:
                    index_longest_pass = index_longest_pass[0]
                    x_start_relevant = passes_in_window[index_longest_pass,0]
                    y_start_relevant = passes_in_window[index_longest_pass,1]
                    x_end_relevant = passes_in_window[index_longest_pass,2]
                    y_end_relevant = passes_in_window[index_longest_pass,3]
                    #if index_longest_pass != 0:
                    chipped_relevant = passes_in_window[index_longest_pass,4]
                    launch_relevant = passes_in_window[index_longest_pass,10]
                    cross_relevant = passes_in_window[index_longest_pass,11]
                    overhit_cross_relevant = passes_in_window[index_longest_pass,12]
                    switch_of_play_relevant = passes_in_window[index_longest_pass,13]
                    freekick_player_id_relevant = 'p' + str(int(time_window_passes.player_id.loc[time_window_passes.unique_event_id==passes_in_window[index_longest_pass,9]].unique()[0]))
                    freekick_player_name_relevant = player_names_raw.player_name.loc[player_names_raw['@uID'] == freekick_player_id_relevant].iloc[0]
                    # else:
                    #     chipped_or_launch_relevant = None
                    #     freekick_player_id_relevant = None
                    #     freekick_player_name_relevant = None

                #we consider only direct freekick shots, hence we need to make sure that they occur straight away or after rolls
                is_shot = (passes_in_window[:,5] == 13) | (passes_in_window[:,5] == 14) | (passes_in_window[:,5] == 15) | (passes_in_window[:,5] == 16)
                first_shot_index = np.where(is_shot)[0]
                if first_shot_index.shape[0] > 0:
                    if first_shot_index[0] > 0:
                        if np.any((passes_in_window[:first_shot_index[0],5]==1) & (passes_in_window[:first_shot_index[0],7] > thresh_pass)):
                            is_shot = []
                if sum(is_shot)==0:
                    x_start_first_shot = None
                    y_start_first_shot = None
                    freekick_player_id_first_shot = None
                    freekick_player_name_first_shot = None
                else:
                    # if is_shot[0]:
                    #     x_start_first_shot = None
                    #     y_start_first_shot = None
                    #     freekick_player_id_first_shot = None
                    #     freekick_player_name_first_shot = None
                    # else:            
                    x_start_first_shot = passes_in_window[np.where(is_shot)[0][0], 0]
                    y_start_first_shot = passes_in_window[np.where(is_shot)[0][0], 1]
                    freekick_player_id_first_shot = 'p' + str(int(time_window_passes.player_id.loc[time_window_passes.unique_event_id==passes_in_window[np.where(is_shot)[0][0],9]].unique()[0]))
                    freekick_player_name_first_shot = player_names_raw.player_name.loc[player_names_raw['@uID'] == freekick_player_id_first_shot].iloc[0]            

                if (passes_in_window[0,5] in [13,14,15,16]): #if the first event of the window is a shot then it is direct straight away
                    direct = 1
                if (passes_in_window[0,5] in [1,2]) & (index_longest_pass==0): #if the first event of the window is the relevant pass
                    direct = 1
                #if (passes_in_window[0,5] in [13,14,15,16]) & (index_longest_pass > 0):
                #    direct = 0
                #if (passes_in_window[0,5] in [1,2]) & (index_longest_pass > 0):
                #    direct = 0
                start_pass_chipped = passes_in_window[0,4]
                start_pass_launch = passes_in_window[0,10]
                start_pass_cross = passes_in_window[0,11]
                start_pass_overhit_cross = passes_in_window[0,12]
                start_pass_switch_of_play = passes_in_window[0,13]
                #relevant_pass_chipped_or_launch = np.where((passes_in_window[-1,0] >= 75.0) & (passes_in_window[-1,4]==1), 1, 0).tolist()
                if (passes_in_window[0,5] in [1,2]) & (passes_in_window[0,7] > thresh_pass):
                    distance_x_percent_start_pass = passes_in_window[0,6]
                    length_start_pass = passes_in_window[0,7]
                else:
                    distance_x_percent_start_pass = np.nan
                    length_start_pass = -1

                if (passes_in_window[index_longest_pass,5] in [1,2]) & (passes_in_window[index_longest_pass,7] > thresh_pass):
                    distance_x_percent_relevant_pass = passes_in_window[index_longest_pass,6]
                    length_relevant_pass = passes_in_window[index_longest_pass,7]
                else:
                    distance_x_percent_relevant_pass = np.nan
                    length_relevant_pass = -1

                #this condition ensures us that all windows not containing any shot with qualifier 26 have not a shot as first event
                if np.any(((passes_in_window[:,5] == 13) | (passes_in_window[:,5] == 14) | (passes_in_window[:,5] == 15) | (passes_in_window[:,5] == 16)) & (passes_in_window[:,8] == 26)):
                    type_of_freekick = 'FreeKick Shot'
                    direct = 1
                    rolled = 1 #since we are under the top condition of more than 1 event in the window with a direct freekick shot, the former events can just be rolls

                else:
                    if passes_in_window[index_longest_pass,5] in [1,2]:
                        if (x_end_relevant > -1):
                            type_of_freekick = np.where(freekick_x_start < 100/3.0, 'None', 
                                np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end_relevant >= 75.0) & (y_end_relevant <= 78.9) & (y_end_relevant >= 21.1), 'Middle Third Ending Frontal', 
                                    np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end_relevant >= 75.0) & ((y_end_relevant > 78.9) | (y_end_relevant < 21.1)), 'Middle Third Ending Lateral', 
                                        np.where((freekick_x_start >= 100/1.5) & (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5) & (x_end_relevant >= 75.0) & ((y_end_relevant > 78.9) | (y_end_relevant < 21.1)), 'Final Third Starting Frontal Ending Lateral', 
                                            np.where((freekick_x_start >= 100/1.5) & (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5) & (x_end_relevant >= 79.0) & ((y_end_relevant <= 78.9) & (y_end_relevant >= 21.1)), 'Final Third Starting Frontal Ending Frontal', 
                                                np.where((freekick_x_start >= 100/1.5) & ((freekick_y_start < 100/3.0) | (freekick_y_start > 100/1.5)) & (x_end_relevant >= 79.0) & ((y_end_relevant > 78.9) | (y_end_relevant < 21.1)), 'Final Third Starting Lateral Ending Lateral', 
                                                    np.where((freekick_x_start >= 100/1.5) & ((freekick_y_start < 100/3.0) | (freekick_y_start > 100/1.5)) & (x_end_relevant >= 75.0) & ((y_end_relevant <= 78.9) & (y_end_relevant >= 21.1)), 'Final Third Starting Lateral Ending Frontal', 'None'))))))).tolist()

                            type_of_freekick = np.where((type_of_freekick == 'Final Third Starting Lateral Ending Lateral') & (np.sign(freekick_y_start - 50.0) == np.sign(y_end_relevant - 50.0)), 
                                'Final Third Starting Lateral Ending Lateral Same Side', 
                                np.where((type_of_freekick == 'Final Third Starting Lateral Ending Lateral') & (np.sign(freekick_y_start - 50.0) != np.sign(y_end_relevant - 50.0)), 
                                'Final Third Starting Lateral Ending Lateral Opposite Side', type_of_freekick)).tolist()

                            #apply some modification when there are crosses as relevant passes that end up on the opposite side - they are actually played frontal but missed by everyone so ending up wide
                            if type_of_freekick == 'Final Third Starting Lateral Ending Lateral Opposite Side':
                                if (passes_in_window[index_longest_pass,11]==1) | (passes_in_window[index_longest_pass,12]==1):
                                    type_of_freekick = 'Final Third Starting Lateral Ending Frontal'

                            if type_of_freekick == 'Middle Third Ending Lateral':
                                if (passes_in_window[index_longest_pass,11]==1) | (passes_in_window[index_longest_pass,12]==1):
                                    type_of_freekick = 'Middle Third Ending Frontal'   

                            if (passes_in_window[index_longest_pass,11]==1) | (passes_in_window[index_longest_pass,12]==1):
                                if (freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5):
                                    type_of_freekick = 'Middle Third Ending Frontal'
                                else:
                                    if (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5):
                                        type_of_freekick = 'Final Third Starting Frontal Ending Frontal'      
                                    else:
                                        type_of_freekick = 'Final Third Starting Lateral Ending Frontal'          


                            # type_of_freekick = np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end_relevant > 100/1.5) & (y_end_relevant <= 78.9) & (y_end_relevant >= 21.1) & (launch_relevant==1), 
                            #     'Middle Third Ending Frontal', np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end_relevant > 100/1.5) & ((y_end_relevant < 21.1) | (y_end_relevant > 78.9)) & (launch_relevant==1), 
                            #         'Middle Third Ending Lateral', 'None')).tolist()

                            #type_of_freekick_copy = type_of_freekick
                            #set direct/indirect for passes
                            if index_longest_pass==0:
                                direct = 1
                            else:
                                if np.any(passes_in_window[:index_longest_pass,7] > thresh_pass):
                                    if sum(passes_in_window[:index_longest_pass,7] > thresh_pass) == 1:
                                        direct = 0
                                        if np.sqrt(((passes_in_window[index_longest_pass-1,2] - passes_in_window[index_longest_pass,0])/100.0*length_pitch)**2 + ((passes_in_window[index_longest_pass-1,3] - passes_in_window[index_longest_pass,1])/100.0*width_pitch)**2) > thresh_dist_indirect + 2*int(freekick_x_start >= 100/1.5):
                                            type_of_freekick = 'None'

                                    else:
                                        direct = None
                                        type_of_freekick = 'None'
                                else:
                                    rolled = 1
                                    if np.sqrt(((passes_in_window[index_longest_pass-1,2] - passes_in_window[index_longest_pass,0])/100.0*length_pitch)**2 + ((passes_in_window[index_longest_pass-1,3] - passes_in_window[index_longest_pass,1])/100.0*width_pitch)**2) <= thresh_dist_direct:
                                        direct = 1
                                    elif np.sqrt(((passes_in_window[index_longest_pass-1,2] - passes_in_window[index_longest_pass,0])/100.0*length_pitch)**2 + ((passes_in_window[index_longest_pass-1,3] - passes_in_window[index_longest_pass,1])/100.0*width_pitch)**2) > thresh_dist_indirect + 2*int(freekick_x_start >= 100/1.5):
                                        type_of_freekick = 'None'
                                    else:
                                        direct = 0



                        #here we check for windows with very short passes and without shots with qualifier 26
                        else:
                            if np.any(((passes_in_window[:,5] == 13) | (passes_in_window[:,5] == 14) | (passes_in_window[:,5] == 15) | (passes_in_window[:,5] == 16)) & (passes_in_window[:,8] == 24)):
                                #we need to check for whether there are little rolls (i.e. direct) or proper passes (i.e. indirect) before the freekick shot
                                index_first_shot_with_24 = np.where(passes_in_window[:,9] == passes_in_window[((passes_in_window[:,5] == 13) | (passes_in_window[:,5] == 14) | (passes_in_window[:,5] == 15) | (passes_in_window[:,5] == 16)) & (passes_in_window[:,8] == 24),9][0])[0][0]
                                if np.any(passes_in_window[:index_first_shot_with_24,7] > thresh_pass):
                                    type_of_freekick = 'None'
                                    #direct = 0
                                else:
                                    type_of_freekick = 'FreeKick Shot'
                                    rolled = 1
                                    direct = 1
                                #direct = 0
                            else:
                                type_of_freekick = 'None'
                    else:
                        type_of_freekick = 'FreeKick Shot'
                        direct = 1
                
                # if index_longest_pass == 0:
                #     x_start_relevant = None
                #     y_start_relevant = None
                #     x_end_relevant = None
                #     y_end_relevant = None
                #     length_relevant_pass = np.nan

            else:
                if freekick_type_id in [1,2]:
                    if (freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5):
                        index_longest_pass = np.where(((passes_in_window[0,2] >= 75.0) & (passes_in_window[0,7] > thresh_pass)) | ((passes_in_window[0,10] == 1) & (passes_in_window[0,7] > thresh_pass)), 
                            0, np.nan).tolist()
                    elif (freekick_x_start >= 100/1.5):
                        if (freekick_y_start < 100/3.0) | (freekick_y_start > 100/1.5):
                            index_longest_pass = np.where((((passes_in_window[0,2] >= 75.0) & (passes_in_window[0,3] >= 21.1) & (passes_in_window[0,3] <= 78.9)) | ((passes_in_window[0,2] >= 79.0) & ((passes_in_window[0,3] < 21.1) | (passes_in_window[0,3] > 78.9)))) & (passes_in_window[0,7] > thresh_pass), 0, np.nan).tolist()
                        else:
                            index_longest_pass = np.where((((passes_in_window[0,2] >= 79.0) & (passes_in_window[0,3] >= 21.1) & (passes_in_window[0,3] <= 78.9)) | ((passes_in_window[0,2] >= 75.0) & ((passes_in_window[0,3] < 21.1) | (passes_in_window[0,3] > 78.9)))) & (passes_in_window[0,7] > thresh_pass), 0, np.nan).tolist()                        
                    else:
                        continue
                else:
                    index_longest_pass = np.nan

                if ~np.isnan(index_longest_pass):
                    index_longest_pass = int(index_longest_pass)
                #index_longest_pass = 0
                x_start_relevant = np.where(np.isnan(index_longest_pass), -1, freekick_x_start).tolist()
                y_start_relevant = np.where(np.isnan(index_longest_pass), -1, freekick_y_start).tolist()
                x_end_relevant = np.where(np.isnan(index_longest_pass), -1, passes_in_window[0,2]).tolist()
                y_end_relevant = np.where(np.isnan(index_longest_pass), -1, passes_in_window[0,3]).tolist()
                chipped_relevant = np.where(np.isnan(index_longest_pass), None, passes_in_window[0,4]).tolist()
                launch_relevant = np.where(np.isnan(index_longest_pass), None, passes_in_window[0,10]).tolist()
                cross_relevant = np.where(np.isnan(index_longest_pass), None, passes_in_window[0,11]).tolist()
                overhit_cross_relevant = np.where(np.isnan(index_longest_pass), None, passes_in_window[0,12]).tolist()
                switch_of_play_relevant = np.where(np.isnan(index_longest_pass), None, passes_in_window[0,13]).tolist()
                freekick_player_id_relevant = np.where(np.isnan(index_longest_pass), None, freekick_player_id).tolist()
                freekick_player_name_relevant = np.where(np.isnan(index_longest_pass), None, freekick_player_name).tolist()
                direct = 1
                start_pass_chipped = passes_in_window[0,4]
                start_pass_launch = passes_in_window[0,10]
                start_pass_cross = passes_in_window[0,11]
                start_pass_overhit_cross = passes_in_window[0,12]
                start_pass_switch_of_play = passes_in_window[0,13]
                is_shot = passes_in_window[0,5] in [13,14,15,16]
                if is_shot==0:
                    x_start_first_shot = None
                    y_start_first_shot = None
                    freekick_player_id_first_shot = None
                    freekick_player_name_first_shot = None
                else:
                    # if is_shot[0]:
                    #     x_start_first_shot = None
                    #     y_start_first_shot = None
                    #     freekick_player_id_first_shot = None
                    #     freekick_player_name_first_shot = None
                    # else:            
                    x_start_first_shot = freekick_x_start
                    y_start_first_shot = freekick_y_start
                    freekick_player_id_first_shot = freekick_player_id
                    freekick_player_name_first_shot = freekick_player_name
                #x_start_first_shot = None
                #y_start_first_shot = None
                #freekick_player_id_first_shot = None
                #freekick_player_name_first_shot = None
                #relevant_pass_chipped_or_launch = None
                if passes_in_window[0,5] in [1,2]:
                    distance_x_percent_start_pass = x_end - x_start
                    length_start_pass = np.round(np.sqrt((x_start/100.0*length_pitch - x_end/100.0*length_pitch)**2 + (y_start/100.0*width_pitch - y_end/100.0*width_pitch)**2),2)
                    distance_x_percent_relevant_pass = np.where(np.isnan(index_longest_pass), np.nan, distance_x_percent_start_pass).tolist()
                    length_relevant_pass = np.where(np.isnan(index_longest_pass), -1, length_start_pass).tolist()
                else:
                    distance_x_percent_start_pass = np.nan
                    length_start_pass = -1
                    distance_x_percent_relevant_pass = np.nan
                    length_relevant_pass = -1
                #length_relevant_pass = None

                

                if passes_in_window[0,5] in [1,2]:
                    if x_end_relevant > -1:
                        type_of_freekick = np.where(freekick_x_start < 100/3.0, 'None', 
                            np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end >= 75.0) & (y_end <= 78.9) & (y_end >= 21.1), 'Middle Third Ending Frontal', 
                                np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end >= 75.0) & ((y_end > 78.9) | (y_end < 21.1)), 'Middle Third Ending Lateral', 
                                    np.where((freekick_x_start >= 100/1.5) & (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5) & (x_end >= 75.0) & ((y_end > 78.9) | (y_end < 21.1)), 'Final Third Starting Frontal Ending Lateral', 
                                        np.where((freekick_x_start >= 100/1.5) & (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5) & (x_end >= 79.0) & ((y_end <= 78.9) & (y_end >= 21.1)), 'Final Third Starting Frontal Ending Frontal', 
                                            np.where((freekick_x_start >= 100/1.5) & ((freekick_y_start < 100/3.0) | (freekick_y_start > 100/1.5)) & (x_end >= 79.0) & ((y_end > 78.9) | (y_end < 21.1)), 'Final Third Starting Lateral Ending Lateral', 
                                                np.where((freekick_x_start >= 100/1.5) & ((freekick_y_start < 100/3.0) | (freekick_y_start > 100/1.5)) & (x_end >= 75.0) & ((y_end <= 78.9) & (y_end >= 21.1)), 'Final Third Starting Lateral Ending Frontal', 'None'))))))).tolist()
                            
                        type_of_freekick = np.where((type_of_freekick == 'Final Third Starting Lateral Ending Lateral') & (np.sign(freekick_y_start - 50.0) == np.sign(y_end - 50.0)), 
                            'Final Third Starting Lateral Ending Lateral Same Side', 
                            np.where((type_of_freekick == 'Final Third Starting Lateral Ending Lateral') & (np.sign(freekick_y_start - 50.0) != np.sign(y_end - 50.0)), 
                            'Final Third Starting Lateral Ending Lateral Opposite Side', type_of_freekick)).tolist()

                        if type_of_freekick == 'Final Third Starting Lateral Ending Lateral Opposite Side':
                            if (passes_in_window[0,11]==1) | (passes_in_window[0,12]==1):
                                type_of_freekick = 'Final Third Starting Lateral Ending Frontal'

                        if type_of_freekick == 'Middle Third Ending Lateral':
                            if (passes_in_window[0,11]==1) | (passes_in_window[0,12]==1):
                                type_of_freekick = 'Middle Third Ending Frontal'  

                        if (passes_in_window[0,11]==1) | (passes_in_window[0,12]==1):
                            if (freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5):
                                type_of_freekick = 'Middle Third Ending Frontal'
                            else:
                                if (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5):
                                    type_of_freekick = 'Final Third Starting Frontal Ending Frontal'      
                                else:
                                    type_of_freekick = 'Final Third Starting Lateral Ending Frontal'

                        # type_of_freekick = np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end_relevant > 100/1.5) & (y_end_relevant <= 78.9) & (y_end_relevant >= 21.1) & (launch_relevant==1), 
                        #     'Middle Third Ending Frontal', np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5) & (x_end_relevant > 100/1.5) & ((y_end_relevant < 21.1) | (y_end_relevant > 78.9)) & (launch_relevant==1), 
                        #         'Middle Third Ending Lateral', 'None')).tolist()

                    else:
                        type_of_freekick = 'None'
                else:
                    type_of_freekick = 'FreeKick Shot'

                is_shot = (passes_in_window[:,5] == 13) | (passes_in_window[:,5] == 14) | (passes_in_window[:,5] == 15) | (passes_in_window[:,5] == 16)
            #now we have to apply the set of rules to the freekick
            
            number_events_in_window = passes_in_window.shape[0]

            #here we want to get event information for the longest pass in the window (if available) and the first shot in the window (if available)

            if x_start_relevant > -1: #if we have at least 2 events and at least one pass and the longest pass is not the first event
                event_id_relevant = passes_in_window[index_longest_pass,9]
                event_id_event_relevant = passes_in_window[index_longest_pass,-1]
                type_id_relevant_event = passes_in_window[index_longest_pass,5]
                relevant_qualifier_ids = ', '.join([str(int(x)) for x in time_window_passes.qualifier_id.loc[time_window_passes.unique_event_id==event_id_relevant].tolist()])
                relevant_qualifier_values = ', '.join([str(x) for x in time_window_passes['value'].loc[time_window_passes.unique_event_id==event_id_relevant].tolist()])
            else:
                event_id_relevant = None
                event_id_event_relevant = None
                type_id_relevant_event = None
                relevant_qualifier_ids = None
                relevant_qualifier_values = None



            if type_of_freekick == 'FreeKick Shot': #if we have at least 2 events and at least one shot and the first event is not a shot
                event_id_first_shot = passes_in_window[np.where(is_shot)[0][0],9]
                event_id_event_first_shot = passes_in_window[np.where(is_shot)[0][0],-3]
                type_id_first_shot = passes_in_window[np.where(is_shot)[0][0],5]
                first_shot_qualifier_ids = ', '.join([str(int(x)) for x in time_window_passes.qualifier_id.loc[time_window_passes.unique_event_id==event_id_first_shot].tolist()])
                first_shot_qualifier_values = ', '.join([str(x) for x in time_window_passes['value'].loc[time_window_passes.unique_event_id==event_id_first_shot].tolist()])
                min_shot = passes_in_window[np.where(is_shot)[0][0], -2]
                sec_shot = passes_in_window[np.where(is_shot)[0][0], -1]
                x_start_shot = passes_in_window[np.where(is_shot)[0][0], 0]
                y_start_shot = passes_in_window[np.where(is_shot)[0][0], 1]
            else:
                event_id_first_shot = None
                event_id_event_first_shot = None
                type_id_first_shot = None
                first_shot_qualifier_ids = None
                first_shot_qualifier_values = None
                min_shot = None 
                sec_shot = None
                x_start_shot = None 
                y_start_shot = None 

            #we add an open play indicator to say whether the free kick is kicked within 5 secs after a foul/offside
            foul_window = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.time_in_seconds >= freekick_time_seconds - 5.0) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)]
            if (np.any(foul_window.type_id==4)) | (np.any(foul_window.type_id==55)):
                open_play_indicator = 1
            else:
                open_play_indicator = 0
            #we apply the same logic to get the actual time between offside/foul awarded and the freekick taken. We also report event type and opta id of any event between the two
            #we need to get the time of the closest foul/offside awarded
            data_foul_off_time = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id.isin([55,4])) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)]['time_in_seconds'].iloc[-1] #we take most recent event before freekick
            if (np.any(opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds==data_foul_off_time)]['type_id']==4)):
                after_foul = 1
            else:
                after_foul = 0
            time_between_foul_and_freekick = np.round(freekick_time_seconds - data_foul_off_time, 2)
            data_foul_off = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds >= data_foul_off_time) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)].reset_index(drop=True)
            if len(data_foul_off.unique_event_id.unique()) == 1:
                number_events_in_between = 0
                opta_event_id_between = None
                opta_type_id_between = None
            else:
                data_foul_off_reduced = data_foul_off[(~data_foul_off.type_id.isin([55,4,2,43,70]))].reset_index(drop=True)
                if data_foul_off_reduced.shape[0] == 0:
                    number_events_in_between = 0
                    opta_event_id_between = None
                    opta_type_id_between = None   
                else:
                    number_events_in_between = len(data_foul_off_reduced.unique_event_id.unique())
                    opta_event_id_between = ', '.join([str(x) for x in data_foul_off_reduced.unique_event_id.unique()])
                    opta_type_id_between = ', '.join([str(int(x)) for x in data_foul_off_reduced.drop_duplicates(['unique_event_id']).type_id])             

            if type_of_freekick == 'FreeKick Shot':
                type_of_freekick = np.where((freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5), 'Middle Third FreeKick Shot', 
                    np.where((freekick_x_start >= 100/1.5) & (freekick_y_start >= 21.1) & (freekick_y_start <= 78.9), 'Final Third Starting Frontal Freekick Shot', 
                        np.where((freekick_x_start >= 100/1.5) & ((freekick_y_start < 21.1) | (freekick_y_start > 78.9)), 'Final Third Starting Lateral Freekick Shot', 'None'))).tolist()

            #before finalising, we want to split the type of freekick description into separate fields 
            if type_of_freekick != 'None':
                area_of_pitch = ' '.join(type_of_freekick.split(' ')[:2])
                frontal_lateral_start = np.where(area_of_pitch == 'Final Third', ' '.join(['Start', type_of_freekick.split(' ')[3]]), 'Start Frontal').tolist()
                frontal_lateral_end = np.where('Freekick Shot' in type_of_freekick, 'End Shot', 
                    np.where('Side' not in type_of_freekick, ' '.join(['End', type_of_freekick.split(' ')[-1]]), 
                        'End Lateral')).tolist()
                frontal_lateral_end = np.where(frontal_lateral_end == 'End Frontal', 'End Central', frontal_lateral_end).tolist()
                frontal_lateral_end = np.where(frontal_lateral_end == 'End Lateral', 'End Wide', frontal_lateral_end).tolist()
                side = np.where('Side' in type_of_freekick, ' '.join(type_of_freekick.split(' ')[-2:]), 'Side Irrelevant').tolist()

            else:
                if (freekick_x_start >= 100/3.0) & (freekick_x_start < 100/1.5):
                    area_of_pitch = 'Middle Third'
                    frontal_lateral_start = 'Start Frontal'
                    frontal_lateral_end = 'End Outside A Target Zone'
                    side = 'Side Irrelevant'
                elif freekick_x_start >= 100/1.5:
                    area_of_pitch = 'Final Third'
                    frontal_lateral_end = 'End Outside A Target Zone'
                    side = 'Side Irrelevant'
                    if (freekick_y_start >= 100/3.0) & (freekick_y_start <= 100/1.5):
                        frontal_lateral_start = 'Start Frontal'
                    else:
                        frontal_lateral_start = 'Start Lateral'
                else:
                    area_of_pitch = None
                    frontal_lateral_start = None
                    frontal_lateral_end = None
                    side = None

            #add additional indicator of whether the freekick starts before or after the box
            after_box = int(freekick_x_start >= 83.0)

            #output also the index of the relevant pass as well as combined coordinates to retain the full sequence. Shots are not included in the codification.
            index_relevant_pass = np.where((freekick_type_id in [13,14,15,16]) | (x_end_relevant==-1), np.nan, index_longest_pass).tolist()

            if ~np.isnan(index_relevant_pass):
                x_starts = ', '.join([str(x) for x in list(passes_in_window[:(int(index_relevant_pass)+1),0])])
                y_starts = ', '.join([str(x) for x in list(passes_in_window[:(int(index_relevant_pass)+1),1])])
                x_ends = ', '.join([str(x) for x in list(passes_in_window[:(int(index_relevant_pass)+1),2])])
                y_ends = ', '.join([str(x) for x in list(passes_in_window[:(int(index_relevant_pass)+1),3])])
                player_ids_pass_sequence = ', '.join(['p' + str(int(time_window_passes.player_id.loc[time_window_passes.unique_event_id==passes_in_window[i,9]].iloc[0])) for i in range(0, int(index_relevant_pass)+1)])
                player_names_pass_sequence = ', '.join([player_names_raw.player_name.loc[player_names_raw['@uID'] == p].iloc[0] for p in player_ids_pass_sequence.split(', ')])
            else:
                x_starts = None
                y_starts = None
                x_ends = None
                y_ends = None
                player_ids_pass_sequence = None
                player_names_pass_sequence = None

            if frontal_lateral_start == 'Start Frontal':
                start_side = 'No Side'
            else:
                if freekick_y_start < 50.0:
                    start_side = 'Right'
                else:
                    start_side = 'Left'


            if np.isnan(index_longest_pass):
                index_longest_pass = 0


            #incorporate the shots logic for each set piece event 
            if 1==1:
                aerial_duel_is_shot = 0
                if frontal_lateral_end != 'End Shot':
                    qualifying_shots = data_shots[((data_shots.Time_in_Seconds <= 
                        10 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds >= 
                        passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.period_id==
                        period_id) & (data_shots.related_event_team_id==attacking_team_id)) | (((data_shots.Time_in_Seconds >= 
                        passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds <= 
                        60 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.period_id==
                        period_id) & (data_shots.related_event_team_id==attacking_team_id) & (data_shots.from_set_piece==1)) | (((data_shots.value == 
                        passes_in_window[index_longest_pass,-3]) | (data_shots.value == 
                        passes_in_window[0,-3])) & (data_shots.related_event_team_id == 
                        attacking_team_id) & (data_shots.period_id==period_id) & (data_shots.Time_in_Seconds >= 
                        passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds <= 
                        60 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1])))].sort_values(['Time_in_Seconds']).reset_index(drop=True)
                    if frontal_lateral_start == 'Start Frontal':
                        qualifying_shots = qualifying_shots[qualifying_shots.from_set_piece==1].reset_index(drop=True)
                else:
                    qualifying_shots = data_shots[(data_shots.from_set_piece==1) & (data_shots.unique_event_id != event_id_first_shot) & (((data_shots.Time_in_Seconds <= 
                        10 + min_shot*60 + sec_shot) & (data_shots.Time_in_Seconds >= 
                        min_shot*60 + sec_shot) & (data_shots.period_id==
                        period_id) & (data_shots.related_event_team_id==attacking_team_id)) | ((data_shots.from_set_piece==1) & (((data_shots.Time_in_Seconds >= 
                        min_shot*60 + sec_shot) & (data_shots.Time_in_Seconds <= 
                        60 + min_shot*60 + sec_shot) & (data_shots.period_id==
                        period_id) & (data_shots.related_event_team_id==attacking_team_id)))))].sort_values(['Time_in_Seconds']).reset_index(drop=True)                
                # if qualifying_shots.shape[0] == 0:
                #     shot_event_ids = None
                #     shot_player_ids = None
                #     shot_player_names = None
                #     shot_label = 'No Shot' 
                if qualifying_shots.shape[0] > 0:
                    for id_shot in qualifying_shots.unique_event_id.unique():
                        if freekicks_taken[(freekicks_taken.period_id==
                            qualifying_shots[qualifying_shots.unique_event_id==
                            id_shot]['period_id'].iloc[0]) & (freekicks_taken.time_in_seconds <= 
                            qualifying_shots[qualifying_shots.unique_event_id==
                            id_shot]['Time_in_Seconds'].iloc[0]) & (freekicks_taken.team_id==
                            qualifying_shots[qualifying_shots.unique_event_id==id_shot]['related_event_team_id'].iloc[0])]['unique_event_id'].iloc[-1] != freekick_event_id:

                            qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id != id_shot].reset_index(drop=True)

                        else:
                            if opta_event_data_df[(opta_event_data_df.period_id==
                                qualifying_shots[qualifying_shots.unique_event_id==
                                id_shot]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                                id_shot]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                                qualifying_shots[qualifying_shots.unique_event_id==id_shot].related_event_team_id.iloc[0]) & (opta_event_data_df.period_id != 
                                5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds']).shape[0] > 0:

                                if opta_event_data_df[(opta_event_data_df.period_id==
                                    qualifying_shots[qualifying_shots.unique_event_id==
                                    id_shot]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                                    id_shot]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                                    qualifying_shots[qualifying_shots.unique_event_id==id_shot].related_event_team_id.iloc[0]) & (opta_event_data_df.period_id != 
                                    5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds'])['unique_event_id'].iloc[-1] != freekick_event_id:    

                                    qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id != id_shot].reset_index(drop=True)

                    shot_event_ids = ', '.join([str(int(x)) for x in qualifying_shots.unique_event_id.tolist()])
                    shot_player_ids = ', '.join(qualifying_shots.player_id.tolist())
                    for shot_event_id in qualifying_shots.unique_event_id.unique():
                        shot_time = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['Time_in_Seconds'].iloc[0]
                        shot_player_id = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['player_id'].iloc[0]
                    #shot_player_names = ', '.join(qualifying_shots.player_name.tolist())
                        shot_player_name = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['player_name'].iloc[0]

                        shot_team_id = 't' + str(int(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['team_id'].iloc[0]))
                        shot_team_name = np.where(int(shot_team_id.replace('t','')) == home_team_id, 
                            home_team_name, away_team_name).tolist()
                    # shot_label = np.where((np.any(qualifying_shots.value == opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0])) & (np.any(qualifying_shots.value != opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0])), 
                    #     'Related & Delayed Shot', np.where(np.all(qualifying_shots.value == opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0]), 
                    #         'Related Shot', 'Delayed Shot')).tolist()
                        shot_label = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                            opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0]) | (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                            opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[0,9]]['event_id'].iloc[0]), 'Related', 
                            'Delayed').tolist()

                        shot_outcome = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['goal'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==0), 
                            'Goal', np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['on_target'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==0), 
                                'On Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['off_target'].iloc[0]==1, 
                                    'Off Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['blocked'].iloc[0]==1, 
                                        'Blocked', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==1, 
                                            'Own Goal', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['chance_missed'].iloc[0]==1,
                                                'Chance Missed', None)))))).tolist()

                        shot_body_part = np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['left_foot'].iloc[0]==1, 
                            'Left Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['right_foot'].iloc[0]==1, 
                                'Right Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['headed'].iloc[0]==1, 
                                    'Header', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['other_body_part'].iloc[0]==1, 
                                        'Other Body Part', None))))



                        shot_x = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['x'].iloc[0]
                        shot_y = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['y'].iloc[0]

                        data_between_set_piece_and_shot = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds >= freekick_time_seconds) & (opta_event_data_df.time_in_seconds <= shot_time) & (~opta_event_data_df.unique_event_id.isin([freekick_event_id, shot_event_id]))].reset_index(drop=True)
                        if len(data_between_set_piece_and_shot.unique_event_id.unique()) == 0:
                            number_events_in_between_shot = 0
                            opta_event_id_between_shot = None
                            opta_type_id_between_shot = None
                            opta_descr_between_shot = None
                        else:
                            data_between_set_piece_and_shot_reduced = data_between_set_piece_and_shot[(~data_between_set_piece_and_shot.type_id.isin([5,43,70]))].reset_index(drop=True)
                            if data_between_set_piece_and_shot_reduced.shape[0] == 0:
                                number_events_in_between_shot = 0
                                opta_event_id_between_shot = None
                                opta_type_id_between_shot = None  
                                opta_descr_between_shot = None 
                            else:
                                number_events_in_between_shot = len(data_between_set_piece_and_shot_reduced.unique_event_id.unique())
                                opta_event_id_between_shot = ', '.join([str(x) for x in data_between_set_piece_and_shot_reduced.unique_event_id.unique()])
                                opta_type_id_between_shot = ', '.join([str(int(x)) for x in data_between_set_piece_and_shot_reduced.drop_duplicates(['unique_event_id']).type_id])
                                try:
                                    opta_descr_between_shot = ', '.join([event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] for x in opta_type_id_between_shot.split(', ')])
                                except:
                                    opta_descr_between_shot = ''
                                    for x in opta_type_id_between_shot.split(', '):
                                        if int(x) in event_type_id.type_id.tolist():
                                            opta_descr_between_shot = opta_descr_between_shot + event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] + ', '
                                        else:
                                            if int(x) == 83:
                                                opta_descr_between_shot = opta_descr_between_shot + 'Attempted Tackle (post-match only)'  + ', '
                                            else:
                                                opta_descr_between_shot = opta_descr_between_shot + 'Event not identified' + ', '
                                if opta_descr_between_shot.endswith(', '):
                                    opta_descr_between_shot = opta_descr_between_shot[:-2]


                        list_all_shots.append([freekick_event_id, shot_event_id, shot_player_id, shot_player_name, shot_team_id, 
                            shot_team_name, shot_label, shot_outcome, shot_body_part, aerial_duel_is_shot, shot_time - freekick_time_seconds, 
                            number_events_in_between_shot, opta_event_id_between_shot, opta_descr_between_shot, 
                            shot_x, shot_y])

                else:
                    shot_event_ids = None 
                    shot_player_ids = None

                if frontal_lateral_end != 'End Shot':
                    where_relevant_pass = np.where(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])[0][-1]
                    events_after_relevant_pass = opta_event_data_df[(~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.iloc[:(where_relevant_pass+1)].unique())) & (opta_event_data_df.time_in_seconds >= 
                        passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]) & (opta_event_data_df.time_in_seconds <= 
                        10 + passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]) & (opta_event_data_df.unique_event_id != 
                        freekick_event_id) & (opta_event_data_df.unique_event_id != 
                        passes_in_window[index_longest_pass,9]) & (opta_event_data_df.period_id ==
                        period_id) & (~opta_event_data_df.type_id.isin([21,34,35,36,38,39,40,43,53,55,58,63,69,70,71,77,79,83]))].reset_index(drop=True)

                else:
                    events_after_relevant_pass = opta_event_data_df[(opta_event_data_df.time_in_seconds >= 
                        min_shot*60 + sec_shot) & (opta_event_data_df.unique_event_id!=event_id_first_shot) & (opta_event_data_df.time_in_seconds <= 10 + min_shot*60 + sec_shot) & (opta_event_data_df.period_id ==
                        period_id) & (~opta_event_data_df.type_id.isin([21,34,35,36,38,39,40,43,53,55,58,63,69,70,71,77,79,83]))].reset_index(drop=True)                

                # if events_after_relevant_pass.shape[0] == 0:
                #     aerial_duel_ids = None
                #     successful_player_id_duel = None
                #     successful_player_name_duel = None
                #     successful_team_id_duel = None 
                #     unsuccessful_player_id_duel = None 
                #     unsuccessful_player_name_duel = None
                #     unsuccessful_team_id_duel = None
                first_contact_keypass_assist = None 
                first_contact_aerial = 0
                first_contact_shot = 0
                first_contact_x = np.nan
                first_contact_y = np.nan 
                description = None
                first_contact_event_id = np.nan
                if events_after_relevant_pass.shape[0] > 0:
                    if (events_after_relevant_pass.type_id.iloc[0] in [1,2,3,4,7,10,11,13,14,15,16,8,12,27,30,41,44,45,49,50,51,52,54,56,59,60,61,67,74]) & (passes_in_window[index_longest_pass,5]!=2):
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['keypass'].iloc[0] == 1:
                            first_contact_keypass_assist = 'Keypass'
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['assist'].iloc[0] == 1:
                            first_contact_keypass_assist = 'Assist'
                        if (3 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (168 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                            first_contact_aerial = 1
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['type_id'].iloc[0] in [13,14,15,16]:
                            first_contact_shot = 1    
                        if events_after_relevant_pass.type_id.iloc[0] not in [4, 44, 67]:
                            if events_after_relevant_pass.type_id.iloc[0] in event_type_id.type_id.tolist():
                                if (events_after_relevant_pass.type_id.iloc[0] == 12) & (15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                    description = 'Headed Clearance'
                                elif (events_after_relevant_pass.type_id.iloc[0] == 16) & (28 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                    description = 'Own Goal'
                                    if 15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist():
                                        description = 'Headed' + ' ' + description
                                else:
                                    description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0]
                                    if first_contact_aerial == 1:
                                        if len(set([3, 15]).intersection(set(opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()))) > 0:
                                            description = 'Headed' + ' ' + description
                                        elif len(set([168]).intersection(set(opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()))) > 0:
                                            description = 'Flick-on' + ' ' + description
                                    if events_after_relevant_pass.type_id.iloc[0] in [1, 3, 7, 61]:
                                        description = np.where(events_after_relevant_pass.outcome.iloc[0]==1, 'Successful', 'Unsuccessful').tolist() + ' ' + description
                                        if (238 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                            description = description + ' ' + '(fair-play)'
                                    if events_after_relevant_pass.type_id.iloc[0] == 51:
                                        description = description + ' ' + np.where(events_after_relevant_pass.qualifier_id.iloc[0] == 169, 'leading to attempt', 
                                            np.where(events_after_relevant_pass.qualifier_id.iloc[0] == 170, 'leading to goal', '')).tolist()
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_event_id = events_after_relevant_pass.unique_event_id.iloc[0]
                            if np.isnan(events_after_relevant_pass.player_id.iloc[0]):
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                            else:
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass.player_id.iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass.team_id.iloc[0]))
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            first_contact_x = events_after_relevant_pass['x'].iloc[0]
                            first_contact_y = events_after_relevant_pass['y'].iloc[0]
                        else:
                            if events_after_relevant_pass.type_id.iloc[0] == 67:
                                #first_contact_aerial = 1
                                description = '50/50 Won'
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                                first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                    attacking_team, defending_team).tolist()
                                first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]
                            if events_after_relevant_pass.type_id.iloc[0] == 44:
                                first_contact_aerial = 1
                                description = 'Aerial Duel Won'
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                                first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                    attacking_team, defending_team).tolist()
                                first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]
                            if events_after_relevant_pass.type_id.iloc[0] == 4:
                                if (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Handball'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Foul for illegal restart'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (314 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (314 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Foul for shot hitting offside player'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Simulation'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist()  
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]
                                else:
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0] 
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                    description = 'Foul Won'
                                    if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==1].player_id.iloc[0]):
                                        first_contact_player_id = None 
                                        first_contact_player_name = None 
                                    else:
                                        first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1].player_id.iloc[0]))
                                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))         
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist()
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]

                    else:
                        first_contact_type = None 
                        first_contact_event_id = np.nan
                        first_contact_player_id = None 
                        first_contact_player_name = None 
                        first_contact_team_id = None 
                        first_contact_team_name = None 
                        first_contact_aerial = None 
                        first_contact_shot = None
                        first_contact_x = np.nan 
                        first_contact_y = np.nan
                        if (passes_in_window[index_longest_pass,5]==2):
                            first_contact_type = 55
                            description = 'Player Caught Offside'
                            first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                            first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])]['team_id'].iloc[0]))
                            first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            first_contact_x = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==140)]['value'].iloc[0])
                            first_contact_y = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==141)]['value'].iloc[0])
                        if (events_after_relevant_pass.type_id.iloc[0] in [5,6]):
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]
                            description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0] + ' Lost'
                            if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]):
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                            else:
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                            first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]

                    if events_after_relevant_pass.type_id.iloc[0] == 44:
                        for aerial_duel_id in events_after_relevant_pass[events_after_relevant_pass.type_id==44].drop_duplicates(['unique_event_id']).unique_event_id.iloc[:2]:
                        #aerial_duel_ids = ', '.join([str(int(x)) for x in events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[:2].tolist()])
                            successful_player_id_duel = 'p' + str(int(events_after_relevant_pass.player_id.loc[events_after_relevant_pass.outcome==1].iloc[0]))
                            successful_player_name_duel = player_names_raw[player_names_raw['@uID']==successful_player_id_duel]['player_name'].iloc[0]
                            successful_team_id_duel = 't' + str(int(events_after_relevant_pass.team_id.loc[events_after_relevant_pass.outcome==1].iloc[0]))
                            successful_team_name_duel = np.where(successful_team_id_duel == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            unsuccessful_player_id_duel = 'p' + str(int(events_after_relevant_pass.player_id.loc[events_after_relevant_pass.outcome==0].iloc[0])) 
                            unsuccessful_player_name_duel = player_names_raw[player_names_raw['@uID']==unsuccessful_player_id_duel]['player_name'].iloc[0]                                        
                            unsuccessful_team_id_duel = 't' + str(int(events_after_relevant_pass.team_id.loc[events_after_relevant_pass.outcome==0].iloc[0]))
                            unsuccessful_team_name_duel = np.where(successful_team_id_duel == 't' + str(int(attacking_team_id)), 
                                defending_team, attacking_team).tolist()

                            successful_x = events_after_relevant_pass['x'].loc[events_after_relevant_pass.outcome==1].iloc[0]
                            successful_y = events_after_relevant_pass['y'].loc[events_after_relevant_pass.outcome==1].iloc[0]
                            unsuccessful_x = events_after_relevant_pass['x'].loc[events_after_relevant_pass.outcome==0].iloc[0]
                            unsuccessful_y = events_after_relevant_pass['y'].loc[events_after_relevant_pass.outcome==0].iloc[0]

                            if (shot_player_ids is not None) & (qualifying_shots.shape[0] > 0):
                                if (successful_player_id_duel == shot_player_ids.split(', ')[0]) & (- events_after_relevant_pass.time_in_seconds.iloc[0] + qualifying_shots.Time_in_Seconds.iloc[0] <= 1) & (15 in list(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==int(shot_event_ids.split(', ')[0])].qualifier_id)):
                                    aerial_duel_is_shot = 1

                            if 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==aerial_duel_id]['player_id'].iloc[0])) == successful_player_id_duel:
                                list_aerial_duels.append([freekick_event_id, aerial_duel_id, successful_player_id_duel, 
                                    successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, successful_x, successful_y, 
                                    'Successful', unsuccessful_player_id_duel, 
                                    unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, unsuccessful_x, unsuccessful_y,
                                    aerial_duel_is_shot])
                            else:
                                list_aerial_duels.append([freekick_event_id, aerial_duel_id, unsuccessful_player_id_duel, 
                                    unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, unsuccessful_x, unsuccessful_y,
                                    'Unsuccessful', successful_player_id_duel, 
                                    successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, successful_x, successful_y, 
                                    aerial_duel_is_shot])


                else:
                    first_contact_type = None 
                    first_contact_event_id = np.nan
                    first_contact_player_id = None 
                    first_contact_player_name = None 
                    first_contact_team_id = None 
                    first_contact_team_name = None 
                    first_contact_aerial = None
                    first_contact_shot = None
                    first_contact_x = np.nan
                    first_contact_y = np.nan 
                    if (passes_in_window[index_longest_pass,5]==2):
                        first_contact_type = 55
                        description = 'Player Caught Offside'
                        first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                        first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])]['team_id'].iloc[0]))
                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                        first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                            attacking_team, defending_team).tolist()
                        first_contact_x = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==140)]['value'].iloc[0])
                        first_contact_y = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==141)]['value'].iloc[0])

            
            time_between_relevant_and_first_contact = np.nan
            if events_after_relevant_pass.shape[0] > 0:
                if (first_contact_type is not None):
                    time_between_relevant_and_first_contact = np.round(events_after_relevant_pass.time_in_seconds.iloc[0] - (passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]))
                if first_contact_keypass_assist is not None:
                    description = description + ' (' + first_contact_keypass_assist + ')'

            goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==30) & (opta_event_data_df.team_id==defending_team_id)]['value'].tolist()[0].split(',')[0]))
            gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
            gk_sub_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==18].unique().tolist()))) == 1, 1, 0).tolist()
            gk_sent_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33]))].unique().tolist()))) == 1, 1, 0).tolist()
            retired_gk = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==20].unique().tolist()))) == 1, 1, 0).tolist()

            if (gk_sub_off==1) | (gk_sent_off==1):
                time_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['time_in_seconds']
                period_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['period_id']

                if len(time_gk_in) > 0:
                    if ((time_gk_in.iloc[0] < freekick_mins*60+freekick_secs) & (period_gk_in.iloc[0] == period_id)) | (period_gk_in.iloc[0] < period_id):
                        goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['player_id'].iloc[0])) 
                        gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
                else:
                    goalkeeper_id = None
                    gk_name = None            


            if retired_gk == 1:
                time_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['time_in_seconds'].iloc[0]
                period_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['period_id'].iloc[0]
            
                if ((time_gk_retired < freekick_mins*60+freekick_secs) & (period_gk_retired == period_id)) | (period_gk_retired < period_id):
                    goalkeeper_id = None
                    gk_name = None


            if (frontal_lateral_end == 'End Outside A Target Zone') & (open_play_indicator == 0):
                list_outside.append(['f' + str(int(game_id)), fixture, attacking_team, defending_team, 't' + str(int(attacking_team_id)), 't' + str(int(defending_team_id)), attacking_team_goals_up_to_freekick_excluded, 
                    defending_team_goals_up_to_freekick_excluded, goal_diff_attack_v_defense, 
                    np.where(goal_diff_attack_v_defense > 0, 'winning', np.where(goal_diff_attack_v_defense == 0, 'drawing', 'losing')).tolist()
                    ,start_side
                    ,number_events_in_window, np.where(direct==1, 'Direct', 'Indirect').tolist(),
                    freekick_event_id, period_id, freekick_mins, 
                    freekick_secs, freekick_x_start, freekick_y_start, passes_in_window[0,2], passes_in_window[0,3], freekick_player_id, freekick_player_name, 
                    distance_x_percent_start_pass, length_start_pass, event_id_relevant,
                    passes_in_window[index_longest_pass, -2], passes_in_window[index_longest_pass, -1], passes_in_window[index_longest_pass, 0], 
                    passes_in_window[index_longest_pass, 1], passes_in_window[index_longest_pass, 2], passes_in_window[index_longest_pass, 3],
                    freekick_player_id_relevant, freekick_player_name_relevant, 
                    distance_x_percent_relevant_pass, length_relevant_pass,
                    type_of_freekick, area_of_pitch, after_box, frontal_lateral_start, frontal_lateral_end, side, time_between_foul_and_freekick,
                    number_events_in_between, opta_event_id_between,
                    player_ids_pass_sequence, player_names_pass_sequence, rolled, first_contact_event_id,
                    first_contact_type, description, first_contact_player_id, first_contact_player_name, first_contact_team_id, 
                    first_contact_team_name, first_contact_aerial, first_contact_shot, first_contact_x, first_contact_y, time_between_relevant_and_first_contact, goalkeeper_id, gk_name])     
                #continue  
            #else:
                #continue     




            if (frontal_lateral_end != 'End Outside A Target Zone') & (open_play_indicator == 0):

                if frontal_lateral_end != 'End Shot':
                    list_frontal_lateral.append(['f' + str(int(game_id)), fixture, attacking_team, defending_team, 't' + str(int(attacking_team_id)), 't' + str(int(defending_team_id)), attacking_team_goals_up_to_freekick_excluded, 
                        defending_team_goals_up_to_freekick_excluded, goal_diff_attack_v_defense, 
                        np.where(goal_diff_attack_v_defense > 0, 'winning', np.where(goal_diff_attack_v_defense == 0, 'drawing', 'losing')).tolist()
                        ,start_side
                        ,number_events_in_window, np.where(direct==1, 'Direct', 'Indirect').tolist(),
                        freekick_event_id, period_id, freekick_mins, 
                        freekick_secs, freekick_x_start, freekick_y_start, passes_in_window[0,2], passes_in_window[0,3], freekick_player_id, freekick_player_name, 
                        distance_x_percent_start_pass, length_start_pass, event_id_relevant, 
                        passes_in_window[index_longest_pass, -2], passes_in_window[index_longest_pass, -1], passes_in_window[index_longest_pass, 0], 
                        passes_in_window[index_longest_pass, 1], passes_in_window[index_longest_pass, 2], passes_in_window[index_longest_pass, 3],
                        freekick_player_id_relevant, freekick_player_name_relevant, 
                        distance_x_percent_relevant_pass, length_relevant_pass,
                        type_of_freekick, area_of_pitch, after_box, frontal_lateral_start, frontal_lateral_end, side, time_between_foul_and_freekick,
                        number_events_in_between, opta_event_id_between,
                        player_ids_pass_sequence, player_names_pass_sequence, rolled, first_contact_event_id,
                        first_contact_type, description, first_contact_player_id, first_contact_player_name, first_contact_team_id, 
                        first_contact_team_name, first_contact_aerial, first_contact_shot, first_contact_x, first_contact_y, time_between_relevant_and_first_contact, goalkeeper_id, gk_name])

                else:
                    # qualifiers_shot = [int(x) for x in first_shot_qualifier_ids.split(', ')]
                    # if type_id_first_shot == 16:
                    #     outcome_shot = 'Goal'
                    # elif type_id_first_shot in [13,14]:
                    #     outcome_shot = 'Off Target'
                    # else:
                    #     if 82 in qualifiers_shot:
                    #         if 101 in qualifiers_shot:
                    #             outcome_shot = 'On Target'
                    #         else:
                    #             outcome_shot = 'Blocked'
                    #     else:
                    #         outcome_shot = 'On Target'
                    list_shots.append(['f' + str(int(game_id)), fixture, attacking_team, defending_team, 't' + str(int(attacking_team_id)), 't' + str(int(defending_team_id)), attacking_team_goals_up_to_freekick_excluded, 
                        defending_team_goals_up_to_freekick_excluded, goal_diff_attack_v_defense, 
                        np.where(goal_diff_attack_v_defense > 0, 'winning', np.where(goal_diff_attack_v_defense == 0, 'drawing', 'losing')).tolist()
                        ,np.where(direct==1, 'Direct', 'Indirect').tolist(),
                        freekick_event_id, period_id, freekick_mins, 
                        freekick_secs, freekick_x_start, freekick_y_start, np.where(passes_in_window[0,8] == 24, passes_in_window[0,2], -1).tolist(), 
                        np.where(passes_in_window[0,8] == 24, passes_in_window[0,3], -1).tolist(), freekick_player_id, freekick_player_name, 
                        event_id_first_shot, min_shot, sec_shot, x_start_shot, y_start_shot, -1, -1,
                        freekick_player_id_first_shot, freekick_player_name_first_shot, rolled, first_contact_event_id,
                        first_contact_type, description, first_contact_player_id, first_contact_player_name, first_contact_team_id, 
                        first_contact_team_name, first_contact_aerial, first_contact_shot, first_contact_x, first_contact_y, time_between_relevant_and_first_contact, goalkeeper_id, gk_name])

                    shot_outcome = np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['goal'].iloc[0]==1, 
                        'Goal', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['on_target'].iloc[0]==1, 
                            'On Target', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['off_target'].iloc[0]==1, 
                                'Off Target', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['blocked'].iloc[0]==1, 
                                    'Blocked', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['own_goal'].iloc[0]==1, 
                                        'Own Goal', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['chance_missed'].iloc[0]==1,
                                            'Chance Missed', None)))))).tolist()

                    shot_body_part = np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['left_foot'].iloc[0]==1, 
                        'Left Foot', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['right_foot'].iloc[0]==1, 
                            'Right Foot', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['headed'].iloc[0]==1, 
                                'Header', np.where(data_shots[data_shots.unique_event_id==event_id_first_shot]['other_body_part'].iloc[0]==1, 
                                    'Other Body Part', None))))


                    list_all_shots.append([freekick_event_id, event_id_first_shot, freekick_player_id_first_shot, 
                        freekick_player_name_first_shot, 't' + str(int(attacking_team_id)), 
                        attacking_team, 'Free Kick Shot', shot_outcome, shot_body_part, None, 0, 0, None, None, freekick_x_start, 
                        freekick_y_start])

        # summary_df = pd.DataFrame(list_events, columns = ['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID', 'Goals Scored', 'Goals Conceded',
        #     'Goal Difference', 'Game State', 'Side', 'Number Events In Window', 'Direct',
        #     'OPTA Event ID', 'Event ID', 'period_id', 'min', 'sec', 'Type ID', 'Player ID', 'Player Name', 'Qualifier IDs', 'Qualifier Values', 
        #     'X Start', 'Y Start', 'X End', 'Y End', 'Chipped', 'Launch', 'Cross', 'Overhit Cross', 'Switch Of Play', '% Distance Along X', 'Length Pass', 'OPTA Event ID Relevant', 'Event ID Relevant',
        #     'Type ID Relevant', 'Player ID Relevant', 'Player Name Relevant', 'Qualifier IDs Relevant', 'Qualifier Values Relevant',
        #     'X Start Relevant', 'Y Start Relevant', 'X End Relevant', 'Y End Relevant', 'Chipped Relevant', 'Launch Relevant', 'Cross Relevant', 'Overhit Cross Relevant', 'Switch Of Play Relevant', '% Distance Along X Relevant Pass', 'Length Relevant Pass',
        #     'OPTA Event ID First Shot', 'Event ID First Shot',
        #     'Type ID First Shot', 'Player ID First Shot', 'Player Name First Shot', 'Qualifier IDs First Shot', 'Qualifier Values First Shot',
        #     'X Start First Shot', 'Y Start First Shot',
        #     'Type Of FreeKick', 'Start Area Of Pitch', 'FreeKick Starts After Box', 'Frontal/Lateral Start', 'Frontal/Lateral End', 'Same Side', 'FreeKick After A Foul', 'Quick FreeKick', 'Time Lapsed From Stop To FreeKick',
        #     'Number Of Events Between Stop And FreeKick', 'OPTA Event IDs Between Stop And FreeKick', 'OPTA Type IDs Between Stop And FreeKick', 'Index Relevant Pass',
        #     'X Starts', 'Y Starts', 'X Ends', 'Y Ends', 'Player IDs In Pass Sequence Up To Relevant Pass', 'Player Names In Pass Sequence Up To Relevant Pass', 'Rolled', 
        #     'FreeKicks Taken From Middle Third', 'Frontal FreeKicks Taken From Final Third Before Box', 'Frontal FreeKicks Taken From Final Third After Box', 
        #     'Lateral FreeKicks Taken From Final Third Before Box', 'Lateral FreeKicks Taken From Final Third After Box'])

        summary_df_outside = pd.DataFrame(list_outside, columns = ['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID', 'Goals Scored', 'Goals Conceded',
            'Goal Difference', 'Game State', 'Side', 'Number Events In Window', 'Direct',
            'OPTA Event ID', 'period_id', 'min', 'sec', 'X Coordinate', 'Y Coordinate', 'End X Coordinate', 'End Y Coordinate', 'Player ID', 'Player Name', 
            '% Distance Along X', 'Length Pass', 
            'Relevant OPTA Event ID', 'Relevant min', 'Relevant sec', 'Relevant X Coordinate', 'Relevant Y Coordinate', 
            'Relevant End X Coordinate', 'Relevant End Y Coordinate',  'Relevant Player ID', 'Relevant Player Name', 
            'Relevant % Distance Along X', 'Relevant Length Pass', 
            'Type Of FreeKick', 'Start Area Of Pitch', 'FreeKick Starts After Box', 
            'Frontal/Lateral Start', 'Frontal/Lateral End', 'Same Side', 'Time Lapsed From Stop And Start',
            'Number Of Events Between Stop And Start', 'OPTA Event IDs Between Stop And Start', 
            'Player IDs In Pass Sequence Up To Relevant', 'Player Names In Pass Sequence Up To Relevant', 'Rolled', 'First Contact Event ID',
            'First Contact Type', 'First Contact Explanation', 'First Contact Player ID', 'First Contact Player Name', 
            'First Contact Team ID', 'First Contact Team Name', 'First Contact Aerial', 'First Contact Shot', 'First Contact X Coordinate', 'First Contact Y Coordinate', 'Time Lapsed From Relevant And First Contact', 
            'Defending Goalkeeper ID', 'Defending Goalkeeper Name'])
        summary_df_outside['Set Piece Type'] = 'Free Kick Outside Target Zone'

        summary_df_frontal_lateral = pd.DataFrame(list_frontal_lateral, columns = ['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID', 'Goals Scored', 'Goals Conceded',
            'Goal Difference', 'Game State', 'Side', 'Number Events In Window', 'Direct',
            'OPTA Event ID', 'period_id', 'min', 'sec', 'X Coordinate', 'Y Coordinate', 'End X Coordinate', 'End Y Coordinate', 'Player ID', 'Player Name', 
            '% Distance Along X', 'Length Pass', 
            'Relevant OPTA Event ID', 'Relevant min', 'Relevant sec', 'Relevant X Coordinate', 'Relevant Y Coordinate', 
            'Relevant End X Coordinate', 'Relevant End Y Coordinate', 'Relevant Player ID', 'Relevant Player Name', 
            'Relevant % Distance Along X', 'Relevant Length Pass', 
            'Type Of FreeKick', 'Start Area Of Pitch', 'FreeKick Starts After Box', 
            'Frontal/Lateral Start', 'Frontal/Lateral End', 'Same Side', 'Time Lapsed From Stop And Start',
            'Number Of Events Between Stop And Start', 'OPTA Event IDs Between Stop And Start', 
            'Player IDs In Pass Sequence Up To Relevant', 'Player Names In Pass Sequence Up To Relevant', 'Rolled', 'First Contact Event ID',
            'First Contact Type', 'First Contact Explanation', 'First Contact Player ID', 'First Contact Player Name', 
            'First Contact Team ID', 'First Contact Team Name', 'First Contact Aerial', 'First Contact Shot', 'First Contact X Coordinate', 'First Contact Y Coordinate', 'Time Lapsed From Relevant And First Contact',
            'Defending Goalkeeper ID', 'Defending Goalkeeper Name'])
        summary_df_frontal_lateral['Set Piece Type'] = 'Free Kick'

        summary_df_shots = pd.DataFrame(list_shots, columns = ['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID', 'Goals Scored', 'Goals Conceded',
            'Goal Difference', 'Game State', 'Direct',
            'OPTA Event ID', 'period_id', 'min', 'sec', 'X Coordinate', 'Y Coordinate', 'End X Coordinate', 'End Y Coordinate', 'Player ID', 'Player Name', 
            'Relevant OPTA Event ID', 'Relevant min', 'Relevant sec', 'Relevant X Coordinate', 'Relevant Y Coordinate', 
            'Relevant End X Coordinate', 'Relevant End Y Coordinate', 'Relevant Player ID', 'Relevant Player Name', 
            'Rolled', 'First Contact Event ID',
            'First Contact Type', 'First Contact Explanation', 'First Contact Player ID', 'First Contact Player Name', 
            'First Contact Team ID', 'First Contact Team Name', 'First Contact Aerial', 'First Contact Shot', 'First Contact X Coordinate', 'First Contact Y Coordinate', 'Time Lapsed From Relevant And First Contact',
            'Defending Goalkeeper ID', 'Defending Goalkeeper Name'])
        summary_df_shots['Set Piece Type'] = 'Free Kick Shot'

        summary_df_all_shots = pd.DataFrame(list_all_shots, columns = 
            ['Set Piece OPTA Event ID', 'Shot OPTA ID', 'Shot Player ID', 'Shot Player Name', 
            'Shot Team ID', 'Shot Team Name', 'Shot Occurrence', 'Shot Outcome', 'Shot Body Part', 'Aerial Duel Is Shot', 'Time Lapsed From Set Piece And Shot', 
            'Number Of Events Between Set Piece And Shot', 'OPTA Event IDs Between Set Piece And Shot',
            'Events Explanation Between Set Piece And Shot', 'Shot X Coordinate', 'Shot Y Coordinate'])

        summary_df_aerial_duels = pd.DataFrame(list_aerial_duels, columns = 
            ['Set Piece OPTA Event ID', 'Aerial Duel OPTA ID', 
            'Aerial Duel Player ID', 'Aerial Duel Player Name', 
            'Aerial Duel Team ID', 'Aerial Duel Team Name', 'X Coordinate Player', 'Y Coordinate Player', 'Successful/Unsuccessful',
            'Other Aerial Duel Player ID', 'Other Aerial Duel Player Name', 
            'Other Aerial Duel Team ID', 'Other Aerial Duel Team Name', 'Other X Coordinate Player', 'Other Y Coordinate Player',
            'Aerial Duel Is Shot'])

        if np.any(summary_df_aerial_duels['Aerial Duel Is Shot']==1):
            for set_piece in summary_df_aerial_duels[summary_df_aerial_duels['Aerial Duel Is Shot']==1].drop_duplicates(['Set Piece OPTA Event ID'])['Set Piece OPTA Event ID']:
                summary_df_all_shots.loc[(summary_df_all_shots['Set Piece OPTA Event ID']==set_piece) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Set Piece OPTA Event ID']==set_piece]['Shot OPTA ID'].iloc[0]), 'Aerial Duel Is Shot'] = 1

        #summary_df['FreeKicks Taken From Middle Third Total'] = np.array([x for y in np.array([np.repeat(0, summary_df.shape[0]-1), [max(summary_df['FreeKicks Taken From Middle Third'])]]) for x in y])
        #summary_df['Frontal FreeKicks Taken From Final Third Before Box Total'] = np.array([x for y in np.array([np.repeat(0, summary_df.shape[0]-1), [max(summary_df['Frontal FreeKicks Taken From Final Third Before Box'])]]) for x in y])
        #summary_df['Frontal FreeKicks Taken From Final Third After Box Total'] = np.array([x for y in np.array([np.repeat(0, summary_df.shape[0]-1), [max(summary_df['Frontal FreeKicks Taken From Final Third After Box'])]]) for x in y])
        #summary_df['Lateral FreeKicks Taken From Final Third Before Box Total'] = np.array([x for y in np.array([np.repeat(0, summary_df.shape[0]-1), [max(summary_df['Lateral FreeKicks Taken From Final Third Before Box'])]]) for x in y])
        #summary_df['Lateral FreeKicks Taken From Final Third After Box Total'] = np.array([x for y in np.array([np.repeat(0, summary_df.shape[0]-1), [max(summary_df['Lateral FreeKicks Taken From Final Third After Box'])]]) for x in y])

        summary_df = pd.concat([summary_df_frontal_lateral, summary_df_outside, summary_df_shots], axis=0, ignore_index=True, sort = False)


        ##here we make adjustments to add first contact shot info to main set pieces file and shots file
        summary_df_all_shots['First Contact Shot'] = np.where(summary_df_all_shots['Aerial Duel Is Shot']==1, 1, 0)
        if np.any(summary_df_all_shots['First Contact Shot']==1):
            for set_piece in summary_df_all_shots[summary_df_all_shots['First Contact Shot']==1].drop_duplicates(['Set Piece OPTA Event ID'])['Set Piece OPTA Event ID']:
                summary_df.loc[summary_df['OPTA Event ID']==set_piece, 'First Contact Shot'] = 1
        if np.any(summary_df['First Contact Shot']==1):
            for set_piece in summary_df[summary_df['First Contact Shot']==1].drop_duplicates(['OPTA Event ID'])['OPTA Event ID']:
                if summary_df_all_shots[summary_df_all_shots['Set Piece OPTA Event ID']==set_piece].shape[0] > 0:
                    summary_df_all_shots.loc[(summary_df_all_shots['Set Piece OPTA Event ID']==set_piece) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Set Piece OPTA Event ID']==set_piece]['Shot OPTA ID'].iloc[0]), 'First Contact Shot'] = 1

        
        #here we add first contact coordinates to shots file
        summary_df_all_shots = summary_df_all_shots.merge(summary_df[['OPTA Event ID', 'First Contact X Coordinate', 
            'First Contact Y Coordinate']], how = 'inner', left_on = ['Set Piece OPTA Event ID'], 
            right_on = ['OPTA Event ID']).drop(['OPTA Event ID'], axis = 1)

        #return summary_df[summary_df['Type Of FreeKick'] != 'None'].drop(['Type Of FreeKick'], axis = 1).reset_index(drop=True)
        return summary_df.drop(['Type Of FreeKick'], axis = 1).reset_index(drop=True), summary_df_all_shots.reset_index(drop=True), summary_df_aerial_duels.reset_index(drop=True)

    def corners_classification(self, df_opta_events: pd.DataFrame, df_player_names_raw: pd.DataFrame, match_info: dict,
                               opta_match_info: dict, df_opta_crosses: pd.DataFrame, df_opta_shots: pd.DataFrame) -> \
            (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """[summary]

        Args:
            df_opta_events (pd.DataFrame): [description]
            df_player_names_raw (pd.DataFrame): [description]
            match_info (dict): [description]
            opta_match_info (dict): [description]
            df_opta_crosses (pd.DataFrame): [description]
            df_opta_shots (pd.DataFrame): [description]

        Returns:
            pd.DataFrame: [description]
            pd.DataFrame: [description]
            pd.DataFrame: [description]

        """
        # always assume running from root
        # this should be loaded from data/
        event_type_id = pd.read_excel(os.path.join('scripts_from_fed', 'f24 - ID definitions.xlsx'))

        opta_event_data_df = df_opta_events

        game_id = opta_match_info['match_id']
        away_team_id = opta_match_info['away_team_id']
        away_team_name = opta_match_info['away_team_name']
        home_team_id = opta_match_info['home_team_id']
        home_team_name = opta_match_info['home_team_name']

        player_names_raw = df_player_names_raw

        data_crosses = df_opta_crosses
        data_shots = df_opta_shots

        length_pitch = match_info['pitchLength']
        width_pitch = match_info['pitchWidth']

        player_names_raw['@uID'] = 'p' + player_names_raw['player_id'].astype(str)
        player_names_raw['player_name'] = player_names_raw['full_name']

        if 'p' in str(list(opta_event_data_df['player_id'].values)):
            opta_event_data_df['player_id'] = opta_event_data_df['player_id'].astype(str).str.replace("p","").astype(float) #remove p

        #here teh actual loop starts
        #summary_list = []

        opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']
        fixture = opta_event_data_df['fixture'].iloc[0]
        freekicks_taken = opta_event_data_df.loc[(((opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.loc[opta_event_data_df.qualifier_id==6].unique().tolist()))) & (opta_event_data_df.type_id==1)) | (((opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.loc[opta_event_data_df.qualifier_id==263].unique().tolist()))) & (opta_event_data_df.type_id.isin([13,14,15,16])))].reset_index(drop = True) #we include all free kicks instances
        freekicks_taken['time_in_seconds'] = freekicks_taken['min']*60.0 + freekicks_taken['sec']
        freekicks_taken['time_in_seconds'] = freekicks_taken['time_in_seconds'].astype(float)



        #| (opta_event_data_df.type_id.isin([1,2,13,14,15,16]))
        #crosses_df = opta_event_data_df.loc[(opta_event_data_df.type_id==1) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.loc[opta_event_data_df.qualifier_id==2].unique().tolist())) & (opta_event_data_df.period_id != 5)] #contains all crosses in a game with related qualifiers

        #thresh_pass = 5.05
        #thresh_dist_direct = 5.05
        #thresh_dist_indirect = 7.07

        list_events = []
        list_all_shots = []
        list_aerial_duels = []

        for freekick_event_id in freekicks_taken.unique_event_id.unique():
            #freekick_event_id = 1162405752
            #freekick_event_id = 1938127774
            #freekick_event_id = 1893310504
            #freekick_event_id = 1418929501
            #freekick_event_id = 116705383

            #check for the freekick event taken 
            attacking_team_id = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, home_team_id, away_team_id).tolist()
            defending_team_id = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, away_team_id, home_team_id).tolist()
            attacking_team = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, home_team_name, away_team_name).tolist()
            defending_team = np.where(freekicks_taken[(freekicks_taken.unique_event_id==freekick_event_id)]['team_id'].iloc[0]==home_team_id, away_team_name, home_team_name).tolist()
            period_id = int(freekicks_taken['period_id'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0])
            freekick_mins = int(freekicks_taken['min'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0])
            freekick_secs = int(freekicks_taken['sec'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0]) 
            freekick_time_seconds = freekick_mins*60.0 + freekick_secs    
            attacking_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==attacking_team_id) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==defending_team_id) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < period_id) | ((opta_event_data_df.time_in_seconds < freekick_time_seconds) & (opta_event_data_df.period_id==period_id)))]['unique_event_id'].unique())    
            defending_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==defending_team_id) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < period_id) | ((opta_event_data_df.time_in_seconds < freekick_time_seconds) & (opta_event_data_df.period_id==period_id)))]['unique_event_id'].unique())    
            goal_diff_attack_v_defense = attacking_team_goals_up_to_freekick_excluded - defending_team_goals_up_to_freekick_excluded
            game_state = np.sign(goal_diff_attack_v_defense)
            freekick_event_id_event = int(freekicks_taken.event_id.loc[freekicks_taken.unique_event_id==freekick_event_id].unique()[0])
            freekick_type_id = int(freekicks_taken.type_id.loc[freekicks_taken.unique_event_id==freekick_event_id].unique()[0])
            freekick_player_id = 'p' + str(int(freekicks_taken.player_id.loc[freekicks_taken.unique_event_id==freekick_event_id].unique()[0]))
            freekick_player_name = player_names_raw.player_name.loc[player_names_raw['@uID'] == freekick_player_id].iloc[0]
            freekick_qualifier_ids = ', '.join([str(int(x)) for x in freekicks_taken.qualifier_id.loc[freekicks_taken.unique_event_id==freekick_event_id].tolist()])
            freekick_qualifier_values = ', '.join([str(x) for x in freekicks_taken['value'].loc[freekicks_taken.unique_event_id==freekick_event_id].tolist()])
            freekick_x_start = freekicks_taken['x'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0]
            freekick_y_start = freekicks_taken['y'].loc[freekicks_taken.unique_event_id==freekick_event_id].iloc[0]
            #rolled = 0 #initialise 
            #direct = 0
            #freekick_contains_cross_qualifier = int(2 in [int(x) for x in freekick_qualifier_ids.split(', ')])

            if freekick_type_id != 1: #we have a direct corner shot so all the following logic does not apply

                qualifiers_shot = [int(x) for x in freekick_qualifier_ids.split(', ')]
                if freekick_type_id == 16:
                    outcome_shot = 'Goal'
                elif freekick_type_id in [13,14]:
                        outcome_shot = 'Off Target'
                else:
                    if 82 in qualifiers_shot:
                        if 101 in qualifiers_shot:
                            outcome_shot = 'On Target'
                        else:
                            outcome_shot = 'Blocked'
                    else:
                        outcome_shot = 'On Target'

                shot_body_part = np.where(data_shots[data_shots.unique_event_id==freekick_event_id]['left_foot'].iloc[0]==1, 
                    'Left Foot', np.where(data_shots[data_shots.unique_event_id==freekick_event_id]['right_foot'].iloc[0]==1, 
                        'Right Foot', np.where(data_shots[data_shots.unique_event_id==freekick_event_id]['headed'].iloc[0]==1, 
                            'Header', np.where(data_shots[data_shots.unique_event_id==freekick_event_id]['other_body_part'].iloc[0]==1, 
                                'Other Body Part', None))))

                data_corner_time = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id.isin([6])) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)]['time_in_seconds'].iloc[-1] #we take most recent event before freekick
                time_between_stop_and_corner = np.round(freekick_time_seconds - data_corner_time, 2)
                data_corner = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds > data_corner_time) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)].reset_index(drop=True)
                if len(data_corner.unique_event_id.unique()) == 0:
                    number_events_in_between = 0
                    opta_event_id_between = None
                    opta_type_id_between = None
                else:
                    data_corner_reduced = data_corner[(~data_corner.type_id.isin([6,43,70]))].reset_index(drop=True)
                    if data_corner_reduced.shape[0] == 0:
                        number_events_in_between = 0
                        opta_event_id_between = None
                        opta_type_id_between = None   
                    else:
                        number_events_in_between = len(data_corner_reduced.unique_event_id.unique())
                        opta_event_id_between = ', '.join([str(x) for x in data_corner_reduced.unique_event_id.unique()])
                        opta_type_id_between = ', '.join([str(int(x)) for x in data_corner_reduced.drop_duplicates(['unique_event_id']).type_id])

                goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==30) & (opta_event_data_df.team_id==defending_team_id)]['value'].tolist()[0].split(',')[0]))
                gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
                gk_sub_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==18].unique().tolist()))) == 1, 1, 0).tolist()
                gk_sent_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33]))].unique().tolist()))) == 1, 1, 0).tolist()
                retired_gk = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==20].unique().tolist()))) == 1, 1, 0).tolist()

                if (gk_sub_off==1) | (gk_sent_off==1):
                    time_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['time_in_seconds']
                    period_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['period_id']

                    if len(time_gk_in) > 0:
                        if ((time_gk_in.iloc[0] < freekick_mins*60+freekick_secs) & (period_gk_in.iloc[0] == period_id)) | (period_gk_in.iloc[0] < period_id):
                            goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['player_id'].iloc[0])) 
                            gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
                    else:
                        goalkeeper_id = None
                        gk_name = None            


                if retired_gk == 1:
                    time_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['time_in_seconds'].iloc[0]
                    period_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['period_id'].iloc[0]
            
                    if ((time_gk_retired < freekick_mins*60+freekick_secs) & (period_gk_retired == period_id)) | (period_gk_retired < period_id):
                        goalkeeper_id = None
                        gk_name = None

                
                list_events.append(['f' + str(int(game_id)), fixture, attacking_team, defending_team, 't' + str(int(attacking_team_id)), 't' + str(int(defending_team_id)), attacking_team_goals_up_to_freekick_excluded, 
                    defending_team_goals_up_to_freekick_excluded, goal_diff_attack_v_defense,
                    np.where(goal_diff_attack_v_defense > 0, 'winning', np.where(goal_diff_attack_v_defense == 0, 'drawing', 'losing')).tolist(), np.where(freekick_y_start < 50.0, 'Right', 'Left').tolist(),
                    freekick_event_id, period_id, freekick_mins, freekick_secs, freekick_x_start, freekick_y_start, -1, -1, freekick_player_id, freekick_player_name,
                    None, None, 'Other', freekick_event_id, freekick_mins, freekick_secs, freekick_x_start, freekick_y_start, -1, -1,
                    freekick_player_id, freekick_player_name,
                    None, None, outcome_shot,
                    None, None, time_between_stop_and_corner, number_events_in_between, opta_event_id_between, 
                    None, None, None, None, None, None, None, None, None, None, None, np.nan, goalkeeper_id, gk_name])


                list_all_shots.append([freekick_event_id, freekick_event_id, freekick_player_id, freekick_player_name, 't' + str(int(attacking_team_id)), 
                    attacking_team, 'Corner Shot', outcome_shot, shot_body_part, 0, 0, None, None, None, freekick_x_start, freekick_y_start])
                
                continue 

            #we might need to use 2 different criteria to identify the relevant pass depending on whether we are starting from an end zone or not.

            #get window of events happening within, say, 3 seconds since the freekick is taken, just to account free kicks which are not boomed straight away
            time_window = opta_event_data_df[(opta_event_data_df['time_in_seconds'] >= freekick_time_seconds) & (opta_event_data_df['time_in_seconds'] <= freekick_time_seconds + 10.0) & (opta_event_data_df.period_id==period_id)].reset_index(drop = True)
            time_window_passes = time_window[(time_window.type_id.isin([1,2, 13, 14, 15, 16])) & (time_window.team_id==attacking_team_id)].reset_index(drop=True) #includes both passes and shots
            passes_in_window = []
            for ev in time_window_passes.unique_event_id.unique():
                x_start = time_window_passes[time_window_passes.unique_event_id==ev]['x'].iloc[0]
                y_start = time_window_passes[time_window_passes.unique_event_id==ev]['y'].iloc[0]
                min_event = int(time_window_passes[(time_window_passes.unique_event_id==ev)]['min'].iloc[0])
                sec_event = int(time_window_passes[(time_window_passes.unique_event_id==ev)]['sec'].iloc[0])
                if time_window_passes[time_window_passes.unique_event_id==ev].type_id.iloc[0] in [1,2]:
                    x_end = float(time_window_passes[(time_window_passes.unique_event_id==ev) & (time_window_passes.qualifier_id==140)]['value'].iloc[0])
                    y_end = float(time_window_passes[(time_window_passes.unique_event_id==ev) & (time_window_passes.qualifier_id==141)]['value'].iloc[0])
                    long_ball = int((1 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    chipped = int((155 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    launch = int((157 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    ball_delivery = 0
                    if int((223 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist())):
                        ball_delivery = 1 #inswinger
                    if int((224 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist())):
                        ball_delivery = 2 #outswinger
                    if int((225 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist())):
                        ball_delivery = 3 #straight
                    #cross = int((2 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    cross = int(ev in data_crosses.unique_event_id.tolist())
                    overhit_cross = int((345 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    switch_of_play = int((196 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    head_pass = int((3 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    flick_on = int((168 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist()))
                    length_pass = np.round(np.sqrt((x_start/100.0*length_pitch - x_end/100.0*length_pitch)**2 + (y_start/100.0*width_pitch - y_end/100.0*width_pitch)**2), 2)
                    distance_x_percent = x_end - x_start #we keep the difference with the relevant sign to make sure we penalise passages going backwards
                    #qual_26_or_24 = None
                else:
                    x_end = -1
                    y_end = -1
                    long_ball = None
                    chipped = None
                    launch = None
                    ball_delivery = 0
                    cross = None
                    overhit_cross = None
                    switch_of_play = None
                    head_pass = None
                    flick_on = None
                    length_pass = -1
                    distance_x_percent = np.nan
                    #qual_26_or_24 = np.where(26 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist(), 26, 
                    #    np.where(24 in time_window_passes[(time_window_passes.unique_event_id==ev)].qualifier_id.tolist(), 24, None)).tolist()
                type_id_event = time_window_passes[time_window_passes.unique_event_id==ev].type_id.iloc[0]
                event_id_event = int(time_window_passes[time_window_passes.unique_event_id==ev].event_id.iloc[0])
                passes_in_window.append([x_start, y_start, x_end, y_end, chipped, type_id_event, distance_x_percent, length_pass, 999, ev, launch, cross, overhit_cross, switch_of_play, head_pass, flick_on, event_id_event, long_ball, ball_delivery, min_event, sec_event])
            passes_in_window = np.array(passes_in_window)

            if passes_in_window.shape[0] >= 1:
                #as relevant pass we can use the first pass that targets an end zone, assuming it is longer than 5 meters
                index_longest_pass = np.where((((passes_in_window[:,2] >= 83.0) & ((((passes_in_window[:,3] >= 36.8) & (passes_in_window[:,1] < 50.0)) | (passes_in_window[:,-4]==1)) | (((passes_in_window[:,3] <= 63.2) & (passes_in_window[:,1] > 50)) | (passes_in_window[:,-4]==1))))) | (passes_in_window[:,11]==1) | (passes_in_window[:,-3] > 0) | ((passes_in_window[:,-4] == 1) & (passes_in_window[:,2] >= self.edge_box_elliptical(passes_in_window[:,3])) & (passes_in_window[:,0] > 88.5) & (np.abs(passes_in_window[:,1] - 50.0) >= 29.0)) | ((passes_in_window[:,-4] == 1) & (passes_in_window[:,9]==freekick_event_id) & (passes_in_window[:,2] >= self.edge_box_elliptical(passes_in_window[:,3]))))
                if type(index_longest_pass) == tuple:
                    index_longest_pass = index_longest_pass[0]
                if index_longest_pass.shape[0] > 0:
                    index_longest_pass = index_longest_pass[0]

                #index_longest_pass = np.where(passes_in_window[:,6] == max(passes_in_window[:,6]))[0] #here by longest we mean longest along the x axis
                if type(index_longest_pass) == tuple:
                    index_longest_pass = index_longest_pass[0]
                if type(index_longest_pass) == np.int64:
                    index_longest_pass = np.array([index_longest_pass])
                if index_longest_pass is None:
                    index_longest_pass = np.array([])

                if index_longest_pass.shape[0] == 0: #if this occurs, it means there are no (qualifying) passes in the window, hence there can be only shots or non relevant passes
                    index_longest_pass = 0
                    x_start_relevant = -1
                    y_start_relevant = -1
                    x_end_relevant = -1
                    y_end_relevant = -1
                    long_ball_relevant = None
                    chipped_relevant = None
                    launch_relevant = None
                    ball_delivery_relevant = 0
                    cross_relevant = None
                    overhit_cross_relevant = None
                    switch_of_play_relevant = None
                    freekick_player_id_relevant = None
                    freekick_player_name_relevant = None
                    event_id_relevant = None
                    event_id_event_relevant = None 
                    type_id_relevant_event = None 
                    relevant_qualifier_ids = None 
                    relevant_qualifier_values = None 
                    distance_x_percent_relevant_pass = -999
                    length_relevant_pass = -1
                else:
                    index_longest_pass = index_longest_pass[0]
                    x_start_relevant = passes_in_window[index_longest_pass,0]
                    y_start_relevant = passes_in_window[index_longest_pass,1]
                    x_end_relevant = passes_in_window[index_longest_pass,2]
                    y_end_relevant = passes_in_window[index_longest_pass,3]
                    #if index_longest_pass != 0:
                    long_ball_relevant = passes_in_window[index_longest_pass, -4]
                    chipped_relevant = passes_in_window[index_longest_pass,4]
                    launch_relevant = passes_in_window[index_longest_pass,10]
                    ball_delivery_relevant = passes_in_window[index_longest_pass, -3]
                    cross_relevant = passes_in_window[index_longest_pass,11]
                    overhit_cross_relevant = passes_in_window[index_longest_pass,12]
                    switch_of_play_relevant = passes_in_window[index_longest_pass,13]
                    freekick_player_id_relevant = 'p' + str(int(time_window_passes.player_id.loc[time_window_passes.unique_event_id==passes_in_window[index_longest_pass,9]].unique()[0]))
                    freekick_player_name_relevant = player_names_raw.player_name.loc[player_names_raw['@uID'] == freekick_player_id_relevant].iloc[0]
                    event_id_relevant = time_window_passes.unique_event_id.loc[time_window_passes.unique_event_id==passes_in_window[index_longest_pass,9]].unique()[0]
                    event_id_event_relevant = time_window_passes.event_id.loc[time_window_passes.unique_event_id==passes_in_window[index_longest_pass,9]].unique()[0]
                    type_id_relevant_event = time_window_passes.type_id.loc[time_window_passes.unique_event_id==passes_in_window[index_longest_pass,9]].unique()[0]
                    relevant_qualifier_ids = ', '.join([str(int(x)) for x in time_window_passes.qualifier_id.loc[time_window_passes.unique_event_id==event_id_relevant].tolist()])
                    relevant_qualifier_values = ', '.join([str(x) for x in time_window_passes['value'].loc[time_window_passes.unique_event_id==event_id_relevant].tolist()])
                    distance_x_percent_relevant_pass = passes_in_window[index_longest_pass,6]
                    length_relevant_pass = passes_in_window[index_longest_pass,7]
            
            # #define when a corner is taken short
            # if (index_longest_pass == 0) & (x_start_relevant > -1):
            #     if chipped_relevant + cross_relevant + long_ball_relevant == 0:
            #         short_corner = 1
            #     else:
            #         short_corner = 0
            # else:
            #     short_corner = 1


            start_long_ball = passes_in_window[0,-4]
            start_pass_chipped = passes_in_window[0,4]
            start_pass_launch = passes_in_window[0,10]
            start_pass_cross = passes_in_window[0,11]
            start_pass_overhit_cross = passes_in_window[0,12]
            start_pass_switch_of_play = passes_in_window[0,13]
            distance_x_percent_start_pass = passes_in_window[0,6]
            length_start_pass = passes_in_window[0,7]
            ball_delivery_start = passes_in_window[0,-3]

            #define when the corner pass is delivered to the edge of the box. We also put a 'played in behind' indicator as there are cases in which the main pass goes behind the 79 threshold on the X
            played_in_behind = 0
            if (passes_in_window[0,2] < 83) & (passes_in_window[0,3] >= 21.1) & (passes_in_window[0,3] <= 78.9):
                if passes_in_window[0,2] >= self.edge_box_elliptical(passes_in_window[0, 3]):
                    passed_to_edge = 1
                else:
                    passed_to_edge = 0
                    played_in_behind = 1
            else:
                passed_to_edge = 0


            ball_delivery_start = np.where(ball_delivery_start==0, 'Other', np.where(ball_delivery_start==1, 'Inswing', 
                np.where(ball_delivery_start==2, 'Outswing', 'Straight'))).tolist()
            ball_delivery_relevant = np.where(ball_delivery_relevant==0, 'Other', np.where(ball_delivery_relevant==1, 'Inswing', 
                np.where(ball_delivery_relevant==2, 'Outswing', 'Straight'))).tolist()


            #modify ball delivery type for the 'other' category
            if played_in_behind == 1:
                if ball_delivery_start == 'Other':
                    if (start_pass_cross + start_long_ball > 0) | ((passes_in_window[0,3] < 70) & (passes_in_window[0,3] > 30)):
                        ball_delivery_start = 'Straight'
                    else:
                        if x_end_relevant > -1:
                            ball_delivery_start = 'Short With Delivery'
                        else:
                            ball_delivery_start = 'Short'

            else:
                if ball_delivery_start == 'Other':
                    if start_pass_cross == 1:
                        ball_delivery_start = 'Straight'
                    else:
                        if (passes_in_window[0,2] >= 83) & (passes_in_window[0,3] >= 21.1) & (passes_in_window[0,3] <= 78.9):
                            ball_delivery_start = 'Other'
                            if x_end_relevant > -1:
                                if event_id_relevant == freekick_event_id:
                                    ball_delivery_relevant = 'Direct Delivery'
                                else:
                                    ball_delivery_relevant = 'Indirect Delivery'
                            else:
                                ball_delivery_relevant = 'No Delivery'
                        else:
                            if start_long_ball == 0:
                                if (passes_in_window[0,3] < 70) & (passes_in_window[0,3] > 30):
                                    ball_delivery_start = 'Straight'
                                else:
                                    if x_end_relevant > -1:
                                        ball_delivery_start = 'Short With Delivery'
                                    else:
                                        ball_delivery_start = 'Short'
                            else:
                                ball_delivery_start = 'Straight'


            if ball_delivery_start == 'Short':
                ball_delivery_relevant = 'No Delivery'
            else:
                if ball_delivery_start != 'Other':
                    ball_delivery_relevant = ball_delivery_start

            if ball_delivery_start in ['Inswing', 'Outswing', 'Straight']:
                index_longest_pass = 0
                ball_delivery_relevant = ball_delivery_start
                x_start_relevant = passes_in_window[0,0]
                y_start_relevant = passes_in_window[0,1]
                x_end_relevant = passes_in_window[0,2]
                y_end_relevant = passes_in_window[0,3]
                    #if index_longest_pass != 0:
                long_ball_relevant = passes_in_window[0, -4]
                chipped_relevant = passes_in_window[0,4]
                launch_relevant = passes_in_window[0,10]
                cross_relevant = passes_in_window[0,11]
                overhit_cross_relevant = passes_in_window[0,12]
                switch_of_play_relevant = passes_in_window[0,13]
                    
                freekick_player_id_relevant = freekick_player_id
                freekick_player_name_relevant = freekick_player_name
                event_id_relevant = freekick_event_id
                event_id_event_relevant = freekick_event_id_event
                type_id_relevant_event = freekick_type_id
                relevant_qualifier_ids = freekick_qualifier_ids
                relevant_qualifier_values = freekick_qualifier_values
                distance_x_percent_relevant_pass = distance_x_percent_start_pass
                length_relevant_pass = length_start_pass

            #is_ball_delivered = int(distance_x_percent_relevant_pass > -999)

            data_corner_time = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id.isin([6])) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.time_in_seconds <= freekick_time_seconds)]['time_in_seconds'].iloc[-1] #we take most recent event before freekick
            time_between_stop_and_corner = np.round(freekick_time_seconds - data_corner_time, 2)
            data_corner = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds > data_corner_time) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)].reset_index(drop=True)
            if len(data_corner.unique_event_id.unique()) == 0:
                number_events_in_between = 0
                opta_event_id_between = None
                opta_type_id_between = None
            else:
                data_corner_reduced = data_corner[(~data_corner.type_id.isin([6,43,70]))].reset_index(drop=True)
                if data_corner_reduced.shape[0] == 0:
                    number_events_in_between = 0
                    opta_event_id_between = None
                    opta_type_id_between = None   
                else:
                    number_events_in_between = len(data_corner_reduced.unique_event_id.unique())
                    opta_event_id_between = ', '.join([str(x) for x in data_corner_reduced.unique_event_id.unique()])
                    opta_type_id_between = ', '.join([str(int(x)) for x in data_corner_reduced.drop_duplicates(['unique_event_id']).type_id])

            #aerial_duel_is_shot = None 
            #incorporate the shots logic for each set piece event 
            if 1 == 1:
                aerial_duel_is_shot = 0
                qualifying_shots = data_shots[((data_shots.Time_in_Seconds <= 
                    10 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds >= 
                    passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.period_id==
                    period_id) & (data_shots.related_event_team_id==attacking_team_id)) | (((data_shots.Time_in_Seconds >= 
                    passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds <= 
                    60 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.period_id==
                    period_id) & (data_shots.related_event_team_id==attacking_team_id) & (data_shots.from_corner==1)) | (((data_shots.value == 
                    passes_in_window[index_longest_pass,-5]) | (data_shots.value == 
                    passes_in_window[0,-5])) & (data_shots.related_event_team_id == 
                    attacking_team_id) & (data_shots.period_id==period_id) & (data_shots.Time_in_Seconds >= 
                    passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds <= 
                    60 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1])))].sort_values(['Time_in_Seconds']).reset_index(drop=True)
                
                if qualifying_shots.shape[0] > 0:
                    
                    for id_shot in qualifying_shots.unique_event_id.unique():
                        if freekicks_taken[(freekicks_taken.period_id==
                            qualifying_shots[qualifying_shots.unique_event_id==
                            id_shot]['period_id'].iloc[0]) & (freekicks_taken.time_in_seconds <= 
                            qualifying_shots[qualifying_shots.unique_event_id==
                            id_shot]['Time_in_Seconds'].iloc[0]) & (freekicks_taken.team_id==
                            qualifying_shots[qualifying_shots.unique_event_id==id_shot]['related_event_team_id'].iloc[0])]['unique_event_id'].iloc[-1] != freekick_event_id:

                            qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id != id_shot].reset_index(drop=True)

                        else:
                            if opta_event_data_df[(opta_event_data_df.period_id==
                                qualifying_shots[qualifying_shots.unique_event_id==
                                id_shot]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                                id_shot]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                                qualifying_shots[qualifying_shots.unique_event_id==id_shot].related_event_team_id.iloc[0]) & (opta_event_data_df.period_id != 
                                5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds']).shape[0] > 0:

                                if opta_event_data_df[(opta_event_data_df.period_id==
                                    qualifying_shots[qualifying_shots.unique_event_id==
                                    id_shot]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                                    id_shot]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                                    qualifying_shots[qualifying_shots.unique_event_id==id_shot].related_event_team_id.iloc[0]) & (opta_event_data_df.period_id != 
                                    5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds'])['unique_event_id'].iloc[-1] != freekick_event_id:    

                                    qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id != id_shot].reset_index(drop=True)

                    shot_event_ids = ', '.join([str(int(x)) for x in qualifying_shots.unique_event_id.tolist()])
                    shot_player_ids = ', '.join(qualifying_shots.player_id.tolist())
                    for shot_event_id in qualifying_shots.unique_event_id.unique():
                        shot_time = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['Time_in_Seconds'].iloc[0]
                        shot_player_id = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['player_id'].iloc[0]
                    #shot_player_names = ', '.join(qualifying_shots.player_name.tolist())
                        shot_player_name = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['player_name'].iloc[0]

                        shot_team_id = 't' + str(int(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['team_id'].iloc[0]))
                        shot_team_name = np.where(int(shot_team_id.replace('t','')) == home_team_id, 
                            home_team_name, away_team_name).tolist()

                        shot_x = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['x'].iloc[0]
                        shot_y = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['y'].iloc[0]
                    # shot_label = np.where((np.any(qualifying_shots.value == opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0])) & (np.any(qualifying_shots.value != opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0])), 
                    #     'Related & Delayed Shot', np.where(np.all(qualifying_shots.value == opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0]), 
                    #         'Related Shot', 'Delayed Shot')).tolist()
                        shot_label = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                            opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0]) | (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                            opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[0,9]]['event_id'].iloc[0]), 'Related', 
                            'Delayed').tolist()

                        shot_outcome = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['goal'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==0), 
                            'Goal', np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['on_target'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==0), 
                                'On Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['off_target'].iloc[0]==1, 
                                    'Off Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['blocked'].iloc[0]==1, 
                                        'Blocked', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==1, 
                                            'Own Goal', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['chance_missed'].iloc[0]==1,
                                                'Chance Missed', None)))))).tolist()

                        shot_body_part = np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['left_foot'].iloc[0]==1, 
                            'Left Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['right_foot'].iloc[0]==1, 
                                'Right Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['headed'].iloc[0]==1, 
                                    'Header', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['other_body_part'].iloc[0]==1, 
                                        'Other Body Part', None))))

                        data_between_set_piece_and_shot = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds >= freekick_time_seconds) & (opta_event_data_df.time_in_seconds <= shot_time) & (~opta_event_data_df.unique_event_id.isin([freekick_event_id, shot_event_id]))].reset_index(drop=True)
                        if len(data_between_set_piece_and_shot.unique_event_id.unique()) == 0:
                            number_events_in_between_shot = 0
                            opta_event_id_between_shot = None
                            opta_type_id_between_shot = None
                            opta_descr_between_shot = None
                        else:
                            data_between_set_piece_and_shot_reduced = data_between_set_piece_and_shot[(~data_between_set_piece_and_shot.type_id.isin([6,43,70]))].reset_index(drop=True)
                            if data_between_set_piece_and_shot_reduced.shape[0] == 0:
                                number_events_in_between_shot = 0
                                opta_event_id_between_shot = None
                                opta_type_id_between_shot = None  
                                opta_descr_between_shot = None 
                            else:
                                number_events_in_between_shot = len(data_between_set_piece_and_shot_reduced.unique_event_id.unique())
                                opta_event_id_between_shot = ', '.join([str(x) for x in data_between_set_piece_and_shot_reduced.unique_event_id.unique()])
                                opta_type_id_between_shot = ', '.join([str(int(x)) for x in data_between_set_piece_and_shot_reduced.drop_duplicates(['unique_event_id']).type_id])
                                try:
                                    opta_descr_between_shot = ', '.join([event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] for x in opta_type_id_between_shot.split(', ')])
                                except:
                                    opta_descr_between_shot = ''
                                    for x in opta_type_id_between_shot.split(', '):
                                        if int(x) in event_type_id.type_id.tolist():
                                            opta_descr_between_shot = opta_descr_between_shot + event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] + ', '
                                        else:
                                            if int(x) == 83:
                                                opta_descr_between_shot = opta_descr_between_shot + 'Attempted Tackle (post-match only)' + ', '
                                            else:
                                                opta_descr_between_shot = opta_descr_between_shot + 'Event not identified' + ', '
                                if opta_descr_between_shot.endswith(', '):
                                    opta_descr_between_shot = opta_descr_between_shot[:-2]


                        list_all_shots.append([freekick_event_id, shot_event_id, shot_player_id, shot_player_name, shot_team_id, 
                            shot_team_name, shot_label, shot_outcome, shot_body_part, aerial_duel_is_shot, shot_time - freekick_time_seconds, 
                            number_events_in_between_shot, opta_event_id_between_shot, opta_descr_between_shot, 
                            shot_x, shot_y])

                else:
                    shot_event_ids = None 
                    shot_player_ids = None

                where_relevant_pass = np.where(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])[0][-1]
                events_after_relevant_pass = opta_event_data_df[(~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.iloc[:(where_relevant_pass+1)].unique())) & (opta_event_data_df.time_in_seconds >= 
                    passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]) & (opta_event_data_df.time_in_seconds <= 
                    10 + passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]) & (opta_event_data_df.period_id ==
                    period_id) & (~opta_event_data_df.type_id.isin([21,34,35,36,38,39,40,43,53,55,58,63,69,70,71,77,79,83])) & (opta_event_data_df.unique_event_id != 
                    freekick_event_id) & (opta_event_data_df.unique_event_id != passes_in_window[index_longest_pass,9])].reset_index(drop=True)

                first_contact_keypass_assist = None 
                first_contact_aerial = 0
                first_contact_shot = 0
                first_contact_x = np.nan
                first_contact_y = np.nan 
                description = None
                first_contact_event_id = np.nan
                if events_after_relevant_pass.shape[0] > 0:
                    if (events_after_relevant_pass.type_id.iloc[0] in [1,2,3,4,7,10,11,13,14,15,16,8,12,27,30,41,44,45,49,50,51,52,54,56,59,60,61,67,74]) & (passes_in_window[index_longest_pass,5]!=2):                    
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['keypass'].iloc[0] == 1:
                            first_contact_keypass_assist = 'Keypass'
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['assist'].iloc[0] == 1:
                            first_contact_keypass_assist = 'Assist'
                        if (3 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (168 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                            first_contact_aerial = 1
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['type_id'].iloc[0] in [13,14,15,16]:
                            first_contact_shot = 1    
                        if events_after_relevant_pass.type_id.iloc[0] not in [4, 44, 67]:
                            if events_after_relevant_pass.type_id.iloc[0] in event_type_id.type_id.tolist():
                                if (events_after_relevant_pass.type_id.iloc[0] == 12) & (15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                    description = 'Headed Clearance'
                                elif (events_after_relevant_pass.type_id.iloc[0] == 16) & (28 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                    description = 'Own Goal'
                                    if 15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist():
                                        description = 'Headed' + ' ' + description
                                else:
                                    description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0]
                                    if first_contact_aerial == 1:
                                        if len(set([3, 15]).intersection(set(opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()))) > 0:
                                            description = 'Headed' + ' ' + description
                                        elif len(set([168]).intersection(set(opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()))) > 0:
                                            description = 'Flick-on' + ' ' + description
                                    if events_after_relevant_pass.type_id.iloc[0] in [1, 3, 7, 61]:
                                        description = np.where(events_after_relevant_pass.outcome.iloc[0]==1, 'Successful', 'Unsuccessful').tolist() + ' ' + description
                                        if (238 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                            description = description + ' ' + '(fair-play)'
                                    if events_after_relevant_pass.type_id.iloc[0] == 51:
                                        description = description + ' ' + np.where(events_after_relevant_pass.qualifier_id.iloc[0] == 169, 'leading to attempt', 
                                            np.where(events_after_relevant_pass.qualifier_id.iloc[0] == 170, 'leading to goal', '')).tolist()
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_event_id = events_after_relevant_pass.unique_event_id.iloc[0]
                            if np.isnan(events_after_relevant_pass.player_id.iloc[0]):
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                            else:
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass.player_id.iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass.team_id.iloc[0]))
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            first_contact_x = events_after_relevant_pass['x'].iloc[0]
                            first_contact_y = events_after_relevant_pass['y'].iloc[0]
                        else:
                            if events_after_relevant_pass.type_id.iloc[0] == 67:
                                #first_contact_aerial = 1
                                description = '50/50 Won'
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                                first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                    attacking_team, defending_team).tolist()
                                first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]
                            if events_after_relevant_pass.type_id.iloc[0] == 44:
                                first_contact_aerial = 1
                                description = 'Aerial Duel Won'
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                                first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                    attacking_team, defending_team).tolist()
                                first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]
                            if events_after_relevant_pass.type_id.iloc[0] == 4:
                                if (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Handball'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Foul for illegal restart'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (314 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (314 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Foul for shot hitting offside player'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Simulation'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist()  
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]
                                else:
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0] 
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                    description = 'Foul Won'
                                    if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==1].player_id.iloc[0]):
                                        first_contact_player_id = None 
                                        first_contact_player_name = None 
                                    else:
                                        first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1].player_id.iloc[0]))
                                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))         
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                        attacking_team, defending_team).tolist()
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]

                    else:
                        first_contact_type = None 
                        first_contact_event_id = np.nan
                        first_contact_player_id = None 
                        first_contact_player_name = None 
                        first_contact_team_id = None 
                        first_contact_team_name = None 
                        first_contact_aerial = None 
                        first_contact_shot = None
                        first_contact_x = np.nan 
                        first_contact_y = np.nan
                        if (passes_in_window[index_longest_pass,5]==2):
                            first_contact_type = 55
                            description = 'Player Caught Offside'
                            first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                            first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])]['team_id'].iloc[0]))
                            first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            first_contact_x = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==140)]['value'].iloc[0])
                            first_contact_y = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==141)]['value'].iloc[0])
                        if (events_after_relevant_pass.type_id.iloc[0] in [5,6]):
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]
                            description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0] + ' Lost'
                            if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]):
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                            else:
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                            first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]

                    if events_after_relevant_pass.type_id.iloc[0] == 44:
                        for aerial_duel_id in events_after_relevant_pass[events_after_relevant_pass.type_id==44].drop_duplicates(['unique_event_id']).unique_event_id.iloc[:2]:
                        #aerial_duel_ids = ', '.join([str(int(x)) for x in events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[:2].tolist()])
                            successful_player_id_duel = 'p' + str(int(events_after_relevant_pass.player_id.loc[events_after_relevant_pass.outcome==1].iloc[0]))
                            successful_player_name_duel = player_names_raw[player_names_raw['@uID']==successful_player_id_duel]['player_name'].iloc[0]
                            successful_team_id_duel = 't' + str(int(events_after_relevant_pass.team_id.loc[events_after_relevant_pass.outcome==1].iloc[0]))
                            successful_team_name_duel = np.where(successful_team_id_duel == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                            unsuccessful_player_id_duel = 'p' + str(int(events_after_relevant_pass.player_id.loc[events_after_relevant_pass.outcome==0].iloc[0])) 
                            unsuccessful_player_name_duel = player_names_raw[player_names_raw['@uID']==unsuccessful_player_id_duel]['player_name'].iloc[0]                                        
                            unsuccessful_team_id_duel = 't' + str(int(events_after_relevant_pass.team_id.loc[events_after_relevant_pass.outcome==0].iloc[0]))
                            unsuccessful_team_name_duel = np.where(successful_team_id_duel == 't' + str(int(attacking_team_id)), 
                                defending_team, attacking_team).tolist()

                            successful_x = events_after_relevant_pass['x'].loc[events_after_relevant_pass.outcome==1].iloc[0]
                            successful_y = events_after_relevant_pass['y'].loc[events_after_relevant_pass.outcome==1].iloc[0]
                            unsuccessful_x = events_after_relevant_pass['x'].loc[events_after_relevant_pass.outcome==0].iloc[0]
                            unsuccessful_y = events_after_relevant_pass['y'].loc[events_after_relevant_pass.outcome==0].iloc[0]

                            if (shot_player_ids is not None) & (qualifying_shots.shape[0] > 0):
                                if (successful_player_id_duel == shot_player_ids.split(', ')[0]) & (- events_after_relevant_pass.time_in_seconds.iloc[0] + qualifying_shots.Time_in_Seconds.iloc[0] <= 1) & (15 in list(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==int(shot_event_ids.split(', ')[0])].qualifier_id)):
                                    aerial_duel_is_shot = 1

                            if 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==aerial_duel_id]['player_id'].iloc[0])) == successful_player_id_duel:
                                list_aerial_duels.append([freekick_event_id, aerial_duel_id, successful_player_id_duel, 
                                    successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, successful_x, successful_y, 
                                    'Successful', unsuccessful_player_id_duel, 
                                    unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, unsuccessful_x, unsuccessful_y,
                                    aerial_duel_is_shot])
                            else:
                                list_aerial_duels.append([freekick_event_id, aerial_duel_id, unsuccessful_player_id_duel, 
                                    unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, unsuccessful_x, unsuccessful_y,
                                    'Unsuccessful', successful_player_id_duel, 
                                    successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, successful_x, successful_y, 
                                    aerial_duel_is_shot])


                else:
                    first_contact_type = None 
                    first_contact_event_id = np.nan
                    first_contact_player_id = None 
                    first_contact_player_name = None 
                    first_contact_team_id = None 
                    first_contact_team_name = None 
                    first_contact_aerial = None
                    first_contact_shot = None
                    first_contact_x = np.nan
                    first_contact_y = np.nan 
                    if (passes_in_window[index_longest_pass,5]==2):
                        first_contact_type = 55
                        description = 'Player Caught Offside'
                        first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                        first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])]['team_id'].iloc[0]))
                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                        first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                            attacking_team, defending_team).tolist()
                        first_contact_x = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==140)]['value'].iloc[0])
                        first_contact_y = float(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==141)]['value'].iloc[0])


            time_between_relevant_and_first_contact = np.nan
            if events_after_relevant_pass.shape[0] > 0:
                if (first_contact_type is not None):
                    time_between_relevant_and_first_contact = np.round(events_after_relevant_pass.time_in_seconds.iloc[0] - (passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]))
                if first_contact_keypass_assist is not None:
                    description = description + ' (' + first_contact_keypass_assist + ')'

            goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==30) & (opta_event_data_df.team_id==defending_team_id)]['value'].tolist()[0].split(',')[0]))
            gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
            gk_sub_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==18].unique().tolist()))) == 1, 1, 0).tolist()
            gk_sent_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33]))].unique().tolist()))) == 1, 1, 0).tolist()
            retired_gk = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==20].unique().tolist()))) == 1, 1, 0).tolist()

            if (gk_sub_off==1) | (gk_sent_off==1):
                time_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['time_in_seconds']
                period_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['period_id']

                if len(time_gk_in) > 0:
                    if ((time_gk_in.iloc[0] < freekick_mins*60+freekick_secs) & (period_gk_in.iloc[0] == period_id)) | (period_gk_in.iloc[0] < period_id):
                        goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==defending_team_id)]['player_id'].iloc[0])) 
                        gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
                else:
                    goalkeeper_id = None
                    gk_name = None            


            if retired_gk == 1:
                time_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['time_in_seconds'].iloc[0]
                period_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['period_id'].iloc[0]
            
                if ((time_gk_retired < freekick_mins*60+freekick_secs) & (period_gk_retired == period_id)) | (period_gk_retired < period_id):
                    goalkeeper_id = None
                    gk_name = None


            list_events.append(['f' + str(int(game_id)), fixture, attacking_team, defending_team, 't' + str(int(attacking_team_id)), 't' + str(int(defending_team_id)), attacking_team_goals_up_to_freekick_excluded, 
                defending_team_goals_up_to_freekick_excluded, goal_diff_attack_v_defense,
                np.where(goal_diff_attack_v_defense > 0, 'winning', np.where(goal_diff_attack_v_defense == 0, 'drawing', 'losing')).tolist(), np.where(freekick_y_start < 50.0, 'Right', 'Left').tolist(),
                freekick_event_id, period_id, freekick_mins, freekick_secs, freekick_x_start, freekick_y_start, passes_in_window[0,2], passes_in_window[0,3], freekick_player_id, freekick_player_name,
                distance_x_percent_start_pass, length_start_pass, ball_delivery_start, event_id_relevant, passes_in_window[index_longest_pass,-2],
                passes_in_window[index_longest_pass, -1], passes_in_window[index_longest_pass, 0], 
                passes_in_window[index_longest_pass, 1], passes_in_window[index_longest_pass, 2], passes_in_window[index_longest_pass, 3],
                freekick_player_id_relevant, freekick_player_name_relevant,
                distance_x_percent_relevant_pass, length_relevant_pass, ball_delivery_relevant,
                passed_to_edge, played_in_behind, time_between_stop_and_corner, number_events_in_between, opta_event_id_between,
                first_contact_event_id, first_contact_type, description, first_contact_player_id, first_contact_player_name, 
                first_contact_team_id, first_contact_team_name, first_contact_aerial, first_contact_shot, first_contact_x, first_contact_y, time_between_relevant_and_first_contact, goalkeeper_id, gk_name])

        summary_df = pd.DataFrame(list_events, columns = ['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID', 'Goals Scored', 'Goals Conceded',
            'Goal Difference', 'Game State', 'Side',
            'OPTA Event ID', 'period_id', 'min', 'sec', 'X Coordinate', 'Y Coordinate', 'End X Coordinate', 'End Y Coordinate', 'Player ID', 'Player Name',
            '% Distance Along X', 'Length Pass', 'Starting Delivery Type', 'Relevant OPTA Event ID', 
            'Relevant min', 'Relevant sec', 'Relevant X Coordinate', 'Relevant Y Coordinate', 'Relevant End X Coordinate', 'Relevant End Y Coordinate',
            'Relevant Player ID', 'Relevant Player Name',
            'Relevant % Distance Along X', 'Relevant Length Pass', 'Actual Delivery Type',
            'Passed To Edge of Box', 'Played In Behind', 
            'Time Lapsed From Stop And Start', 'Number Of Events Between Stop And Start', 
            'OPTA Event IDs Between Stop And Start', 
            'First Contact Event ID', 'First Contact Type', 'First Contact Explanation', 'First Contact Player ID', 'First Contact Player Name', 
            'First Contact Team ID', 'First Contact Team Name', 'First Contact Aerial', 'First Contact Shot', 'First Contact X Coordinate', 'First Contact Y Coordinate', 'Time Lapsed From Relevant And First Contact', 'Defending Goalkeeper ID', 'Defending Goalkeeper Name'])
        summary_df['Set Piece Type'] = 'Corner'

        summary_df_all_shots = pd.DataFrame(list_all_shots, columns = 
            ['Set Piece OPTA Event ID', 'Shot OPTA ID', 'Shot Player ID', 'Shot Player Name', 
            'Shot Team ID', 'Shot Team Name', 'Shot Occurrence', 'Shot Outcome', 'Shot Body Part', 'Aerial Duel Is Shot', 'Time Lapsed From Set Piece And Shot', 
            'Number Of Events Between Set Piece And Shot', 'OPTA Event IDs Between Set Piece And Shot',
            'Events Explanation Between Set Piece And Shot', 'Shot X Coordinate', 'Shot Y Coordinate'])

        summary_df_aerial_duels = pd.DataFrame(list_aerial_duels, columns = 
            ['Set Piece OPTA Event ID', 'Aerial Duel OPTA ID', 
            'Aerial Duel Player ID', 'Aerial Duel Player Name', 
            'Aerial Duel Team ID', 'Aerial Duel Team Name', 'X Coordinate Player', 'Y Coordinate Player', 'Successful/Unsuccessful',
            'Other Aerial Duel Player ID', 'Other Aerial Duel Player Name', 
            'Other Aerial Duel Team ID', 'Other Aerial Duel Team Name', 'Other X Coordinate Player', 'Other Y Coordinate Player',
            'Aerial Duel Is Shot'])

        if np.any(summary_df_aerial_duels['Aerial Duel Is Shot']==1):
            for set_piece in summary_df_aerial_duels[summary_df_aerial_duels['Aerial Duel Is Shot']==1].drop_duplicates(['Set Piece OPTA Event ID'])['Set Piece OPTA Event ID']:
                summary_df_all_shots.loc[(summary_df_all_shots['Set Piece OPTA Event ID']==set_piece) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Set Piece OPTA Event ID']==set_piece]['Shot OPTA ID'].iloc[0]), 'Aerial Duel Is Shot'] = 1
        
        ##here we make adjustments to add first contact shot info to main set pieces file and shots file
        summary_df_all_shots['First Contact Shot'] = np.where(summary_df_all_shots['Aerial Duel Is Shot']==1, 1, 0)
        if np.any(summary_df_all_shots['First Contact Shot']==1):
            for set_piece in summary_df_all_shots[summary_df_all_shots['First Contact Shot']==1].drop_duplicates(['Set Piece OPTA Event ID'])['Set Piece OPTA Event ID']:
                summary_df.loc[summary_df['OPTA Event ID']==set_piece, 'First Contact Shot'] = 1
        if np.any(summary_df['First Contact Shot']==1):
            for set_piece in summary_df[summary_df['First Contact Shot']==1].drop_duplicates(['OPTA Event ID'])['OPTA Event ID']:
                summary_df_all_shots.loc[(summary_df_all_shots['Set Piece OPTA Event ID']==set_piece) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Set Piece OPTA Event ID']==set_piece]['Shot OPTA ID'].iloc[0]), 'First Contact Shot'] = 1

        
        #here we add first contact coordinates to shots file
        summary_df_all_shots = summary_df_all_shots.merge(summary_df[['OPTA Event ID', 'First Contact X Coordinate', 
            'First Contact Y Coordinate']], how = 'inner', left_on = ['Set Piece OPTA Event ID'], 
            right_on = ['OPTA Event ID']).drop(['OPTA Event ID'], axis = 1)

        return summary_df.reset_index(drop=True), summary_df_all_shots.reset_index(drop=True), summary_df_aerial_duels.reset_index(drop=True)


    def crosses_contextualisation(self, match_info, opta_match_info, df_player_names_raw,
                                  df_opta_events, df_opta_crosses, df_opta_shots,
                                  final_df_set_pieces, final_df_shots) -> (pd.DataFrame, pd.DataFrame,
                                                                           pd.DataFrame, pd.DataFrame, pd.DataFrame):

        """[summary]

        Args:
            match_info (): [description]
            opta_match_info (): [description]
            df_player_names_raw (): [description]
            df_opta_events (): [description]
            df_opta_crosses (): [description]
            df_opta_shots (): [description]
            final_df_set_pieces (): [description]
            final_df_shots (): [description]

        Returns:
            pd.DataFrame: [description]
            pd.DataFrame: [description]
            pd.DataFrame: [description]
            pd.DataFrame: [description]
            pd.DataFrame: [description]

        """

        data_squads = self.get_opta_squad_data()

        length_pitch = match_info['pitchLength']
        width_pitch = match_info['pitchWidth']

        event_type_id = pd.read_excel(os.path.join('scripts_from_fed', 'f24 - ID definitions.xlsx'))

        # Prep opta_event_data_df
        opta_event_data_df = df_opta_events
        opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']

        # Get opta_match_info
        game_id = opta_match_info['match_id']
        away_team_id = opta_match_info['away_team_id']
        away_team_name = opta_match_info['away_team_name']
        home_team_id = opta_match_info['home_team_id']
        home_team_name = opta_match_info['home_team_name']

        # Get df_player_names_raw
        player_names_raw = df_player_names_raw

        # Prep df_opta_crosses
        data_crosses = df_opta_crosses
        data_crosses['player_id'] = ['p' + str(int(x)) for x in data_crosses['player_id']]
        data_crosses['team_id'] = ['t' + str(int(x)) for x in data_crosses['team_id']]
        data_crosses['game_id'] = ['f' + str(int(x)) for x in data_crosses['game_id']]

        # Prep df_opta_shots
        data_shots = df_opta_shots
        data_shots['Time_in_Seconds'] = data_shots['min']*60.0 + data_shots['sec']
        #data_shots['player_id'] = ['p' + str(int(x)) for x in data_shots['player_id']]
        data_shots['team_id'] = ['t' + str(int(x)) for x in data_shots['team_id']]
        data_shots['related_event_team_id'] = ['t' + str(int(x)) for x in data_shots['related_event_team_id']]
        data_shots['game_id'] = ['f' + str(int(x)) for x in data_shots['game_id']]

        # Prep final_df_set_pieces
        data_set_pieces = final_df_set_pieces
        data_set_pieces['Time_in_Seconds'] = data_set_pieces['min']*60.0 + data_set_pieces['sec']
        data_set_pieces['Time_in_Seconds_Relevant'] = data_set_pieces['Relevant min']*60.0 + data_set_pieces['Relevant sec']
        data_set_pieces['2nd Phase Cross OPTA Event ID'] = None

        # Prep final_df_shots which are from set pieces
        data_set_piece_shots = final_df_shots
        data_set_piece_shots['2nd Phase Cross'] = 'No'
        data_set_piece_shots['2nd Phase Cross OPTA Event ID'] = None

        data_shots = data_shots.merge(data_set_piece_shots[['Shot OPTA ID',
                                                            'Shot Occurrence']], how = 'left', left_on = 'unique_event_id', right_on = 'Shot OPTA ID')
        data_shots['from_direct_freekick'] = np.where(data_shots['Shot Occurrence']=='Free Kick Shot', 1, 0)
        data_shots = data_shots.drop(['Shot Occurrence', 'Shot OPTA ID'], axis = 1).reset_index(drop=True)

        data_shots['from_set_piece'] = np.where(data_shots['from_direct_freekick']==1, 0,
                                                data_shots['from_set_piece'])

        data_corners = data_set_pieces[data_set_pieces['Set Piece Type']=='Corner'].reset_index(drop=True)

        data_freekick_shots = data_set_pieces[data_set_pieces['Set Piece Type']=='Free Kick Shot'].reset_index(drop=True)

        data_fl = data_set_pieces[data_set_pieces['Set Piece Type']=='Free Kick'].reset_index(drop=True)

        data_out = data_set_pieces[data_set_pieces['Set Piece Type']=='Free Kick Outside Target Zone'].reset_index(drop=True)

        data_crosses['Cross Type'] = None
        data_crosses['Set Piece OPTA Event ID'] = None
        data_crosses['Goals Scored'] = None
        data_crosses['Goals Conceded'] = None
        data_crosses['Goal Difference'] = None
        data_crosses['Game State'] = None
        data_crosses['Defending Team ID'] = None
        data_crosses['Time Between Relevant Pass And Cross'] = None
        data_crosses['Number Events Between Relevant Pass And Cross'] = None
        data_crosses['Defending Goalkeeper ID'] = None
        data_crosses['Defending Goalkeeper Name'] = None
        data_crosses['Linked 2nd Phase Cross IDs'] = None
        data_crosses['First Contact Type'] = None
        data_crosses['First Contact Explanation'] = None
        data_crosses['First Contact Player ID'] = None
        data_crosses['First Contact Player Name'] = None
        data_crosses['First Contact Team ID'] = None
        data_crosses['First Contact Team Name'] = None
        data_crosses['First Contact Aerial'] = None
        data_crosses['Length Pass'] = None
        data_crosses['% Distance Along X'] = None

        set_pieces_full = data_set_pieces

        shots_full = data_set_piece_shots

        data_crosses['Player Name'] = None
        data_crosses['Cross After A Penalty Kick'] = 'No'
        data_crosses['Penalty Kick OPTA ID'] = np.nan 
        data_crosses['Time Between Penalty Kick And Cross'] = np.nan
        data_crosses['Cross After A Throw-in'] = 'No'
        data_crosses['Throw-in OPTA ID'] = np.nan 
        data_crosses['Time Between Throw-in And Cross'] = np.nan
        data_crosses['Time Lapsed From Cross And First Contact'] = np.nan
       
        list_all_shots = []
        list_aerial_duels = []
        cnt = 0
        start_time = time.process_time()
        
        for game in data_crosses.game_id.unique():
            #shots_to_track = []
            
            for cross_id in data_crosses[data_crosses.game_id==game].unique_event_id:

                sentinel = 0
                
                attacking_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==
                    16) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==
                        cross_id].team_id.iloc[0].replace('t', ''))) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id!=int(data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0].replace('t', ''))) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) | ((opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id].Time_in_Seconds.iloc[0]) & (opta_event_data_df.period_id==data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0])))]['unique_event_id'].unique())    
                defending_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==
                    16) & (opta_event_data_df.team_id!=int(data_crosses[data_crosses.unique_event_id==
                        cross_id].team_id.iloc[0].replace('t', ''))) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0].replace('t', ''))) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) | ((opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id].Time_in_Seconds.iloc[0]) & (opta_event_data_df.period_id==data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0])))]['unique_event_id'].unique())    
                goal_diff_attack_v_defense = attacking_team_goals_up_to_freekick_excluded - defending_team_goals_up_to_freekick_excluded
                game_state = np.sign(goal_diff_attack_v_defense)
                game_state_word = np.where(game_state==-1, 'losing', np.where(game_state==0, 'drawing', 'winning')).tolist()

                #we need to use this for 2nd phase crosses
                # data_last_set_piece = set_pieces_full[(set_pieces_full['game_id'] == 
                #     data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (set_pieces_full['Attacking Team'] == 
                #     data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (set_pieces_full['period_id'] == 
                #     data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (set_pieces_full['Time_in_Seconds'] < 
                #     data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])].sort_values(['Time_in_Seconds']).reset_index(drop=True)


                data_last_set_piece = opta_event_data_df[(opta_event_data_df.period_id==
                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                    int(data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0].replace('t', ''))) & (opta_event_data_df.period_id != 
                    5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds']).reset_index(drop=True)               
                data_last_set_piece = data_last_set_piece.rename(columns = {'unique_event_id': 'OPTA Event ID'})


                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Goals Scored'] = attacking_team_goals_up_to_freekick_excluded
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Goals Conceded'] = defending_team_goals_up_to_freekick_excluded
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Goal Difference'] = goal_diff_attack_v_defense
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Game State'] = game_state_word
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Defending Team ID'] = np.where(home_team_name == 
                    data_crosses[data_crosses.unique_event_id==cross_id]['Defending Team Name'].iloc[0], 
                    't' + str(int(home_team_id)), 't' + str(int(away_team_id))).tolist()

        
                #check for corner
                if np.any(cross_id == data_corners['OPTA Event ID']):
                
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Corner'
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = cross_id

                    #continue 
                    sentinel = 1

                else:
                    if np.any(cross_id == data_corners['Relevant OPTA Event ID']): 
                        if data_corners[data_corners['Relevant OPTA Event ID']==cross_id]['Starting Delivery Type'].iloc[0] in ['Short With Delivery', 'Other']:
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Delivery From Short & Other Corner Situation'
                        else:
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Corner Situation'
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = 0
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = 0
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_corners[data_corners['Relevant OPTA Event ID']==cross_id]['OPTA Event ID'].iloc[0]

                        #continue 
                        sentinel = 1

                    else:
                        #here we need to set up a recursive algorithm
                        if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                            data_shots[data_shots.from_corner==1]['Time_in_Seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
                            cross_id]['Time_in_Seconds'].iloc[0] >= 
                            data_shots[data_shots.from_corner==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                            data_shots[data_shots.from_corner==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                            data_shots[data_shots.from_corner==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
                            data_shots[data_shots.from_corner==1]['related_event_team_id']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                            data_corners['Time_in_Seconds_Relevant'] + 15.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                            data_corners['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                            data_corners['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                            data_corners['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
                            data_corners['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['Time_in_Seconds'] + 7.0) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['Time_in_Seconds']) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['period_id']) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['game_id']) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['Attacking Team Name']))):


                            #we have already ruled out crosses being the actual corner pass or being the relevant pass
                            if data_last_set_piece.shape[0] > 0:
                                if data_corners[(data_corners['game_id'] == 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_corners['Attacking Team'] == 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_corners['period_id'] == 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_corners['Time_in_Seconds'] < 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1] == data_last_set_piece['OPTA Event ID'].iloc[-1]:
                            
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Corner Situation'
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_corners[(data_corners['game_id'] == 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_corners['Attacking Team'] == 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_corners['period_id'] == 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_corners['Time_in_Seconds'] < 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]
                                    
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_corners[data_corners['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_corners[data_corners['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                        cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_corners[data_corners['OPTA Event ID']==
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([6,43,70]))].drop_duplicates(['unique_event_id']).shape[0]

                                    #continue
                                    sentinel = 1



                #check for set pieces
                if np.any(cross_id == data_fl['OPTA Event ID']):

                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Freekick'
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = cross_id

                    #continue
                    sentinel = 1

                else:
                    if np.any(cross_id == data_out['OPTA Event ID']):

                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Freekick No Target'
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = cross_id

                        #continue
                        sentinel = 1

                    else:
                        if np.any(cross_id == data_fl['Relevant OPTA Event ID']): 

                            if data_fl[data_fl['Relevant OPTA Event ID']==cross_id]['Length Pass'].iloc[0] >= 0:
                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Delivery From Freekick Situation'
                            else:
                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Freekick'
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_fl[data_fl['Relevant OPTA Event ID']==cross_id]['OPTA Event ID'].iloc[0]

                            #continue
                            sentinel = 1

                        else:
                            if np.any(cross_id == data_out['Relevant OPTA Event ID']): 

                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Delivery From Freekick Situation No Target'
                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_out[data_out['Relevant OPTA Event ID']==cross_id]['OPTA Event ID'].iloc[0]

                                #continue
                                sentinel = 1

                            else:
                                #here we need to set up a recursive algorithm
                                if np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                    data_shots[data_shots.from_set_piece==1]['Time_in_Seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
                                    cross_id]['Time_in_Seconds'].iloc[0] >= 
                                    data_shots[data_shots.from_set_piece==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                                    data_shots[data_shots.from_set_piece==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                                    data_shots[data_shots.from_set_piece==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
                                    data_shots[data_shots.from_set_piece==1]['related_event_team_id'])):

                                    if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_fl['Time_in_Seconds_Relevant'] + 60.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                        data_fl['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                                        data_fl['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                                        data_fl['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
                                        data_fl['Attacking Team']))):

                                        if data_last_set_piece.shape[0] > 0:
                                            if data_fl[(data_fl['game_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_fl['Attacking Team'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_fl['period_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_fl['Time_in_Seconds'] < 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1] == data_last_set_piece['OPTA Event ID'].iloc[-1]:

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation'
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_fl[(data_fl['game_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_fl['Attacking Team'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_fl['period_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_fl['Time_in_Seconds'] < 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_fl[data_fl['OPTA Event ID']==
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([5,43,70]))].drop_duplicates(['unique_event_id']).shape[0]

                                                #continue
                                                sentinel = 1

                                    if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_out['Time_in_Seconds_Relevant'] + 60.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                        data_out['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                                        data_out['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                                        data_out['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
                                        data_out['Attacking Team']))):

                                        if data_last_set_piece.shape[0] > 0:
                                            if data_out[(data_out['game_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_out['Attacking Team'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_out['period_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_out['Time_in_Seconds'] < 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1] == data_last_set_piece['OPTA Event ID'].iloc[-1]:

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation No Target'
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_out[(data_out['game_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_out['Attacking Team'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_out['period_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_out['Time_in_Seconds'] < 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]     

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_out[data_out['OPTA Event ID']==
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([5,43,70]))].drop_duplicates(['unique_event_id']).shape[0]

                                                #continue  
                                                sentinel = 1                     

                        
                                else:
                                    if (np.any((((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_fl['Time_in_Seconds_Relevant'] + 10.0) & (data_fl['Frontal/Lateral Start']=='Start Frontal')) | ((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_fl['Time_in_Seconds_Relevant'] + 15.0) & (data_fl['Frontal/Lateral Start']=='Start Lateral'))) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                        data_fl['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                                        data_fl['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                                        data_fl['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
                                        data_fl['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['Time_in_Seconds'] + 7.0) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['Time_in_Seconds']) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['period_id']) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['game_id']) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['Attacking Team Name']))):

                                        

                                        #we have already ruled out crosses being the actual corner pass or being the relevant pass
                                        if data_last_set_piece.shape[0] > 0:
                                            if data_fl[(data_fl['game_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_fl['Attacking Team'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_fl['period_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_fl['Time_in_Seconds'] <= 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1] == data_last_set_piece['OPTA Event ID'].iloc[-1]:
                                        
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation'
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_fl[(data_fl['game_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_fl['Attacking Team'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_fl['period_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_fl['Time_in_Seconds'] <= 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_fl[data_fl['OPTA Event ID']==
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([5,43,70]))].drop_duplicates(['unique_event_id']).shape[0]

                                                #continue
                                                sentinel = 1


                                    if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_out['Time_in_Seconds_Relevant'] + 10.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                        data_out['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                                        data_out['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                                        data_out['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
                                        data_out['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                                        data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['Time_in_Seconds'] + 7.0) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['Time_in_Seconds']) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['period_id']) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['game_id']) & 
                                        (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
                                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['Attacking Team Name']))):


                                        #we have already ruled out crosses being the actual corner pass or being the relevant pass
                                        if data_last_set_piece.shape[0] > 0:
                                            if data_out[(data_out['game_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_out['Attacking Team'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_out['period_id'] == 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_out['Time_in_Seconds'] < 
                                                data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1] == data_last_set_piece['OPTA Event ID'].iloc[-1]:

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation No Target'
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_out[(data_out['game_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_out['Attacking Team'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_out['period_id'] == 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_out['Time_in_Seconds'] < 
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                                    cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_out[data_out['OPTA Event ID']==
                                                    data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([5,43,70]))].drop_duplicates(['unique_event_id']).shape[0]

                            
                                                #continue
                                                sentinel = 1


                #check for freekick shots
                if np.any(cross_id == data_freekick_shots['OPTA Event ID']):
            
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = ''
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = 0

                    #continue 
                    sentinel = 1

                else:
                    if np.any(cross_id == data_freekick_shots['Relevant OPTA Event ID']): 
                
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = ''
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = 0

                        #continue
                        sentinel = 1

                    else:
                        #here we need to set up a recursive algorithm
                        if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                            data_shots[data_shots.from_direct_freekick==1]['Time_in_Seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
                            cross_id]['Time_in_Seconds'].iloc[0] >= 
                            data_shots[data_shots.from_direct_freekick==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                            data_shots[data_shots.from_direct_freekick==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                            data_shots[data_shots.from_direct_freekick==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
                            data_shots[data_shots.from_direct_freekick==1]['team_id']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                            data_freekick_shots['Time_in_Seconds_Relevant'] + 15.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                            data_freekick_shots['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                            data_freekick_shots['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                            data_freekick_shots['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
                            data_freekick_shots['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                            data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['Time_in_Seconds'] + 7.0) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From FreeKick Shot Situation']['Time_in_Seconds']) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['period_id']) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['game_id']) & 
                            (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
                                data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['Attacking Team Name']))): 

                    
                            #we have already ruled out crosses being the actual corner pass or being the relevant pass
                            if data_last_set_piece.shape[0] > 0:
                                if data_freekick_shots[(data_freekick_shots['game_id'] == 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_freekick_shots['Attacking Team'] == 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_freekick_shots['period_id'] == 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_freekick_shots['Time_in_Seconds'] < 
                                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1] == data_last_set_piece['OPTA Event ID'].iloc[-1]:

                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Shot Situation'
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = data_freekick_shots[(data_freekick_shots['game_id'] == 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_freekick_shots['Attacking Team'] == 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_freekick_shots['period_id'] == 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_freekick_shots['Time_in_Seconds'] < 
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_freekick_shots[data_freekick_shots['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_freekick_shots[data_freekick_shots['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                        cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_freekick_shots[data_freekick_shots['OPTA Event ID']==
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([5,43,70]))].drop_duplicates(['unique_event_id']).shape[0]

                                    #continue
                                    sentinel = 1


                if sentinel == 0:
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Open Play'
                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = None

                    
                    if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                        data_shots[data_shots.from_penalty==1]['Time_in_Seconds'] + 15.0) & (data_crosses[data_crosses.unique_event_id==
                        cross_id]['Time_in_Seconds'].iloc[0] >= 
                        data_shots[data_shots.from_penalty==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                        data_shots[data_shots.from_penalty==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
                        data_shots[data_shots.from_penalty==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
                        data_shots[data_shots.from_penalty==1]['team_id']))):

                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross After A Penalty Kick'] = 'Yes'
                        penalty_opta_id = data_shots[(data_shots.from_penalty==
                            1) & (data_shots.game_id==data_crosses[data_crosses.unique_event_id==
                            cross_id]['game_id'].iloc[0]) & (data_shots.period_id==data_crosses[data_crosses.unique_event_id==
                            cross_id]['period_id'].iloc[0]) & (data_shots['Time_in_Seconds'] <= data_crosses[data_crosses.unique_event_id==
                            cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots['Time_in_Seconds'] >= data_crosses[data_crosses.unique_event_id==
                            cross_id]['Time_in_Seconds'].iloc[0] - 15.0) & (data_shots['team_id'] == data_crosses[data_crosses.unique_event_id==
                            cross_id]['team_id'].iloc[0])]['unique_event_id'].iloc[-1]
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Penalty Kick OPTA ID'] = penalty_opta_id
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Penalty Kick And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_shots[data_shots.unique_event_id==penalty_opta_id]['Time_in_Seconds'].iloc[0]


                    if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
                        opta_event_data_df[(opta_event_data_df.type_id==1) & (opta_event_data_df.qualifier_id==107)]['time_in_seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
                        cross_id]['Time_in_Seconds'].iloc[0] >= 
                        opta_event_data_df[(opta_event_data_df.type_id==1) & (opta_event_data_df.qualifier_id==107)]['time_in_seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
                        opta_event_data_df[(opta_event_data_df.type_id==1) & (opta_event_data_df.qualifier_id==107)]['period_id']) & (int(data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0].replace('f', ''))==
                        opta_event_data_df[(opta_event_data_df.type_id==1) & (opta_event_data_df.qualifier_id==107)]['game_id']) & (int(data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0].replace('t', ''))==
                        opta_event_data_df[(opta_event_data_df.type_id==1) & (opta_event_data_df.qualifier_id==107)]['team_id']))):

                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross After A Throw-in'] = 'Yes'
                        throw_in_opta_id = opta_event_data_df[(opta_event_data_df.type_id==1) & (opta_event_data_df.qualifier_id==107) & (opta_event_data_df.game_id==
                            int(data_crosses[data_crosses.unique_event_id==
                            cross_id]['game_id'].iloc[0].replace('f',''))) & (opta_event_data_df.period_id==data_crosses[data_crosses.unique_event_id==
                            cross_id]['period_id'].iloc[0]) & (opta_event_data_df['time_in_seconds'] <= data_crosses[data_crosses.unique_event_id==
                            cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['time_in_seconds'] >= data_crosses[data_crosses.unique_event_id==
                            cross_id]['Time_in_Seconds'].iloc[0] - 7.0) & (opta_event_data_df['team_id'] == int(data_crosses[data_crosses.unique_event_id==
                            cross_id]['team_id'].iloc[0].replace('t', '')))]['unique_event_id'].iloc[-1]
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Throw-in OPTA ID'] = throw_in_opta_id
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Throw-in And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - opta_event_data_df[opta_event_data_df.unique_event_id==throw_in_opta_id]['time_in_seconds'].iloc[0]



                aerial_duel_is_shot = 0 
            
                #incorporate the shots logic for each crossing event.  
                if (data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0] == 'Open Play'):
                    qualifying_shots = data_shots[((data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds <= 
                        5 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.period_id==
                        data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) & (data_shots.game_id==game) & (data_shots.related_event_team_id==data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0])) | (((data_shots.value == 
                        opta_event_data_df[opta_event_data_df.unique_event_id==cross_id]['event_id'].iloc[0])) & (data_shots.game_id==game) & (data_shots.related_event_team_id == 
                        data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0]) & (data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds <= 
                        60 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]))].sort_values(['Time_in_Seconds']).reset_index(drop=True)
                    #if qualifying_shots.shape[0] > 0:
                    #    for opta_id_shot in qualifying_shots.unique_event_id.tolist():
                    #        shots_to_track.append(opta_id_shot) #we need this to make sure these do not fall within the set pieces/corner deliveries, such that each qualifying shot is assigned to only one cross
                
                elif ('2nd Phase' in data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0]):
                    qualifying_shots = data_shots[(data_shots.game_id==game) & (((data_shots.Time_in_Seconds <= 
                        5 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.period_id==
                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_shots.related_event_team_id==data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0])) | (((data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds <= 
                        60 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.period_id==
                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_shots.related_event_team_id==data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]) & ((data_shots.from_set_piece==1) | (data_shots.from_corner==1))) | (((data_shots.value == 
                        opta_event_data_df[opta_event_data_df.unique_event_id==cross_id]['event_id'].iloc[0])) & (data_shots.related_event_team_id == 
                        data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]) & (data_shots.period_id==data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds <= 
                        60 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]))))].sort_values(['Time_in_Seconds']).reset_index(drop=True)                

                else:
                    qualifying_shots = data_shots[(data_shots.game_id==game) & (((data_shots.Time_in_Seconds <= 
                        10 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.period_id==
                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_shots.related_event_team_id==data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0])) | (((data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds <= 
                        60 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.period_id==
                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_shots.related_event_team_id==data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]) & ((data_shots.from_set_piece==1) | (data_shots.from_corner==1))) | (((data_shots.value == 
                        opta_event_data_df[opta_event_data_df.unique_event_id==cross_id]['event_id'].iloc[0])) & (data_shots.related_event_team_id == 
                        data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]) & (data_shots.period_id==data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_shots.Time_in_Seconds >= 
                        data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (data_shots.Time_in_Seconds <= 
                        60 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]))))].sort_values(['Time_in_Seconds']).reset_index(drop=True)      

                    if set_pieces_full[(set_pieces_full['OPTA Event ID']==cross_id) | (set_pieces_full['Relevant OPTA Event ID']==cross_id)]['Frontal/Lateral Start'].iloc[0] == 'Start Frontal':
                        qualifying_shots = qualifying_shots[qualifying_shots.from_set_piece==1].reset_index(drop=True)         
                    #we do that to make sure we remove shots which would be referred actually to 2nd phase crosses
                if qualifying_shots.shape[0] > 0:
                    for opta_id_shot in qualifying_shots.unique_event_id.tolist():

                        if (data_crosses[(data_crosses.game_id==game) & (data_crosses.period_id==
                            data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) & (data_crosses.Time_in_Seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                            opta_id_shot]['Time_in_Seconds'].iloc[0]) & (data_crosses.team_id==qualifying_shots[qualifying_shots.unique_event_id==opta_id_shot].related_event_team_id.iloc[0])].sort_values(['Time_in_Seconds'])['unique_event_id'].iloc[-1] != cross_id):

                            qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id!=opta_id_shot].reset_index(drop=True)


                        else:    
                            if (set_pieces_full[(set_pieces_full.game_id==game) & (set_pieces_full.period_id==
                                data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) & (set_pieces_full['Time_in_Seconds'] <= qualifying_shots[qualifying_shots.unique_event_id==
                                opta_id_shot]['Time_in_Seconds'].iloc[0]) & (set_pieces_full['Attacking Team ID']==qualifying_shots[qualifying_shots.unique_event_id==opta_id_shot].related_event_team_id.iloc[0])].sort_values(['Time_in_Seconds']).shape[0] > 0):
                            
                                if (set_pieces_full[(set_pieces_full.game_id==game) & (set_pieces_full.period_id==
                                    data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) & (set_pieces_full['Time_in_Seconds'] <= qualifying_shots[qualifying_shots.unique_event_id==
                                    opta_id_shot]['Time_in_Seconds'].iloc[0]) & (set_pieces_full['Attacking Team ID']==qualifying_shots[qualifying_shots.unique_event_id==opta_id_shot].related_event_team_id.iloc[0])].sort_values(['Time_in_Seconds'])['Time_in_Seconds'].iloc[-1] > data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]):

                                    qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id!=opta_id_shot].reset_index(drop=True)

                            if opta_id_shot in qualifying_shots.unique_event_id.tolist():
                                if (opta_event_data_df[(opta_event_data_df.period_id==
                                    data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                                    opta_id_shot]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                                    int(data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0].replace('t', ''))) & (opta_event_data_df.period_id != 
                                    5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds']).shape[0] > 0):

                                    if (opta_event_data_df[(opta_event_data_df.period_id==
                                        data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds <= qualifying_shots[qualifying_shots.unique_event_id==
                                        opta_id_shot]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df['team_id']==
                                        int(data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0].replace('t', ''))) & (opta_event_data_df.period_id != 
                                        5) & (opta_event_data_df.type_id != 43) & (((opta_event_data_df.type_id.isin([1,2])) & (opta_event_data_df.qualifier_id.isin([5,6,107]))) | ((opta_event_data_df.type_id.isin([13,14,15,16])) & (opta_event_data_df.qualifier_id.isin([9, 26, 263]))))].sort_values(['time_in_seconds'])['time_in_seconds'].iloc[-1] > data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]):    

                                        qualifying_shots = qualifying_shots[qualifying_shots.unique_event_id!=opta_id_shot].reset_index(drop=True)
                    
                    if (qualifying_shots.shape[0] > 0):
                        if ('2nd Phase' in data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0]) & (qualifying_shots.sort_values(['Time_in_Seconds']).from_regular_play.sum() == qualifying_shots.shape[0]):
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Open Play'
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = None
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = None
                            data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = None
                        
                        if (data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0] == 'Open Play') & ((qualifying_shots.sort_values(['Time_in_Seconds']).from_set_piece + qualifying_shots.sort_values(['Time_in_Seconds']).from_corner).sum() > 0):
                            set_piece_event_id = data_last_set_piece['OPTA Event ID'].iloc[-1]
                            if set_piece_event_id in set_pieces_full['OPTA Event ID'].tolist():
                                if 'Free Kick' in set_pieces_full[set_pieces_full['OPTA Event ID']==set_piece_event_id]['Set Piece Type'].iloc[0]: #qualifying_shots.sort_values(['Time_in_Seconds']).from_corner.sum() == 0:
                                    if set_piece_event_id in data_fl['OPTA Event ID'].tolist():
                                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation'
                                    if set_piece_event_id in data_out['OPTA Event ID'].tolist():
                                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation No Target'
                                    if set_piece_event_id in data_freekick_shots['OPTA Event ID'].tolist():
                                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Shot Situation'
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = set_piece_event_id                    
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - set_pieces_full[set_pieces_full['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= set_pieces_full[set_pieces_full['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                        cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != set_pieces_full[set_pieces_full['OPTA Event ID']==
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([5,43,70]))].drop_duplicates(['unique_event_id']).shape[0]
                                if 'Corner' in set_pieces_full[set_pieces_full['OPTA Event ID']==set_piece_event_id]['Set Piece Type'].iloc[0]: #if qualifying_shots.sort_values(['Time_in_Seconds']).from_set_piece.sum() == 0:
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Corner Situation'
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Set Piece OPTA Event ID'] = set_piece_event_id                    
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - set_pieces_full[set_pieces_full['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
                                    data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= set_pieces_full[set_pieces_full['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
                                        cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
                                        cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != set_pieces_full[set_pieces_full['OPTA Event ID']==
                                        data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['Relevant OPTA Event ID'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id) & (~opta_event_data_df.type_id.isin([6,43,70]))].drop_duplicates(['unique_event_id']).shape[0]


                if qualifying_shots.shape[0] == 0:
                    shot_event_ids = None
                    shot_player_ids = None
                    shot_player_names = None
                    #shot_label = 'No Shot' 
                else:
                    shot_event_ids = ', '.join([str(int(x)) for x in qualifying_shots.unique_event_id.tolist()])
                    shot_player_ids = ', '.join(qualifying_shots.player_id.tolist())
                    shot_player_names = ', '.join(qualifying_shots.player_name.tolist())
                    for shot_event_id in qualifying_shots.unique_event_id.unique():
                        shot_time = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['Time_in_Seconds'].iloc[0]
                        shot_player_id = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['player_id'].iloc[0]
                        shot_player_name = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['player_name'].iloc[0]

                        shot_team_id = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['team_id'].iloc[0]
                        shot_team_name = np.where(int(shot_team_id.replace('t','')) == home_team_id, 
                            home_team_name, away_team_name).tolist()
                    # shot_label = np.where((np.any(qualifying_shots.value == opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0])) & (np.any(qualifying_shots.value != opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0])), 
                    #     'Related & Delayed Shot', np.where(np.all(qualifying_shots.value == opta_event_data_df[opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]]['event_id'].iloc[0]), 
                    #         'Related Shot', 'Delayed Shot')).tolist()

                        if data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0] != 'Open Play':
                            shot_label = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                                opta_event_data_df[opta_event_data_df.unique_event_id==cross_id]['event_id'].iloc[0]) | (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                                opta_event_data_df[opta_event_data_df.unique_event_id==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['event_id'].iloc[0]), 'Related', 
                                'Delayed').tolist()
                        else:
                            shot_label = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id].value.iloc[0] == 
                                opta_event_data_df[opta_event_data_df.unique_event_id==cross_id]['event_id'].iloc[0]), 'Related', 
                                'Delayed').tolist()                        

                        shot_outcome = np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['goal'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==0), 
                            'Goal', np.where((qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['on_target'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==0), 
                                'On Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['off_target'].iloc[0]==1, 
                                    'Off Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['blocked'].iloc[0]==1, 
                                        'Blocked', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['own_goal'].iloc[0]==1, 
                                            'Own Goal', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['chance_missed'].iloc[0]==1,
                                                'Chance Missed', None)))))).tolist()

                        shot_body_part = np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['left_foot'].iloc[0]==1, 
                            'Left Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['right_foot'].iloc[0]==1, 
                                'Right Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['headed'].iloc[0]==1, 
                                    'Header', np.where(qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['other_body_part'].iloc[0]==1, 
                                        'Other Body Part', None)))).tolist()

                        shot_x = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['x'].iloc[0]
                        shot_y = qualifying_shots[qualifying_shots.unique_event_id==shot_event_id]['y'].iloc[0]

                        data_between_set_piece_and_shot = opta_event_data_df[(opta_event_data_df.period_id==data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds >= data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.time_in_seconds <= shot_time) & (~opta_event_data_df.unique_event_id.isin([cross_id, shot_event_id]))].reset_index(drop=True)
                        if len(data_between_set_piece_and_shot.unique_event_id.unique()) == 0:
                            number_events_in_between_shot = 0
                            opta_event_id_between_shot = None
                            opta_type_id_between_shot = None
                            opta_descr_between_shot = None
                        else:
                            data_between_set_piece_and_shot_reduced = data_between_set_piece_and_shot[(~data_between_set_piece_and_shot.type_id.isin([5,6,43,70]))].reset_index(drop=True)
                            if data_between_set_piece_and_shot_reduced.shape[0] == 0:
                                number_events_in_between_shot = 0
                                opta_event_id_between_shot = None
                                opta_type_id_between_shot = None  
                                opta_descr_between_shot = None 
                            else:
                                number_events_in_between_shot = len(data_between_set_piece_and_shot_reduced.unique_event_id.unique())
                                opta_event_id_between_shot = ', '.join([str(x) for x in data_between_set_piece_and_shot_reduced.unique_event_id.unique()])
                                opta_type_id_between_shot = ', '.join([str(int(x)) for x in data_between_set_piece_and_shot_reduced.drop_duplicates(['unique_event_id']).type_id])
                                try:
                                    opta_descr_between_shot = ', '.join([event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] for x in opta_type_id_between_shot.split(', ')])
                                except:
                                    opta_descr_between_shot = ''
                                    for x in opta_type_id_between_shot.split(', '):
                                        if int(x) in event_type_id.type_id.tolist():
                                            opta_descr_between_shot = opta_descr_between_shot + event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] + ', '
                                        else:
                                            if int(x) == 83:
                                                opta_descr_between_shot = opta_descr_between_shot + 'Attempted Tackle (post-match only)' +', '
                                            else:
                                                opta_descr_between_shot = opta_descr_between_shot + 'Event not identified' + ', '
                                if opta_descr_between_shot.endswith(', '):
                                    opta_descr_between_shot = opta_descr_between_shot[:-2]


                        list_all_shots.append([cross_id, shot_event_id, shot_player_id, shot_player_name, shot_team_id, 
                            shot_team_name, shot_label, shot_outcome, shot_body_part, aerial_duel_is_shot, shot_time - data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0], 
                            number_events_in_between_shot, opta_event_id_between_shot, opta_descr_between_shot, 
                            shot_x, shot_y])

                where_relevant_pass = np.where(opta_event_data_df.unique_event_id==cross_id)[0][-1]
                events_after_relevant_pass = opta_event_data_df[(~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.iloc[:(where_relevant_pass+1)].unique())) & (opta_event_data_df.time_in_seconds >= 
                    data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.time_in_seconds <= 
                    10 + data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id ==
                    data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) & (~opta_event_data_df.type_id.isin([21,34,35,36,38,39,40,43,53,55,58,63,69,70,71,77,79,83])) & (opta_event_data_df.unique_event_id != cross_id)].reset_index(drop=True)

                if events_after_relevant_pass.shape[0] > 0:  
                    if events_after_relevant_pass.type_id.iloc[0] == 5:
                        events_after_relevant_pass = opta_event_data_df[(opta_event_data_df.unique_event_id.isin(events_after_relevant_pass.unique_event_id)) | ((opta_event_data_df.type_id==
                            events_after_relevant_pass.type_id.iloc[0]) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==
                                events_after_relevant_pass.type_id.iloc[0]) & (opta_event_data_df.qualifier_id==
                                233) & (opta_event_data_df['value']==
                                str(int(events_after_relevant_pass.event_id.iloc[0]))) & (opta_event_data_df.team_id != events_after_relevant_pass.team_id.iloc[0])])))].reset_index(drop=True)

                first_contact_keypass_assist = None 
                first_contact_aerial = 0
                first_contact_shot = 0
                first_contact_x = np.nan
                first_contact_y = np.nan 
                description = None
                first_contact_event_id = np.nan
                if events_after_relevant_pass.shape[0] > 0:
                    if (events_after_relevant_pass.type_id.iloc[0] in [1,2,3,4,7,10,11,13,14,15,16,8,12,27,30,41,44,45,49,50,51,52,54,56,59,60,61,67,74]) & (data_crosses[data_crosses.unique_event_id==cross_id].type_id.iloc[0]!=2):
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['keypass'].iloc[0] == 1:
                            first_contact_keypass_assist = 'Keypass'
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['assist'].iloc[0] == 1:
                            first_contact_keypass_assist = 'Assist'
                        if (3 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (15 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (168 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                            first_contact_aerial = 1
                        if events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['type_id'].iloc[0] in [13,14,15,16]:
                            first_contact_shot = 1
                        if events_after_relevant_pass.type_id.iloc[0] not in [4, 44, 67]:
                            if events_after_relevant_pass.type_id.iloc[0] in event_type_id.type_id.tolist():
                                if (events_after_relevant_pass.type_id.iloc[0] == 12) & (15 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                    description = 'Headed Clearance'
                                elif (events_after_relevant_pass.type_id.iloc[0] == 16) & (28 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                    description = 'Own Goal'
                                    if 15 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist():
                                        description = 'Headed' + ' ' + description
                                else:
                                    description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0]
                                    if first_contact_aerial == 1:
                                        if len(set([3, 15]).intersection(set(opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()))) > 0:
                                            description = 'Headed' + ' ' + description
                                        elif len(set([168]).intersection(set(opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()))) > 0:
                                            description = 'Flick-on' + ' ' + description
                                    if events_after_relevant_pass.type_id.iloc[0] in [1, 3, 7, 61]:
                                        description = np.where(events_after_relevant_pass.outcome.iloc[0]==1, 'Successful', 'Unsuccessful').tolist() + ' ' + description
                                        if (238 in opta_event_data_df[opta_event_data_df.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                            description = description + ' ' + '(fair-play)'
                                    if events_after_relevant_pass.type_id.iloc[0] == 51:
                                        description = description + ' ' + np.where(events_after_relevant_pass.qualifier_id.iloc[0] == 169, 'leading to attempt', 
                                            np.where(events_after_relevant_pass.qualifier_id.iloc[0] == 170, 'leading to goal', '')).tolist()
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_event_id = events_after_relevant_pass.unique_event_id.iloc[0]
                            if np.isnan(events_after_relevant_pass.player_id.iloc[0]):
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                            else:
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass.player_id.iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass.team_id.iloc[0]))
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                home_team_name, away_team_name).tolist()
                            first_contact_x = events_after_relevant_pass['x'].iloc[0]
                            first_contact_y = events_after_relevant_pass['y'].iloc[0]
                        else:
                            if events_after_relevant_pass.type_id.iloc[0] == 67:
                                #first_contact_aerial = 1
                                description = '50/50 Won'
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                                first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                    home_team_name, away_team_name).tolist()
                                first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]
                            if events_after_relevant_pass.type_id.iloc[0] == 44:
                                first_contact_aerial = 1
                                description = 'Aerial Duel Won'
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                                first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                    home_team_name, away_team_name).tolist()
                                first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]
                            if events_after_relevant_pass.type_id.iloc[0] == 4:
                                if (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Handball'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                        home_team_name, away_team_name).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Foul for illegal restart'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                        home_team_name, away_team_name).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]                       
                                elif (314 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (314 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Foul for shot hitting offside player'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                        home_team_name, away_team_name).tolist() 
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]   
                                elif (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0]  
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]                           
                                    description = 'Simulation'
                                    first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                        home_team_name, away_team_name).tolist()  
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]
                                else:
                                    first_contact_type = events_after_relevant_pass.type_id.iloc[0] 
                                    first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==1].unique_event_id.iloc[0]
                                    description = 'Foul Won'
                                    if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==1].player_id.iloc[0]):
                                        first_contact_player_id = None 
                                        first_contact_player_name = None 
                                    else:
                                        first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1].player_id.iloc[0]))
                                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                    first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))         
                                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                        home_team_name, away_team_name).tolist()
                                    first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['x'].iloc[0]
                                    first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==1]['y'].iloc[0]

                    else:
                        first_contact_type = None 
                        first_contact_event_id = np.nan
                        first_contact_player_id = None 
                        first_contact_player_name = None 
                        first_contact_team_id = None 
                        first_contact_team_name = None 
                        first_contact_aerial = None 
                        first_contact_shot = None
                        first_contact_x = np.nan 
                        first_contact_y = np.nan
                        if (data_crosses[data_crosses.unique_event_id==cross_id].type_id.iloc[0]==2):
                            first_contact_type = 55
                            description = 'Player Caught Offside'
                            first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                            first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id)]['team_id'].iloc[0]))
                            first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                home_team_name, away_team_name).tolist()
                            first_contact_x = float(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id) & (opta_event_data_df.qualifier_id==140)]['value'].iloc[0])
                            first_contact_y = float(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id) & (opta_event_data_df.qualifier_id==141)]['value'].iloc[0])
                        if (events_after_relevant_pass.type_id.iloc[0] in [5,6]):
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_event_id = events_after_relevant_pass[events_after_relevant_pass.outcome==0].unique_event_id.iloc[0]
                            description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0]
                            if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]):
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                            else:
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                                home_team_name, away_team_name).tolist()
                            first_contact_x = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['x'].iloc[0]
                            first_contact_y = events_after_relevant_pass[events_after_relevant_pass.outcome==0]['y'].iloc[0]

                    if events_after_relevant_pass.type_id.iloc[0] == 44:
                        for aerial_duel_id in events_after_relevant_pass[events_after_relevant_pass.type_id==44].drop_duplicates(['unique_event_id']).unique_event_id.iloc[:2]:
                        #aerial_duel_ids = ', '.join([str(int(x)) for x in events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[:2].tolist()])
                            successful_player_id_duel = 'p' + str(int(events_after_relevant_pass.player_id.loc[events_after_relevant_pass.outcome==1].iloc[0]))
                            successful_player_name_duel = player_names_raw[player_names_raw['@uID']==successful_player_id_duel]['player_name'].iloc[0]
                            successful_team_id_duel = 't' + str(int(events_after_relevant_pass.team_id.loc[events_after_relevant_pass.outcome==1].iloc[0]))
                            successful_team_name_duel = np.where(successful_team_id_duel == 't' + str(int(home_team_id)), 
                                home_team_name, away_team_name).tolist()
                            unsuccessful_player_id_duel = 'p' + str(int(events_after_relevant_pass.player_id.loc[events_after_relevant_pass.outcome==0].iloc[0])) 
                            unsuccessful_player_name_duel = player_names_raw[player_names_raw['@uID']==unsuccessful_player_id_duel]['player_name'].iloc[0]                                        
                            unsuccessful_team_id_duel = 't' + str(int(events_after_relevant_pass.team_id.loc[events_after_relevant_pass.outcome==0].iloc[0]))
                            unsuccessful_team_name_duel = np.where(successful_team_id_duel == 't' + str(int(home_team_id)), 
                                away_team_name, home_team_name).tolist()

                            successful_x = events_after_relevant_pass['x'].loc[events_after_relevant_pass.outcome==1].iloc[0]
                            successful_y = events_after_relevant_pass['y'].loc[events_after_relevant_pass.outcome==1].iloc[0]
                            unsuccessful_x = events_after_relevant_pass['x'].loc[events_after_relevant_pass.outcome==0].iloc[0]
                            unsuccessful_y = events_after_relevant_pass['y'].loc[events_after_relevant_pass.outcome==0].iloc[0]

                            if (shot_player_ids is not None) & (qualifying_shots.shape[0] > 0):
                                if (successful_player_id_duel == shot_player_ids.split(', ')[0]) & (- events_after_relevant_pass.time_in_seconds.iloc[0] + qualifying_shots.Time_in_Seconds.iloc[0] <= 1) & (15 in list(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==int(shot_event_ids.split(', ')[0])].qualifier_id)):
                                    aerial_duel_is_shot = 1

                            if 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==aerial_duel_id]['player_id'].iloc[0])) == successful_player_id_duel:
                                list_aerial_duels.append([cross_id, aerial_duel_id, successful_player_id_duel, 
                                    successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, successful_x, successful_y, 
                                    'Successful', unsuccessful_player_id_duel, 
                                    unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, unsuccessful_x, unsuccessful_y,
                                    aerial_duel_is_shot])
                            else:
                                list_aerial_duels.append([cross_id, aerial_duel_id, unsuccessful_player_id_duel, 
                                    unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, unsuccessful_x, unsuccessful_y,
                                    'Unsuccessful', successful_player_id_duel, 
                                    successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, successful_x, successful_y, 
                                    aerial_duel_is_shot])


                else:
                    first_contact_type = None 
                    first_contact_event_id = np.nan
                    first_contact_player_id = None 
                    first_contact_player_name = None 
                    first_contact_team_id = None 
                    first_contact_team_name = None 
                    first_contact_aerial = None
                    first_contact_shot = None
                    first_contact_x = np.nan
                    first_contact_y = np.nan 
                    if (data_crosses[data_crosses.unique_event_id==cross_id].type_id.iloc[0]==2):
                        first_contact_type = 55
                        description = 'Player Caught Offside'
                        first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                        first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id)]['team_id'].iloc[0]))
                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                        first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(home_team_id)), 
                            home_team_name, away_team_name).tolist()
                        first_contact_x = float(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id) & (opta_event_data_df.qualifier_id==140)]['value'].iloc[0])
                        first_contact_y = float(opta_event_data_df[(opta_event_data_df.unique_event_id==cross_id) & (opta_event_data_df.qualifier_id==141)]['value'].iloc[0])


                time_between_relevant_and_first_contact = np.nan
                if events_after_relevant_pass.shape[0] > 0:
                    if (first_contact_type is not None):
                        time_between_relevant_and_first_contact = np.round(events_after_relevant_pass.time_in_seconds.iloc[0] - (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]))
                    if first_contact_keypass_assist is not None:
                        description = description + ' (' + first_contact_keypass_assist + ')'

                goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==30) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id]['Defending Team ID'].iloc[0].replace('t','')))]['value'].tolist()[0].split(',')[0]))
                gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
                gk_sub_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==18].unique().tolist()))) == 1, 1, 0).tolist()
                gk_sent_off = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33]))].unique().tolist()))) == 1, 1, 0).tolist()
                retired_gk = np.where(len(set([int(goalkeeper_id.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==20].unique().tolist()))) == 1, 1, 0).tolist()

                if (gk_sub_off==1) | (gk_sent_off==1):
                    time_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id]['Defending Team ID'].iloc[0].replace('t','')))]['time_in_seconds']
                    period_gk_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id]['Defending Team ID'].iloc[0].replace('t','')))]['period_id']

                    if len(time_gk_in) > 0:
                        if ((time_gk_in.iloc[0] < data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (period_gk_in.iloc[0] == data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0])) | (period_gk_in.iloc[0] < data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]):
                            goalkeeper_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.value=='Goalkeeper') & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id]['Defending Team ID'].iloc[0].replace('t','')))]['player_id'].iloc[0])) 
                            gk_name = player_names_raw[player_names_raw['@uID']==goalkeeper_id]['player_name'].iloc[0]
                    else:
                        goalkeeper_id = None
                        gk_name = None            


                if retired_gk == 1:
                    time_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['time_in_seconds'].iloc[0]
                    period_gk_retired = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.player_id==int(goalkeeper_id.replace('p','')))]['period_id'].iloc[0]
            
                    if ((time_gk_retired < data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0]) & (period_gk_retired == data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0])) | (period_gk_retired < data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]):
                        goalkeeper_id = None
                        gk_name = None

                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Player Name'] = player_names_raw[player_names_raw['@uID']==data_crosses[data_crosses.unique_event_id==cross_id].player_id.iloc[0]]['player_name'].iloc[0]
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Defending Goalkeeper ID'] = goalkeeper_id
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Defending Goalkeeper Name'] = gk_name 
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Event ID'] = first_contact_event_id
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Type'] = first_contact_type
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Explanation'] = description
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Player ID'] = first_contact_player_id
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Player Name'] = first_contact_player_name
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Team ID'] = first_contact_team_id
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Team Name'] = first_contact_team_name
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Aerial'] = first_contact_aerial
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Shot'] = first_contact_shot
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact X Coordinate'] = first_contact_x
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'First Contact Y Coordinate'] = first_contact_y
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Length Pass'] = np.round(np.sqrt((data_crosses[data_crosses.unique_event_id==cross_id]['x'].iloc[0]/100.0*length_pitch - data_crosses[data_crosses.unique_event_id==cross_id]['x_end'].iloc[0]/100.0*length_pitch)**2 + (data_crosses[data_crosses.unique_event_id==cross_id]['y'].iloc[0]/100.0*width_pitch - data_crosses[data_crosses.unique_event_id==cross_id]['y_end'].iloc[0]/100.0*width_pitch)**2), 2)
                data_crosses.loc[data_crosses.unique_event_id==cross_id, '% Distance Along X'] = np.round(data_crosses[data_crosses.unique_event_id==cross_id]['x_end'].iloc[0] - data_crosses[data_crosses.unique_event_id==cross_id]['x'].iloc[0], 1)
                data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Lapsed From Cross And First Contact'] = time_between_relevant_and_first_contact

                if '2nd Phase' in data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0]:
                    if sum(data_crosses['Set Piece OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0])==1:
                        data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Linked 2nd Phase Cross IDs'] = str(int(cross_id))
                    else:
                        for cross_to_fill in data_crosses[data_crosses['Set Piece OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['unique_event_id'].tolist():
                            data_crosses.loc[data_crosses.unique_event_id==cross_to_fill, 'Linked 2nd Phase Cross IDs'] = ', '.join([str(int(cross_to_fill)), ', '.join([str(int(x)) for x in data_crosses[(data_crosses['Set Piece OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]) & (data_crosses.unique_event_id!=cross_to_fill)]['unique_event_id'].tolist()])])



                if '2nd Phase' in data_crosses[data_crosses.unique_event_id==cross_id]['Cross Type'].iloc[0]:
                    if np.any(set_pieces_full['OPTA Event ID'] == data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]):
                        if set_pieces_full[set_pieces_full['OPTA Event ID'] == data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['2nd Phase Cross OPTA Event ID'].iloc[0] is None:
                            set_pieces_full.loc[set_pieces_full['OPTA Event ID'] == data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0], '2nd Phase Cross OPTA Event ID'] = str(int(cross_id))
                        else:
                            set_pieces_full.loc[set_pieces_full['OPTA Event ID'] == data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0], '2nd Phase Cross OPTA Event ID'] = ', '.join([set_pieces_full[set_pieces_full['OPTA Event ID'] == data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]]['2nd Phase Cross OPTA Event ID'].iloc[0], str(int(cross_id))])

                        if shot_event_ids is not None:
                            for shot_event_id in shot_event_ids.split(', '):
                                if int(shot_event_id) in shots_full['Shot OPTA ID'].tolist():
                                    shots_full.loc[shots_full['Shot OPTA ID']==int(shot_event_id), '2nd Phase Cross'] = 'Yes'
                                    shots_full.loc[shots_full['Shot OPTA ID']==int(shot_event_id), '2nd Phase Cross OPTA Event ID'] = cross_id
                                    if (aerial_duel_is_shot ==1) & (shot_event_id == shot_event_ids.split(', ')[0]):
                                        shots_full.loc[shots_full['Shot OPTA ID']==int(shot_event_id), 'Aerial Duel Is Shot'] = 1
                                    if shots_full.loc[shots_full['Shot OPTA ID']==int(shot_event_id)]['Shot Occurrence'].iloc[0] != 'Related':
                                        shots_full.loc[shots_full['Shot OPTA ID']==int(shot_event_id), 'Shot Occurrence'] = '2nd Phase Cross'

                                #here potentially there is room to add shots that do not fall within 10 seconds from the set piece relevant pass but happen as a result of a 2nd phase cross
                                else:
                                    set_piece_id = data_crosses[data_crosses.unique_event_id==cross_id]['Set Piece OPTA Event ID'].iloc[0]
                                    shot_time = qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['Time_in_Seconds'].iloc[0]
                                    shot_player_id = shot_player_ids.split(', ')[np.where(np.array(shot_event_ids.split(', ')) == shot_event_id)[0][0]]
                                    shot_player_name = shot_player_names.split(', ')[np.where(np.array(shot_event_ids.split(', ')) == shot_event_id)[0][0]]
                                    shot_team_id = qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['team_id'].iloc[0]
                                    shot_team_name = np.where(int(shot_team_id.replace('t','')) == home_team_id, 
                                        home_team_name, away_team_name).tolist()

                                    shot_occurrence = '2nd Phase Cross' 

                                    shot_outcome = np.where((qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['goal'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['own_goal'].iloc[0]==0), 
                                        'Goal', np.where((qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['on_target'].iloc[0]==1) & (qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['own_goal'].iloc[0]==0), 
                                            'On Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['off_target'].iloc[0]==1, 
                                                'Off Target', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['blocked'].iloc[0]==1, 
                                                    'Blocked', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['own_goal'].iloc[0]==1, 
                                                        'Own Goal', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['chance_missed'].iloc[0]==1,
                                                            'Chance Missed', None)))))).tolist()

                                    shot_body_part = np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['left_foot'].iloc[0]==1, 
                                        'Left Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['right_foot'].iloc[0]==1, 
                                            'Right Foot', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['headed'].iloc[0]==1, 
                                                'Header', np.where(qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['other_body_part'].iloc[0]==1, 
                                                    'Other Body Part', None))))

                                    shot_x = qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['x'].iloc[0]
                                    shot_y = qualifying_shots[qualifying_shots.unique_event_id==int(shot_event_id)]['y'].iloc[0]

                                    data_between_set_piece_and_shot = opta_event_data_df[(opta_event_data_df.period_id==opta_event_data_df[opta_event_data_df.unique_event_id==set_piece_id]['period_id'].iloc[0]) & (opta_event_data_df.time_in_seconds >= opta_event_data_df[opta_event_data_df.unique_event_id==set_piece_id]['time_in_seconds'].iloc[0]) & (opta_event_data_df.time_in_seconds <= shot_time) & (~opta_event_data_df.unique_event_id.isin([set_piece_id, int(shot_event_id)]))].reset_index(drop=True)
                                    if len(data_between_set_piece_and_shot.unique_event_id.unique()) == 0:
                                        number_events_in_between_shot = 0
                                        opta_event_id_between_shot = None
                                        opta_type_id_between_shot = None
                                        opta_descr_between_shot = None
                                    else:
                                        data_between_set_piece_and_shot_reduced = data_between_set_piece_and_shot[(~data_between_set_piece_and_shot.type_id.isin([5,6,43,70]))].reset_index(drop=True)
                                        if data_between_set_piece_and_shot_reduced.shape[0] == 0:
                                            number_events_in_between_shot = 0
                                            opta_event_id_between_shot = None
                                            opta_type_id_between_shot = None  
                                            opta_descr_between_shot = None 
                                        else:
                                            number_events_in_between_shot = len(data_between_set_piece_and_shot_reduced.unique_event_id.unique())
                                            opta_event_id_between_shot = ', '.join([str(x) for x in data_between_set_piece_and_shot_reduced.unique_event_id.unique()])
                                            opta_type_id_between_shot = ', '.join([str(int(x)) for x in data_between_set_piece_and_shot_reduced.drop_duplicates(['unique_event_id']).type_id])
                                            try:
                                                opta_descr_between_shot = ', '.join([event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] for x in opta_type_id_between_shot.split(', ')])
                                            except:
                                                opta_descr_between_shot = ''
                                                for x in opta_type_id_between_shot.split(', '):
                                                    if int(x) in event_type_id.type_id.tolist():
                                                        opta_descr_between_shot = opta_descr_between_shot + event_type_id[event_type_id.type_id==int(x)]['name'].iloc[0] + ', '
                                                    else:
                                                        if int(x) == 83:
                                                            opta_descr_between_shot = opta_descr_between_shot + 'Attempted Tackle (post-match only)' +', '
                                                        else:
                                                            opta_descr_between_shot = opta_descr_between_shot + 'Event not identified' + ', '
                                            if opta_descr_between_shot.endswith(', '):
                                                opta_descr_between_shot = opta_descr_between_shot[:-2]

                                    if shot_player_id in data_squads['@uID'].tolist():
                                        preferred_foot = data_squads[data_squads['@uID']==shot_player_id]['preferred_foot'].iloc[0]
                                    else:
                                        preferred_foot = None

                                    if (aerial_duel_is_shot ==1) & (shot_event_id == shot_event_ids.split(', ')[0]):
                                        shot_is_aerial_duel = 1
                                    else:
                                        shot_is_aerial_duel = 0

                                    shot_is_first_contact = 0 #by definition, a shot from a 2nd phase cross can't be the first contact after a set piece
                                    
                                    first_contact_x_set_piece = set_pieces_full[set_pieces_full['OPTA Event ID']==set_piece_id]['First Contact X Coordinate'].iloc[0]
                                    first_contact_y_set_piece = set_pieces_full[set_pieces_full['OPTA Event ID']==set_piece_id]['First Contact Y Coordinate'].iloc[0]


                                    shots_full = shots_full.append({'Set Piece OPTA Event ID': set_piece_id, 'Shot OPTA ID': int(shot_event_id), 
                                        'Shot Player ID': shot_player_id, 'Shot Player Name': shot_player_name, 
                                        'Shot Team ID': shot_team_id, 'Shot Team Name': shot_team_name, 
                                        'Shot Occurrence': shot_occurrence, 'Shot Outcome': shot_outcome, 'Shot Body Part': shot_body_part,
                                        'Aerial Duel Is Shot': shot_is_aerial_duel, 'Time Lapsed From Set Piece And Shot': shot_time -  opta_event_data_df[opta_event_data_df.unique_event_id==set_piece_id]['time_in_seconds'].iloc[0],  
                                        'Number Of Events Between Set Piece And Shot': number_events_in_between_shot, 
                                        'OPTA Event IDs Between Set Piece And Shot': opta_event_id_between_shot, 
                                        'Events Explanation Between Set Piece And Shot': opta_descr_between_shot,
                                        'Shot X Coordinate': shot_x, 'Shot Y Coordinate': shot_y, 
                                        'First Contact Shot': shot_is_first_contact,
                                        'First Contact X Coordinate': first_contact_x_set_piece, 
                                        'First Contact Y Coordinate': first_contact_y_set_piece, 'Preferred Foot': preferred_foot,
                                        '2nd Phase Cross': 'Yes', '2nd Phase Cross OPTA Event ID': cross_id}, ignore_index = True).reset_index(drop=True)



                
                cnt += 1
                print ('{} crosses have been processed in {}.'.format(cnt, time.process_time() - start_time ))   

        summary_df_all_shots = pd.DataFrame(list_all_shots, columns = 
            ['Cross OPTA Event ID', 'Shot OPTA ID', 'Shot Player ID', 'Shot Player Name', 
            'Shot Team ID', 'Shot Team Name', 'Shot Occurrence', 'Shot Outcome', 'Shot Body Part', 'Aerial Duel Is Shot', 'Time Lapsed From Cross And Shot', 
            'Number Of Events Between Cross And Shot', 'OPTA Event IDs Between Cross And Shot',
            'Events Explanation Between Cross And Shot', 'Shot X Coordinate', 'Shot Y Coordinate'])

        summary_df_aerial_duels = pd.DataFrame(list_aerial_duels, columns = 
            ['Cross OPTA Event ID', 'Aerial Duel OPTA ID', 
            'Aerial Duel Player ID', 'Aerial Duel Player Name', 
            'Aerial Duel Team ID', 'Aerial Duel Team Name', 'X Coordinate Player', 'Y Coordinate Player', 'Successful/Unsuccessful',
            'Other Aerial Duel Player ID', 'Other Aerial Duel Player Name', 
            'Other Aerial Duel Team ID', 'Other Aerial Duel Team Name', 'Other X Coordinate Player', 'Other Y Coordinate Player',
            'Aerial Duel Is Shot'])

        if np.any(summary_df_aerial_duels['Aerial Duel Is Shot']==1):
            for cross in summary_df_aerial_duels[summary_df_aerial_duels['Aerial Duel Is Shot']==1].drop_duplicates(['Cross OPTA Event ID'])['Cross OPTA Event ID']:
                summary_df_all_shots.loc[(summary_df_all_shots['Cross OPTA Event ID']==cross) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Cross OPTA Event ID']==cross]['Shot OPTA ID'].iloc[0]), 'Aerial Duel Is Shot'] = 1

        ##here we make adjustments to add first contact shot info to main set pieces file and shots file
        summary_df_all_shots['First Contact Shot'] = np.where(summary_df_all_shots['Aerial Duel Is Shot']==1, 1, 0)
        if np.any(summary_df_all_shots['First Contact Shot']==1):
            for cross_id in summary_df_all_shots[summary_df_all_shots['First Contact Shot']==1].drop_duplicates(['Cross OPTA Event ID'])['Cross OPTA Event ID']:
                data_crosses.loc[data_crosses['unique_event_id']==cross_id, 'First Contact Shot'] = 1
        if np.any(data_crosses['First Contact Shot']==1):
            for cross_id in data_crosses[data_crosses['First Contact Shot']==1].drop_duplicates(['unique_event_id'])['unique_event_id']:
                if summary_df_all_shots[summary_df_all_shots['Cross OPTA Event ID']==cross_id].shape[0] > 0:
                    summary_df_all_shots.loc[(summary_df_all_shots['Cross OPTA Event ID']==cross_id) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Cross OPTA Event ID']==cross_id]['Shot OPTA ID'].iloc[0]), 'First Contact Shot'] = 1

        
        #here we add first contact coordinates to shots file
        summary_df_all_shots = summary_df_all_shots.merge(data_crosses[['unique_event_id', 'First Contact X Coordinate', 
            'First Contact Y Coordinate']], how = 'inner', left_on = ['Cross OPTA Event ID'], 
            right_on = ['unique_event_id']).drop(['unique_event_id'], axis = 1)

        summary_df_all_shots = summary_df_all_shots.merge(data_squads.drop(['Position', 'Name', 'team_id'], axis =1), how = 'left', left_on = ['Shot Player ID'], 
            right_on = ['@uID']).drop(['@uID'], axis = 1).rename(columns = {'preferred_foot': 'Preferred Foot'}).reset_index(drop=True)#.sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)


        data_crosses['Side'] = np.where(data_crosses['y'] < 50, 'Right', 'Left')
        data_crosses['Early/Lateral/Deep'] = np.where(data_crosses['x'] < 83.0, 'Early', 
            np.where(data_crosses['x'] < 94.2, 'Lateral', 'Deep'))

        data_crosses = data_crosses.drop(['event_id', 'Time_in_Seconds', 
            'pass_situation', 'freekick_taken', 'corner_taken', 'throw_in_taken', 'Our Cross Qualifier'], axis = 1).reset_index(drop=True)
        data_crosses = data_crosses.merge(data_squads.drop(['Position', 'Name', 'team_id'], axis =1), how = 'left', left_on = ['player_id'], 
            right_on = ['@uID']).drop(['@uID'], axis = 1).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)

        data_crosses['Outcome'] = np.where(data_crosses['outcome']==1, 'Successful', 'Unsuccessful')
        data_crosses['Keypass/Assist'] = np.where((data_crosses['keypass']==1) & (data_crosses['assist']==0), 
            'Keypass', np.where(data_crosses['assist']==1, 
                'Assist', None))

        data_crosses = data_crosses.rename(columns = {'fixture': 'Fixture', 'unique_event_id': 'OPTA Event ID', 
            'player_id': 'Player ID', 'team_id': 'Attacking Team ID', 
            'Attacking Team Name': 'Attacking Team', 'Defending Team Name': 'Defending Team', 
            'x': 'X Coordinate', 'y': 'Y Coordinate', 'x_end': 'End X Coordinate', 'y_end': 'End Y Coordinate',
            'preferred_foot': 'Preferred Foot', 'Time Between Relevant Pass And Cross': 'Time Between Set Piece And Cross',
            'Number Events Between Relevant Pass And Cross': 'Number Events Between Set Piece And Cross', 
            'blocked_pass': 'Blocked Pass', 'cutback': 'Cutback', 'out_of_pitch': 'Out Of Pitch', 
            'ending_too_wide': 'Ending Too Wide'})

        data_crosses = data_crosses[['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID',
        'Goals Scored', 'Goals Conceded', 'Goal Difference', 'Game State', 'Side', 'Early/Lateral/Deep',
        'OPTA Event ID', 'period_id', 'min', 'sec', 'X Coordinate', 'Y Coordinate', 'End X Coordinate', 'End Y Coordinate', 'Length Pass', '% Distance Along X',
        'Player ID', 'Player Name', 'Preferred Foot', 'Outcome', 'Keypass/Assist', 'Blocked Pass', 'Cutback', 'OPTA Pull Back Qualifier',
        'Out Of Pitch', 'Ending Too Wide', 'Cross Type', 'Set Piece OPTA Event ID', 'OPTA Cross Qualifier', 'Time Between Set Piece And Cross', 'Number Events Between Set Piece And Cross',
        'Linked 2nd Phase Cross IDs', 'First Contact Event ID', 'First Contact Type', 'First Contact Explanation', 'First Contact Player ID', 'First Contact Player Name',
        'First Contact Team ID', 'First Contact Team Name', 'First Contact Aerial', 'First Contact Shot', 'First Contact X Coordinate', 'First Contact Y Coordinate', 'Time Lapsed From Cross And First Contact',
        'Defending Goalkeeper ID', 'Defending Goalkeeper Name', 'Cross After A Penalty Kick', 'Penalty Kick OPTA ID', 
        'Time Between Penalty Kick And Cross', 'Cross After A Throw-in', 'Throw-in OPTA ID', 
        'Time Between Throw-in And Cross']]

        shots_full = shots_full.sort_values(['Shot OPTA ID']).reset_index(drop=True)

        
        # Add fixtures and game_id Todo Check with Fed at some point

        shots_full['game_id'] = opta_match_info['match_id']
        shots_full['Fixture'] = opta_match_info['fixture']

        summary_df_all_shots['game_id'] = opta_match_info['match_id']
        summary_df_all_shots['Fixture'] = opta_match_info['fixture']

        summary_df_aerial_duels['game_id'] = opta_match_info['match_id']
        summary_df_aerial_duels['Fixture'] = opta_match_info['fixture']


        return data_crosses, set_pieces_full, shots_full, summary_df_all_shots, summary_df_aerial_duels


    @staticmethod
    def edge_box_linear(y, x_tresh=75, y_tresh=50, x0=83):
        """[summary]

        Args:
            y (): [description]
            x_tresh (): [description]
            y_tresh (): [description]
            x0 (): [description]

        Returns:

        """

        if y <= 50:
            y0 = 21.1
        else:
            y0 = 78.9

        b0 = (y_tresh*x0 - x_tresh*y0)/(x0 - x_tresh)
        b1 = (y_tresh - b0)/x_tresh

        x = min([(y - b0)/b1, x0])

        return x

    @staticmethod
    def edge_box_elliptical(y, x_tresh=75, y0=50, x0=83, y_tresh=21.1):
        """[summary]

        Args:
            y (): [description]
            x_tresh (): [description]
            y0 (): [description]
            x0 (): [description]
            y_tresh (): [description]

        Returns:

        """
        rx = np.abs(x0 - x_tresh)
        ry = np.abs(y0 - y_tresh)

        if type(y) == np.ndarray:
            list_x = []
            for value in y:
                if (value >= y_tresh) & (value <= 100 - y_tresh):
                    list_x.append(min((2*ry**2*x0 - np.sqrt(ry**4*(2*x0)**2 - 4*ry**2*((ry*x0)**2 + (rx*y0)**2 - (rx*ry)**2 + (rx*value)**2 - 2*rx**2*y0*value)))/(2*ry**2), 77))
                else:
                    list_x.append(x0)
            x = np.array(list_x)
        else:
            if (y >= y_tresh) & (y <= 100 - y_tresh):
                x = min((2*ry**2*x0 - np.sqrt(ry**4*(2*x0)**2 - 4*ry**2*((ry*x0)**2 + (rx*y0)**2 - (rx*ry)**2 + (rx*y)**2 - 2*rx**2*y0*y)))/(2*ry**2), 77)
            else:
                x = x0

        return x
