####this is the function that takes the necessary file paths related to a specific game to output a summary file for that game
import os 
import pandas as pd 
import xmltodict
import numpy as np 
import time

os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies

from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
from crosses_loop_in_tracking_data import only_open_play_crosses, cross_label, crosses_classification

# path_squads = '\\\ctgshares\\Drogba\\Analysts\\Shared Assets\\srml-8-2019-squads.xml'

# try:
#     with open(path_squads, encoding = 'utf-8') as fd:
#         opta_squads = xmltodict.parse(fd.read())
# except UnicodeDecodeError:
#     with open(path_squads, encoding = 'latin-1') as fd:
#         opta_squads = xmltodict.parse(fd.read())

# list_squads = []
# for i in range(19):
#     data_players_in = pd.DataFrame(opta_squads['SoccerFeed']['SoccerDocument']['Team'][i]['Player'])
#     if '@loan' in data_players_in.columns:
#         data_players_in = data_players_in.drop(['@loan'], axis = 1)
#     data_players_out = pd.DataFrame(opta_squads['SoccerFeed']['SoccerDocument']['PlayerChanges']['Team'][i]['Player'])
#     if '@loan' in data_players_out.columns:
#         data_players_out = data_players_out.drop(['@loan'], axis = 1)
#     data_players = pd.concat([data_players_in, data_players_out]).reset_index(drop=True)
#     data_players['team_id'] = opta_squads['SoccerFeed']['SoccerDocument']['Team'][i]['@uID']
#     list_squads.append(data_players)

# data_squads = pd.concat(list_squads).reset_index(drop=True)
# data_squads['preferred_foot'] = [pd.DataFrame(x)['#text'][pd.DataFrame(x)['@Type']=='preferred_foot'].iloc[0] if 'preferred_foot' in pd.DataFrame(x)['@Type'].tolist() else 'Not Available' for x in data_squads.Stat]
# data_squads = data_squads.drop('Stat', axis = 1).drop_duplicates().reset_index(drop=True)

#path_squads = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Squads & Results\\srml-8-2019-squads.xml'
path_squads = '\\\ctgshares\\Drogba\\Analysts\\Shared Assets\\srml-8-2020-squads.xml'

try:
    with open(path_squads, encoding = 'utf-8') as fd:
        opta_squads = xmltodict.parse(fd.read())
except UnicodeDecodeError:
    with open(path_squads, encoding = 'latin-1') as fd:
        opta_squads = xmltodict.parse(fd.read())

list_squads = []
for i in range(20):
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


#set_pieces_full = data_set_pieces
#shots_full = data_set_piece_shots

def crosses_contextualisation(set_pieces_full, shots_full, data_crosses, data_shots, data_corners, data_fl, data_out, data_freekick_shots, data_squads, season, competition, games_paths):
    import xmltodict
    import json

    data_crosses['Player Name'] = None
    data_crosses['Cross After A Penalty Kick'] = 'No'
    data_crosses['Penalty Kick OPTA ID'] = np.nan 
    data_crosses['Time Between Penalty Kick And Cross'] = np.nan
    data_crosses['Cross After A Throw-in'] = 'No'
    data_crosses['Throw-in OPTA ID'] = np.nan 
    data_crosses['Time Between Throw-in And Cross'] = np.nan
    data_crosses['Time Lapsed From Cross And First Contact'] = np.nan
    event_type_id = pd.read_excel('C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\OPTA\\f24 - ID definitions.xlsx')
    list_all_shots = []
    list_aerial_duels = []
    cnt = 0
    start_time = time.process_time()
    
    for game in data_crosses.game_id.unique():
        #shots_to_track = []
        path_game = [x for x in games_paths if game.replace('f','g') in x][0]
        path_events = [os.path.join(path_game, x) for x in os.listdir(path_game) if 'f24' in x][0]
        path_match_results = [os.path.join(path_game, x) for x in os.listdir(path_game) if 'srml' in x][0]
        if len([os.path.join(path_game, x) for x in os.listdir(path_game) if 'meta' in x.lower()]) > 0:
            path_track_meta = [os.path.join(path_game, x) for x in os.listdir(path_game) if 'meta' in x.lower()][0]
        else:
            path_track_meta = None

        opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
        opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']
        referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results) 

        if path_track_meta is None:
            length_pitch = 105.0
            width_pitch = 68.0
        elif path_track_meta.endswith('json'):
            with open(path_track_meta, 'r') as f:
                datastore = json.load(f)
            length_pitch = float(datastore['pitchLength'])
            width_pitch = float(datastore['pitchWidth'])
        else:
            with open(path_track_meta) as fd:
                opta_meta = xmltodict.parse(fd.read())
            length_pitch = float(opta_meta['TracabMetaData']['match']['@fPitchXSizeMeters'])
            width_pitch = float(opta_meta['TracabMetaData']['match']['@fPitchYSizeMeters'])
        
        for cross_id in data_crosses[data_crosses.game_id==game].unique_event_id:

            sentinel = 0
            
            attacking_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==
                16) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==
                    cross_id].team_id.iloc[0].replace('t', ''))) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id!=int(data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0].replace('t', ''))) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) | (opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id].Time_in_Seconds.iloc[0]))]['unique_event_id'].unique())    
            defending_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==
                16) & (opta_event_data_df.team_id!=int(data_crosses[data_crosses.unique_event_id==
                    cross_id].team_id.iloc[0].replace('t', ''))) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0].replace('t', ''))) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) | (opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id].Time_in_Seconds.iloc[0]))]['unique_event_id'].unique())    
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
            print ('{} crosses have been processed in {} seconds for {} {}.'.format(cnt, time.process_time() - start_time, competition, 
                season))   

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



    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Crosses Output.xlsx'.format(season, competition), engine='xlsxwriter')
    data_crosses.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
    worksheet = writer.sheets['Sheet1']  # pull worksheet object
    for idx, col in enumerate(data_crosses):  # loop through all columns
        series = data_crosses[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()       

    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Set Pieces with 2nd Phase Output.xlsx'.format(season, competition), engine='xlsxwriter')
    set_pieces_full.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
    worksheet = writer.sheets['Sheet1']  # pull worksheet object
    for idx, col in enumerate(set_pieces_full):  # loop through all columns
        series = set_pieces_full[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()       

    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Set Pieces with 2nd Phase Output.xlsx'.format(season, competition), engine='xlsxwriter')
    shots_full.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
    worksheet = writer.sheets['Sheet1']  # pull worksheet object
    for idx, col in enumerate(shots_full):  # loop through all columns
        series = shots_full[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()    

    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Crosses Output.xlsx'.format(season, competition), engine='xlsxwriter')
    summary_df_all_shots.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
    worksheet = writer.sheets['Sheet1']  # pull worksheet object
    for idx, col in enumerate(summary_df_all_shots):  # loop through all columns
        series = summary_df_all_shots[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()       

    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Aerial Duels from Crosses Output.xlsx'.format(season, competition), engine='xlsxwriter')
    summary_df_aerial_duels.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
    worksheet = writer.sheets['Sheet1']  # pull worksheet object
    for idx, col in enumerate(summary_df_aerial_duels):  # loop through all columns
        series = summary_df_aerial_duels[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()    

    return None 







###loop
seasons = ['2020-21']
#seasons = ['2019-20']
#seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
for season in seasons:
    parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}'.format(season)
    #folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x))]
    #folders_to_keep = ['Premier League']
    folders_to_keep = ['Champions League']
    #folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x)) & (x != 'Premier League')]

    for competition in folders_to_keep:
        #if (season == '2019-20') & (competition == 'Premier League'):
        #    continue

        ###import crosses alongside set pieces and corners and start to work out 
        #path_crosses = '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\set pieces classification\\crosses'
        path_crosses = '\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\crosses v4'.format(season, competition)
        data_crosses = pd.concat([pd.read_excel(os.path.join(path_crosses,x)) for x in os.listdir(path_crosses) if '~' not in x], axis = 0).reset_index(drop=True)
        data_crosses['player_id'] = ['p' + str(int(x)) for x in data_crosses['player_id']]
        data_crosses['team_id'] = ['t' + str(int(x)) for x in data_crosses['team_id']]
        data_crosses['game_id'] = ['f' + str(int(x)) for x in data_crosses['game_id']]

        path_shots = '\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\shots'.format(season, competition)
        data_shots = pd.concat([pd.read_excel(os.path.join(path_shots,x)) for x in os.listdir(path_shots)], axis = 0).reset_index(drop=True)
        data_shots['Time_in_Seconds'] = data_shots['min']*60.0 + data_shots['sec']
        #data_shots['player_id'] = ['p' + str(int(x)) for x in data_shots['player_id']]
        data_shots['team_id'] = ['t' + str(int(x)) for x in data_shots['team_id']]
        data_shots['related_event_team_id'] = ['t' + str(int(x)) for x in data_shots['related_event_team_id']]
        data_shots['game_id'] = ['f' + str(int(x)) for x in data_shots['game_id']]

        data_set_pieces = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Set Pieces Output.xlsx'.format(season, competition)).reset_index(drop=True)
        data_set_pieces['Time_in_Seconds'] = data_set_pieces['min']*60.0 + data_set_pieces['sec']
        data_set_pieces['Time_in_Seconds_Relevant'] = data_set_pieces['Relevant min']*60.0 + data_set_pieces['Relevant sec']
        data_set_pieces['2nd Phase Cross OPTA Event ID'] = None

        data_set_piece_shots = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Set Pieces Output.xlsx'.format(season, competition)).reset_index(drop=True) 
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

        #data_crosses['Cross Type'] = None 
        #data_crosses['OPTA Event ID Of Situation'] = None #we leave it empty if the cross results to be open play

        data_crosses['Cross Type'] = None
        data_crosses['Set Piece OPTA Event ID'] = None
        data_crosses['Goals Scored'] = None
        data_crosses['Goals Conceded'] = None 
        data_crosses['Goal Difference'] = None 
        data_crosses['Game State'] = None
        data_crosses['Defending Team ID'] = None
        data_crosses['Time Between Relevant Pass And Cross'] = None
        data_crosses['Number Events Between Relevant And Cross'] = None
        data_crosses['Defending Goalkeeper ID'] = None 
        data_crosses['Defending Goalkeeper Name'] = None 
        data_crosses['Linked 2nd Phase Cross IDs'] = None
        data_crosses['First Contact Event ID'] = None 
        data_crosses['First Contact Type'] = None 
        data_crosses['First Contact Explanation'] = None 
        data_crosses['First Contact Player ID'] = None 
        data_crosses['First Contact Player Name'] = None 
        data_crosses['First Contact Team ID'] = None 
        data_crosses['First Contact Team Name'] = None 
        data_crosses['First Contact Aerial'] = None 
        data_crosses['First Contact Shot'] = None 
        data_crosses['First Contact X Coordinate'] = None 
        data_crosses['First Contact Y Coordinate'] = None 
        data_crosses['Length Pass'] = None 
        data_crosses['% Distance Along X'] = None 



        parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}\\{}'.format(season, competition)
        subfolders_to_keep = [x for x in os.listdir(parent_folder) if ('spectrum' not in x) & ('f73' not in x)]

        #we need to check whether we have already reached the game level folders or we are still at a higher level
        if sum([x.startswith('g') for x in subfolders_to_keep]) > 0: #if true, we already hit the game level folders
            games_paths = [os.path.join(parent_folder, x) for x in subfolders_to_keep]
        else:
            games_paths = [os.path.join(parent_folder, y, 
                x) for y in subfolders_to_keep for x in os.listdir(os.path.join(parent_folder, y))]

        crosses_contextualisation(data_set_pieces, data_set_piece_shots, data_crosses, data_shots, data_corners, data_fl, 
            data_out, data_freekick_shots, data_squads, season, competition, games_paths) 

        #crosses_filtering(data_crosses, data_shots, data_corners, data_fl, 
        #    data_out, data_freekick_shots, data_squads, season, competition, games_paths) 








# def crosses_filtering(data_crosses, data_shots, data_corners, data_fl, data_out, data_freekick_shots, data_squads, season, competition, games_paths):
#     cnt = 0
#     start_time = time.process_time()
#     for game in data_crosses.game_id.unique():
#         for cross_id in data_crosses[data_crosses.game_id==game].unique_event_id:

#             path_game = [x for x in games_paths if game.replace('f','g') in x][0]
#             path_events = [os.path.join(path_game, x) for x in os.listdir(path_game) if 'f24' in x][0]

#             opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
#             opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']
#             #referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results) 
#             attacking_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==
#                 16) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==
#                     cross_id].team_id.iloc[0].replace('t', ''))) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id!=int(data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0].replace('t', ''))) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) | (opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id].Time_in_Seconds.iloc[0]))]['unique_event_id'].unique())    
#             defending_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==
#                 16) & (opta_event_data_df.team_id!=int(data_crosses[data_crosses.unique_event_id==
#                     cross_id].team_id.iloc[0].replace('t', ''))) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==int(data_crosses[data_crosses.unique_event_id==cross_id].team_id.iloc[0].replace('t', ''))) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < data_crosses[data_crosses.unique_event_id==cross_id].period_id.iloc[0]) | (opta_event_data_df.time_in_seconds < data_crosses[data_crosses.unique_event_id==cross_id].Time_in_Seconds.iloc[0]))]['unique_event_id'].unique())    
#             goal_diff_attack_v_defense = attacking_team_goals_up_to_freekick_excluded - defending_team_goals_up_to_freekick_excluded
#             game_state = np.sign(goal_diff_attack_v_defense)
#             game_state_word = np.where(game_state==-1, 'losing', np.where(game_state==0, 'drawing', 'winning')).tolist()

#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Goals Scored'] = attacking_team_goals_up_to_freekick_excluded
#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Goals Conceded'] = defending_team_goals_up_to_freekick_excluded
#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Goal Difference'] = goal_diff_attack_v_defense
#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Game State'] = game_state_word
#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Defending Team ID'] = np.where(home_team_name == 
#                 data_crosses[data_crosses.unique_event_id==cross_id]['Defending Team Name'].iloc[0], 
#                 't' + str(int(home_team_id)), 't' + str(int(away_team_id))).tolist()

    
#             #check for corner
#             if np.any(cross_id == data_corners['OPTA Event ID']):
            
#                 data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Corner'
#                 data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = cross_id

#                 continue 

#             else:
#                 if np.any(cross_id == data_corners['OPTA Event ID Relevant']): 
#                     if data_corners[data_corners['OPTA Event ID Relevant']==cross_id]['Starting Delivery Type'].iloc[0] in ['Short With Delivery', 'Other']:
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Delivery From Short & Other Corner Situation'
#                     else:
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Corner Situation'
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = 0
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = 0
#                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_corners[data_corners['OPTA Event ID Relevant']==cross_id]['OPTA Event ID'].iloc[0]

#                     continue 

#                 else:
#                     #here we need to set up a recursive algorithm
#                     if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                         data_shots[data_shots.from_corner==1]['Time_in_Seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
#                         cross_id]['Time_in_Seconds'].iloc[0] >= 
#                         data_shots[data_shots.from_corner==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                         data_shots[data_shots.from_corner==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                         data_shots[data_shots.from_corner==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
#                         data_shots[data_shots.from_corner==1]['team_id']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                         data_corners['Time_in_Seconds_Relevant'] + 15.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                         data_corners['Time_in_Seconds_Relevant'] + 10.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                         data_corners['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                         data_corners['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                         data_corners['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
#                         data_corners['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['Time_in_Seconds'] + 7.0) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['Time_in_Seconds']) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['period_id']) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['game_id']) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Corner Situation']['Attacking Team Name']))):


#                         #we have already ruled out crosses being the actual corner pass or being the relevant pass
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Corner Situation'
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_corners[(data_corners['game_id'] == 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_corners['Attacking Team'] == 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_corners['period_id'] == 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_corners['Time_in_Seconds'] < 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]
                        
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_corners[data_corners['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_corners[data_corners['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
#                             cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
#                             cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
#                             cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_corners[data_corners['OPTA Event ID']==
#                             data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['OPTA Event ID Relevant'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id)].drop_duplicates(['unique_event_id']).shape[0]

#                         continue



#             #check for set pieces
#             if np.any(cross_id == data_fl['OPTA Event ID']):

#                 data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Freekick'
#                 data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = cross_id

#                 continue

#             else:
#                 if np.any(cross_id == data_out['OPTA Event ID']):

#                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Freekick No Target'
#                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = cross_id

#                     continue

#                 else:
#                     if np.any(cross_id == data_fl['OPTA Event ID Relevant']): 

#                         if data_fl[data_fl['OPTA Event ID Relevant']==cross_id]['Length Pass'].iloc[0] >= 0:
#                             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Delivery From Freekick Situation'
#                         else:
#                             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Crossed Freekick'
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_fl[data_fl['OPTA Event ID Relevant']==cross_id]['OPTA Event ID'].iloc[0]

#                         continue

#                     else:
#                         if np.any(cross_id == data_out['OPTA Event ID Relevant']): 

#                             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Delivery From Freekick Situation No Target'
#                             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_out[data_out['OPTA Event ID Relevant']==cross_id]['OPTA Event ID'].iloc[0]

#                             continue

#                         else:
#                             #here we need to set up a recursive algorithm
#                             if np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                 data_shots[data_shots.from_set_piece==1]['Time_in_Seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
#                                 cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                 data_shots[data_shots.from_set_piece==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                                 data_shots[data_shots.from_set_piece==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                                 data_shots[data_shots.from_set_piece==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
#                                 data_shots[data_shots.from_set_piece==1]['team_id'])):

#                                 if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                     data_fl['Time_in_Seconds_Relevant'] + 60.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                     data_fl['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                                     data_fl['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                                     data_fl['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
#                                     data_fl['Attacking Team']))):
                
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation'
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_fl[(data_fl['game_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_fl['Attacking Team'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_fl['period_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_fl['Time_in_Seconds'] < 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_fl[data_fl['OPTA Event ID']==
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['OPTA Event ID Relevant'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id)].drop_duplicates(['unique_event_id']).shape[0]

#                                     continue

#                                 if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                     data_out['Time_in_Seconds_Relevant'] + 60.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                     data_out['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                                     data_out['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                                     data_out['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
#                                     data_out['Attacking Team']))):

#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation No Target'
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_out[(data_out['game_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_out['Attacking Team'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_out['period_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_out['Time_in_Seconds'] < 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]     

#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_out[data_out['OPTA Event ID']==
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['OPTA Event ID Relevant'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id)].drop_duplicates(['unique_event_id']).shape[0]

#                                     continue                       

                    
#                             else:
#                                 if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                     data_fl['Time_in_Seconds_Relevant'] + 15.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >=
#                                     data_fl['Time_in_Seconds_Relevant'] + 10.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                     data_fl['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                                     data_fl['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                                     data_fl['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
#                                     data_fl['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                     data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['Time_in_Seconds'] + 7.0) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['Time_in_Seconds']) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['period_id']) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['game_id']) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation']['Attacking Team Name']))):


#                                     #we have already ruled out crosses being the actual corner pass or being the relevant pass
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation'
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_fl[(data_fl['game_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_fl['Attacking Team'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_fl['period_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_fl['Time_in_Seconds'] < 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_fl[data_fl['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_fl[data_fl['OPTA Event ID']==
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['OPTA Event ID Relevant'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id)].drop_duplicates(['unique_event_id']).shape[0]

#                                     continue


#                                 if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                     data_out['Time_in_Seconds_Relevant'] + 15.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                     data_out['Time_in_Seconds_Relevant'] + 10.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                     data_out['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                                     data_out['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                                     data_out['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
#                                     data_out['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                                     data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['Time_in_Seconds'] + 7.0) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['Time_in_Seconds']) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['period_id']) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['game_id']) & 
#                                     (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
#                                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Situation No Target']['Attacking Team Name']))):


#                                     #we have already ruled out crosses being the actual corner pass or being the relevant pass
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Situation No Target'
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_out[(data_out['game_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_out['Attacking Team'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_out['period_id'] == 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_out['Time_in_Seconds'] < 
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
#                                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_out[data_out['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
#                                         cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_out[data_out['OPTA Event ID']==
#                                         data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['OPTA Event ID Relevant'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id)].drop_duplicates(['unique_event_id']).shape[0]

                
#                                     continue


#             #check for freekick shots
#             if np.any(cross_id == data_freekick_shots['OPTA Event ID']):
        
#                 data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = ''
#                 data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = 0

#                 continue 

#             else:
#                 if np.any(cross_id == data_freekick_shots['OPTA Event ID Relevant']): 
            
#                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = ''
#                     data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = 0

#                     continue

#                 else:
#                     #here we need to set up a recursive algorithm
#                     if (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                         data_shots[data_shots.from_direct_freekick==1]['Time_in_Seconds'] + 7.0) & (data_crosses[data_crosses.unique_event_id==
#                         cross_id]['Time_in_Seconds'].iloc[0] >= 
#                         data_shots[data_shots.from_direct_freekick==1]['Time_in_Seconds']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                         data_shots[data_shots.from_direct_freekick==1]['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                         data_shots[data_shots.from_direct_freekick==1]['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['team_id'].iloc[0]==
#                         data_shots[data_shots.from_direct_freekick==1]['team_id']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                         data_freekick_shots['Time_in_Seconds_Relevant'] + 15.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                         data_freekick_shots['Time_in_Seconds_Relevant'] + 10.0) & (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                         data_freekick_shots['Time_in_Seconds_Relevant']) & (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]==
#                         data_freekick_shots['period_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]==
#                         data_freekick_shots['game_id']) & (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]==
#                         data_freekick_shots['Attacking Team']))) | (np.any((data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] <= 
#                         data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['Time_in_Seconds'] + 7.0) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] >= 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From FreeKick Shot Situation']['Time_in_Seconds']) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0] == 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['period_id']) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0] == 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['game_id']) & 
#                         (data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0] == 
#                             data_crosses[data_crosses['Cross Type'] == '2nd Phase From Freekick Shot Situation']['Attacking Team Name']))): 

                
#                         #we have already ruled out crosses being the actual corner pass or being the relevant pass
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = '2nd Phase From Freekick Shot Situation'
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = data_freekick_shots[(data_freekick_shots['game_id'] == 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['game_id'].iloc[0]) & (data_freekick_shots['Attacking Team'] == 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['Attacking Team Name'].iloc[0]) & (data_freekick_shots['period_id'] == 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['period_id'].iloc[0]) & (data_freekick_shots['Time_in_Seconds'] < 
#                             data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0])]['OPTA Event ID'].iloc[-1]

#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Time Between Relevant Pass And Cross'] = data_crosses[data_crosses.unique_event_id==cross_id]['Time_in_Seconds'].iloc[0] - data_freekick_shots[data_freekick_shots['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]
#                         data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Number Events Between Relevant Pass And Cross'] = opta_event_data_df[(opta_event_data_df.time_in_seconds >= data_freekick_shots[data_freekick_shots['OPTA Event ID']==data_crosses[data_crosses.unique_event_id==
#                             cross_id]['OPTA Event ID Of Situation'].iloc[0]]['Time_in_Seconds_Relevant'].iloc[0]) & (opta_event_data_df.time_in_seconds <= data_crosses[data_crosses.unique_event_id==
#                             cross_id]['Time_in_Seconds'].iloc[0]) & (opta_event_data_df.period_id == data_crosses[data_crosses.unique_event_id==
#                             cross_id]['period_id'].iloc[0]) & (opta_event_data_df.unique_event_id != data_freekick_shots[data_freekick_shots['OPTA Event ID']==
#                             data_crosses[data_crosses.unique_event_id==cross_id]['OPTA Event ID Of Situation'].iloc[0]]['OPTA Event ID Relevant'].iloc[0]) & (opta_event_data_df.unique_event_id != cross_id)].drop_duplicates(['unique_event_id']).shape[0]

#                         continue


    
#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'Cross Type'] = 'Open Play'
#             data_crosses.loc[data_crosses.unique_event_id==cross_id, 'OPTA Event ID Of Situation'] = None



#             cnt += 1
#             print ('{} open play crosses have been detected in {} seconds for {} {}.'.format(cnt, time.process_time() - start_time, competition, 
#                 season))   

#     data_crosses['Side'] = np.where(data_crosses['y'] < 50, 'Right', 'Left')
#     data_crosses['Early/Lateral/Deep'] = np.where(data_crosses['x'] < 83.0, 'Early', 
#         np.where(data_crosses['x'] < 94.2, 'Lateral', 'Deep'))

#     data_crosses = data_crosses.drop(['event_id', 'Time_in_Seconds', 
#         'pass_situation', 'freekick_taken', 'corner_taken', 'throw_in_taken', 'Our Cross Qualifier'], axis = 1).reset_index(drop=True)
#     data_crosses = data_crosses.merge(data_squads.drop(['Position'], axis =1), how = 'left', left_on = ['team_id', 'player_id'], 
#         right_on = ['team_id', '@uID']).drop(['@uID'], axis = 1).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True]).reset_index(drop=True)

#     data_crosses = data_crosses.rename(columns = {'fixture': 'Fixture', 'unique_event_id': 'OPTA Event ID', 
#         'player_id': 'Player ID', 'Name': 'Player Name', 'team_id': 'Attacking Team ID', 
#         'Attacking Team Name': 'Attacking Team', 'Defending Team Name': 'Defending Team'})

#     data_crosses = data_crosses[['game_id', 'Fixture', 'Attacking Team', 'Defending Team', 'Attacking Team ID', 'Defending Team ID',
#     'Goals Scored', 'Goals Conceded', 'Goal Difference', 'Game State', 'Side', 'Early/Lateral/Deep',
#     'OPTA Event ID', 'period_id', 'min', 'sec', 'Player ID', 'Player Name', 'preferred_foot', 'outcome', 'keypass', 'assist', 'blocked_pass', 'cutback', 
#     'out_of_pitch', 'ending_too_wide', 'Cross Type', 'OPTA Event ID Of Situation', 'OPTA Cross Qualifier', 'Time Between Relevant Pass And Cross', 'Number Events Between Relevant Pass And Cross']]

#     data_crosses = data_crosses[(data_crosses['Time Between Relevant Pass And Cross'] >= 10) & (data_crosses['Time Between Relevant Pass And Cross'].notnull())].reset_index(drop=True)


#     writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces\\list crosses to inspect.xlsx'.format(season, competition), engine='xlsxwriter')
#     data_crosses.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
#     worksheet = writer.sheets['Sheet1']  # pull worksheet object
#     for idx, col in enumerate(data_crosses):  # loop through all columns
#         series = data_crosses[col]
#         max_len = max((
#             series.astype(str).map(len).max(),  # len of largest item
#             len(str(series.name))  # len of column name/header
#             )) + 1  # adding a little extra space
#         worksheet.set_column(idx, idx, max_len)  # set column width
#     writer.save()       

#     return None