import pandas as pd
import numpy as np

import time
import logging
import datetime
import os
import xmltodict
import json

class OPTATransformer:
    config = None
    logger = None
    data_source = None
    match_date = None
    formations = '{"formations_id":{"0":2,"1":3,"2":4,"3":5,"4":6,"5":7,"6":8,"7":9,"8":10,"9":11,"10":12,"11":13,"12":14,"13":15,"14":16,"15":17,"16":18,"17":19,"18":20,"19":21,"20":22,"21":23,"22":24,"23":25},"formation":{"0":442,"1":41212,"2":433,"3":451,"4":4411,"5":4141,"6":4231,"7":4321,"8":532,"9":541,"10":352,"11":343,"12":31312,"13":4222,"14":3511,"15":3421,"16":3412,"17":3142,"18":343,"19":4132,"20":4240,"21":4312,"22":3241,"23":3331}}'

    def __init__(self):
        #self.config = config
        self.logger = logging.getLogger('{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))

    def opta_core_stats(self, df_opta_events, opta_match_info, df_player_names_raw, players_df_lineup, match_info):
        #season_id, competition_id, add t to team id and opp team_id, add f to game_id, add p to player_id, opp team formation id, opp team formation

        formations = pd.read_json(self.formations)

        #let's consider the players eligible for inclusion - i.e. all the ones involved in the game
        home_players_starters = [int(x) for x in df_opta_events.value.loc[(df_opta_events.qualifier_id==30) & (df_opta_events.type_id==34) & (df_opta_events.team_id==opta_match_info['home_team_id'])].iloc[0].split(',')[:11]]
        home_players_subs_on = [int(x) for x in df_opta_events.player_id.loc[(df_opta_events.type_id==19) & (df_opta_events.team_id==opta_match_info['home_team_id'])].unique().tolist()]
        if len(home_players_subs_on) == 0:
            home_players_subs_on = [-999]
        away_players_starters = [int(x) for x in df_opta_events.value.loc[(df_opta_events.qualifier_id==30) & (df_opta_events.type_id==34) & (df_opta_events.team_id==opta_match_info['away_team_id'])].iloc[0].split(',')[:11]]
        away_players_subs_on = [int(x) for x in df_opta_events.player_id.loc[(df_opta_events.type_id==19) & (df_opta_events.team_id==opta_match_info['away_team_id'])].unique().tolist()]
        if len(away_players_subs_on) == 0:
            away_players_subs_on = [-999]

        players_to_consider = [x for y in [home_players_starters, home_players_subs_on, away_players_starters, away_players_subs_on] for x in y if x != -999]

        #here teh actual loop starts
        summary_list = []

        for player in players_to_consider:
            player_id = player
            player_name = df_player_names_raw.full_name.loc[df_player_names_raw['player_id'] == (player_id)].iloc[0]
            player_team_id = np.where(len(set([player_id]).intersection([x for y in [home_players_starters, home_players_subs_on] for x in y])) == 1, opta_match_info['home_team_id'], opta_match_info['away_team_id']).tolist()
            player_team_name = np.where(len(set([player_id]).intersection([x for y in [home_players_starters, home_players_subs_on] for x in y])) == 1, opta_match_info['home_team_name'], opta_match_info['away_team_name']).tolist()
            opp_team_id = np.where(player_team_id == opta_match_info['home_team_id'], opta_match_info['away_team_id'], opta_match_info['home_team_id']).tolist()
            opp_team_name = np.where(player_team_name == opta_match_info['home_team_name'], opta_match_info['away_team_name'], opta_match_info['home_team_name']).tolist()
            home_away = np.where(player_team_id == opta_match_info['home_team_id'], 'Home', 'Away').tolist()

            if match_info['home_formation'] is None:
                home_formation = formations.formation.loc[(formations.formations_id==int(df_opta_events['value'].loc[(df_opta_events.type_id==34) & (df_opta_events.qualifier_id==130) & (df_opta_events.team_id==opta_match_info['home_team_id'])].iloc[0]))].iloc[0]
            else:
                home_formation = match_info['home_formation']

            if match_info['away_formation'] is None:
                away_formation = formations.formation.loc[(formations.formations_id==int(df_opta_events['value'].loc[(df_opta_events.type_id==34) & (df_opta_events.qualifier_id==130) & (df_opta_events.team_id==opta_match_info['away_team_id'])].iloc[0]))].iloc[0]
            else:
                away_formation = match_info['away_formation']

            team_formation = np.where(player_team_id == opta_match_info['home_team_id'], home_formation, away_formation).tolist()
            opp_team_formation = np.where(player_team_id == opta_match_info['home_team_id'], away_formation, home_formation).tolist()
            team_formation_id = formations.formations_id.loc[formations.formation == team_formation].iloc[0]
            opp_team_formation_id = formations.formations_id.loc[formations.formation == opp_team_formation].iloc[0]
            if '@Formation_Place' not in list(players_df_lineup):
                players_initial = np.array([int(x) for x in df_opta_events.value.loc[(df_opta_events.type_id==34) & (df_opta_events.qualifier_id==30) & (df_opta_events.team_id==player_team_id)].iloc[0].split(',')])
                players_position_ids = [int(x) for x in df_opta_events.value.loc[(df_opta_events.type_id==34) & (df_opta_events.qualifier_id==131) & (df_opta_events.team_id==player_team_id)].iloc[0].split(',')]
                player_position_id = players_position_ids[np.where(players_initial==player_id)[0][0]]
            else:
                player_position_id = int(players_df_lineup['@Formation_Place'].loc[players_df_lineup['@PlayerRef'] == 'p' + str(player_id)].iloc[0])
            player_start = np.where(len(set([player_id]).intersection([x for y in [home_players_starters, away_players_starters] for x in y])) == 1, 1, 0).tolist()
            player_sub_on = np.where(len(set([player_id]).intersection([x for y in [home_players_subs_on, away_players_subs_on] for x in y])) == 1, 1, 0).tolist()
            player_sub_off = np.where(len(set([player_id]).intersection(set(df_opta_events.player_id.loc[df_opta_events.type_id==18].unique().tolist()))) == 1, 1, 0).tolist()

            #aside to deal with position id and any change of formation when a sub on is considered
            if player_sub_on == 1:
                period_id_sub = int(df_opta_events['period_id'].loc[
                                        (df_opta_events.type_id == 19) & (df_opta_events.qualifier_id == 44) & (
                                                df_opta_events.player_id == player_id)].iloc[0])
                if df_opta_events.value.loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==44) & (df_opta_events.player_id==player_id)].iloc[0] == 'Goalkeeper':
                    player_position_id = 1
                    time_sub = float(df_opta_events['min'].loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==44) & (df_opta_events.player_id==player_id)].iloc[0])*60.0 + float(df_opta_events['sec'].loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==44) & (df_opta_events.player_id==player_id)].iloc[0])
                else:
                    #if 292 in df_opta_events.qualifier_id.loc[(df_opta_events.type_id==19) & (df_opta_events.player_id==player_id)].tolist():
                    #    player_position_id = int(df_opta_events.value.loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==292) & (df_opta_events.player_id==player_id)].iloc[0])
                    #    time_sub = float(df_opta_events['min'].loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==292) & (df_opta_events.player_id==player_id)].iloc[0])*60.0 + float(df_opta_events['sec'].loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==292) & (df_opta_events.player_id==player_id)].iloc[0])
                    #else:
                    player_position_id = int(df_opta_events.value.loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==145) & (df_opta_events.player_id==player_id)].iloc[0])
                    time_sub = float(df_opta_events['min'].loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==145) & (df_opta_events.player_id==player_id)].iloc[0])*60.0 + float(df_opta_events['sec'].loc[(df_opta_events.type_id==19) & (df_opta_events.qualifier_id==145) & (df_opta_events.player_id==player_id)].iloc[0])

                if np.any(df_opta_events.type_id[(df_opta_events.team_id==player_team_id)].unique() == 40):
                    time_formation_difference_is_eligible = []
                    for change_id in df_opta_events.unique_event_id.loc[(df_opta_events.team_id==player_team_id) & (df_opta_events.type_id==40)].unique().tolist():
                        period_id_formation_change = int(
                            df_opta_events['period_id'].loc[df_opta_events.unique_event_id ==
                                                            change_id].unique().tolist()[0])
                        time_difference = float(df_opta_events['min'].loc[df_opta_events.unique_event_id ==
                                                                          change_id].unique().tolist()[
                                                    0]) * 60.0 + float(
                            df_opta_events['sec'].loc[df_opta_events.unique_event_id ==
                                                      change_id].unique().tolist()[0]) - time_sub

                        if (time_difference <= 60.0) & (period_id_formation_change <= period_id_sub):
                            time_formation_difference_is_eligible.append(1)
                            if time_difference >= 0.0:
                                player_position_id = int(1 + np.where(np.array([int(x) for x in df_opta_events[(df_opta_events.unique_event_id==change_id) & (df_opta_events.qualifier_id==30)].value.iloc[0].split(', ')]) == player)[0][0])

                        else:
                            time_formation_difference_is_eligible.append(0)
                    if np.any(np.asarray(time_formation_difference_is_eligible) == 1):
                        team_formation_id = int(df_opta_events.value.loc[(df_opta_events.team_id==
                                                                          player_team_id) & (df_opta_events.type_id==40) & (df_opta_events.qualifier_id==130)].tolist()[np.where(np.asarray(time_formation_difference_is_eligible)==1)[0][-1]])
                        team_formation = formations.formation.loc[formations.formations_id==team_formation_id].iloc[0]

                if np.any(df_opta_events.type_id[(df_opta_events.team_id==opp_team_id)].unique() == 40):
                    time_formation_difference_is_eligible = []
                    for change_id in df_opta_events.unique_event_id.loc[(df_opta_events.team_id==opp_team_id) & (df_opta_events.type_id==40)].unique().tolist():
                        period_id_formation_change = int(
                            df_opta_events['period_id'].loc[df_opta_events.unique_event_id ==
                                                            change_id].unique().tolist()[0])
                        time_difference = float(df_opta_events['min'].loc[df_opta_events.unique_event_id ==
                                                                          change_id].unique().tolist()[
                                                    0]) * 60.0 + float(
                            df_opta_events['sec'].loc[df_opta_events.unique_event_id ==
                                                      change_id].unique().tolist()[0]) - time_sub

                        if (time_difference <= 60.0) & (period_id_formation_change <= period_id_sub):
                            time_formation_difference_is_eligible.append(1)
                            #if time_difference >= 0.0:
                            #    player_position_id = int(1 + np.where(np.array([int(x) for x in df_opta_events[(df_opta_events.unique_event_id==change_id) & (df_opta_events.qualifier_id==30)].value.iloc[0].split(', ')]) == player)[0][0])

                        else:
                            time_formation_difference_is_eligible.append(0)
                    if np.any(np.asarray(time_formation_difference_is_eligible) == 1):
                        opp_team_formation_id = int(df_opta_events.value.loc[(df_opta_events.team_id==
                                                                              opp_team_id) & (df_opta_events.type_id==40) & (df_opta_events.qualifier_id==130)].tolist()[np.where(np.asarray(time_formation_difference_is_eligible)==1)[0][-1]])
                        opp_team_formation = formations.formation.loc[formations.formations_id==team_formation_id].iloc[0]

            summary_list.append(['c' + str(df_opta_events['competition_id'].unique()[0]), df_opta_events['competition_name'].unique()[0],
                                 df_opta_events['season_id'].unique()[0], df_opta_events['season_name'].unique()[0],
                                 opta_match_info['match_date'].split('T')[0], 'f' + str(opta_match_info['match_id']), 'p' + str(player_id), player_name,
                                 't' + str(player_team_id), player_team_name, 't' + str(opp_team_id), opp_team_name, home_away, team_formation_id, team_formation, opp_team_formation_id, opp_team_formation,
                                 player_position_id, player_start, player_sub_on, player_sub_off])

        summary_df = pd.DataFrame(summary_list, columns = ['Competition ID', 'Competition Name', 'Season ID', 'Season Name',
                                                           'Date', 'Game ID', 'Player ID', 'Player Name',
                                                           'Team ID', 'Team Name', 'Opposition Team ID', 'Opposition Team Name', 'Home/Away', 'Team Formation ID',
                                                           'Team Formation', 'Opposition Team Formation ID', 'Opposition Team Formation', 'Position ID',
                                                           'Start', 'Substitute On', 'Substitute Off'])
        return summary_df

    def opta_core_stats_with_time_possession(self, df_time_possession, df_opta_core_stats):
        #time_possession_df['game_id'] = time_possession_df['game_id'].astype(int)
        df_time_possession['Player_ID'] = df_time_possession['Player_ID'].astype(int)
        df_time_possession['Player_ID'] = ['p' + str(x) for x in df_time_possession['Player_ID']]

        #join the two dfs and replace columns
        df_opta_core_stats = df_opta_core_stats.merge(df_time_possession, how = 'inner', left_on = ['Player ID'],
                                                      right_on = ['Player_ID']).drop(['Player_ID'], axis = 1)

        df_opta_core_stats['Time Played'] = df_opta_core_stats['time played']
        df_opta_core_stats['Time In Possession'] = df_opta_core_stats['time in possession for the team']
        df_opta_core_stats['Time Out Of Possession'] = df_opta_core_stats['time out possession for the team']
        df_opta_core_stats.drop(['time played', 'time in possession for the team', 'time out possession for the team'], axis = 1,
                                inplace = True)
        return df_opta_core_stats

    def opta_crosses_classification(self, df_opta_events): # TODO check if it matches new version line 155 crosses loop in tracking data.py


        df_opta_events['Time_in_Seconds'] = df_opta_events['min'] * 60.0 + df_opta_events['sec']
        event_data_open_play_crosses_big = df_opta_events.groupby(['unique_event_id']).apply(
            self.only_open_play_crosses)  # .drop_duplicates(['unique_event_id'])

        event_data_open_play_crosses = event_data_open_play_crosses_big.drop_duplicates(['unique_event_id']).reset_index(
            drop=True)

        events = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id.isin([1, 2])) & (
                event_data_open_play_crosses_big.open_play_cross == 1)].unique_event_id.tolist()))
        teams = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id.isin([1, 2])) & (
                event_data_open_play_crosses_big.open_play_cross == 1)].team_id.tolist()))
        periods = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id.isin([1, 2])) & (
                event_data_open_play_crosses_big.open_play_cross == 1)].period_id.tolist()))

        for i in range(len(events)):
            current_time = np.round(event_data_open_play_crosses[event_data_open_play_crosses.unique_event_id == events[i]][
                                        'Time_in_Seconds'].iloc[0], 2)
            event_data_window = event_data_open_play_crosses[(event_data_open_play_crosses.period_id == periods[i]) & (
                    event_data_open_play_crosses.team_id == teams[i]) & (
                                                                     event_data_open_play_crosses.Time_in_Seconds >= current_time - 10.0) & (
                                                                     event_data_open_play_crosses.Time_in_Seconds < current_time)]
            # if in the previous 10 secs window we have an event related to a set piece or a corner
            if len(set([2, 3]).intersection(set(event_data_window.open_play_cross))) > 0:
                # if there is a corner event in the previous 10 secs window
                if np.any(event_data_window.open_play_cross == 3):
                    event_data_open_play_crosses.loc[
                        (event_data_open_play_crosses.unique_event_id == events[i]), 'open_play_cross'] = 6
                else:
                    # loop over the set pieces events
                    for j in range(len(event_data_window[event_data_window.open_play_cross == 2]['Time_in_Seconds'])):
                        current_time_set_piece = np.round(
                            event_data_window[event_data_window.open_play_cross == 2]['Time_in_Seconds'].iloc[j], 2)
                        event_data_window_set_piece = event_data_open_play_crosses[
                            (event_data_open_play_crosses.period_id == periods[i]) & (
                                    event_data_open_play_crosses.team_id == teams[i]) & (
                                    event_data_open_play_crosses.Time_in_Seconds >= current_time_set_piece - 5.0) & (
                                    event_data_open_play_crosses.Time_in_Seconds < current_time_set_piece)]
                        # if in the previous 5 secs window we do not have a foul
                        if np.all(event_data_window_set_piece.open_play_cross != 4):
                            if event_data_window[event_data_window.open_play_cross == 2]['x'].iloc[j] > 40.0:
                                event_data_open_play_crosses.loc[
                                    (event_data_open_play_crosses.unique_event_id == events[i]), 'open_play_cross'] = 5

                        # if we have any foul
                        else:
                            # if all of the fouls is in favour of the defending team
                            if np.all(event_data_window_set_piece[
                                          event_data_window_set_piece.open_play_cross == 4].outcome == 0):
                                if event_data_window[event_data_window.open_play_cross == 2]['x'].iloc[j] > 40.0:
                                    event_data_open_play_crosses.loc[
                                        (event_data_open_play_crosses.unique_event_id == events[i]), 'open_play_cross'] = 5

        data_output = df_opta_events.merge(event_data_open_play_crosses[(event_data_open_play_crosses.type_id.isin([1, 2]))][
                                               ['unique_event_id', 'open_play_cross']].reset_index(drop=True),
                                           how='inner', left_on=['unique_event_id'], right_on=['unique_event_id']).sort_values(
            ['period_id', 'min', 'sec'])

        data_output['pass_situation'] = np.where(data_output['open_play_cross'] == 1, 'open play',
                                                 np.where((data_output['open_play_cross'] == 2) | (
                                                         data_output['open_play_cross'] == 5), 'free kick', 'corner'))

        data_output = data_output.drop(columns=['open_play_cross'])

        if data_output.shape[0] == 0:
            data_output['qualifier_ids'] = None
            data_output['qualifier_values'] = None
            data_output = data_output.drop(columns=['qualifier_id', 'value'])
            data_output['x_end'] = None
            data_output['y_end'] = None

        else:
            # we need to pivot the qualifier ids and values such that each row is an event
            data_output_grouped = data_output.groupby(
                [x for x in data_output.columns if x not in ['qualifier_id', 'value']])
            list_output = []
            for index, df in data_output_grouped:
                list_output.append(self.merge_qualifiers(df.reset_index(drop=True)))
            data_output = pd.concat(list_output)
            data_output['x_end'] = [float(data_output['qualifier_values'].iloc[i].split(', ')[np.where(
                np.array([int(y) for y in data_output['qualifier_ids'].iloc[i].split(', ')]) == 140)[0][0]]) for i in
                                    range(data_output.shape[0])]
            data_output['y_end'] = [float(data_output['qualifier_values'].iloc[i].split(', ')[np.where(
                np.array([int(y) for y in data_output['qualifier_ids'].iloc[i].split(', ')]) == 141)[0][0]]) for i in
                                    range(data_output.shape[0])]

        data_output['freekick_taken'] = [int(5 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
        data_output['corner_taken'] = [int(6 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
        data_output['throw_in_taken'] = [int(107 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
        data_output['blocked_pass'] = [int(236 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
        data_output['chipped_pass'] = [int(155 in [int(y) for y in x.split(', ')]) for x in data_output[
            'qualifier_ids']]  # this is the column that tells us whether a pass is high ot low

        # let's use some set of rules to classify by ourselves passes as crosses
        data_output['OPTA Cross Qualifier'] = [int(2 in [int(y) for y in x.split(', ')]) for x in
                                               data_output['qualifier_ids']]
        data_output['Headed Pass'] = [int(3 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]

        data_output = self.cross_label(data_output)

        data_output['Attacking Team Name'] = np.where(data_output['team_id'] == data_output['home_team_id'],
                                                      data_output['home_team_name'], data_output['away_team_name'])
        data_output['Defending Team Name'] = np.where(data_output['team_id'] == data_output['home_team_id'],
                                                      data_output['away_team_name'], data_output['home_team_name'])

        # crosses can't be switch of play or through balls
        data_output['cross_to_remove'] = [(len(set([196, 4]).intersection(set([int(y) for y in x.split(', ')]))) > 0) for x
                                          in data_output['qualifier_ids']]
        data_output['cross_to_remove'] = np.where(
            (data_output['cross_to_remove'] == 1) & (data_output['corner_taken'] == 1), 0, data_output['cross_to_remove'])
        # final_df['cross_to_remove'] = np.where(final_df['x']==0,1,final_df['cross_to_remove'])
        data_output['cross_to_remove'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 0,
                                                  np.where(data_output['cross_to_remove'] == 1, 1,
                                                           np.where(
                                                               (data_output['x'] < 70) & (data_output['x_end'] < 83.0) & (
                                                                       (data_output['y_end'] < 36.8) | (
                                                                       data_output['y_end'] > 63.2)), 1,
                                                               data_output['cross_to_remove'])))

        data_output['out_of_pitch'] = np.where(
            (data_output['x_end'] >= 100.0) | (data_output['y_end'] <= 0) | (data_output['y_end'] >= 100), 1, 0)
        data_output['ending_too_wide'] = np.where(
            ((data_output['y_end'] <= 21) | (data_output['y_end'] >= 79)) & (data_output['y_end'] < 100.0) & (
                    data_output['y_end'] > 0.0) & (data_output['x_end'] < 100.0) & (
                    np.sign(data_output['y'] - 50.0) != np.sign(data_output['y_end'] - 50.0)), 1, 0)

        return data_output[(data_output['Our Cross Qualifier'] == 1) & (data_output.cross_to_remove == 0)].reset_index(
            drop=True)

    #get all shot events
    def opta_shots(self, df_opta_events, df_player_names_raw, opta_match_info): # TODO check line 91 set_pieces_classification.py
        '''

        :param df_opta_events:
        :type df_opta_events:
        :param df_player_names_raw:
        :type df_player_names_raw:
        :param opta_match_info:
        :type opta_match_info:
        :return:
        :rtype:
        '''

        data = df_opta_events
        player_names_raw = df_player_names_raw

        data['Time_in_Seconds'] = data['min']*60.0 + data['sec']
        data['player_id'] = data['player_id'].fillna(0)
        #data['player_id'] = ['p' + str(int(x)) for x in data.player_id]
        data['player_id'] = list(map(lambda x: 'p' + str(int(x)), data.player_id)) # TODO Check
        data['player_name'] = [player_names_raw.full_name.loc[player_names_raw['player_id']==int(x[1:])].iloc[0] if int(x[1:]) in player_names_raw['player_id'].tolist() else None for x in data.player_id]

        data_shots = data[((data.period_id==1) | (data.period_id==2) | (data.period_id==3) | (data.period_id==4)) & (data.type_id.isin([13,14,15,16]))].reset_index(drop = True)
        #data_shots['value'] = data_shots['value'].fillna(0)
        #data_shots['value'] = data_shots['value'].astype(int)
        # & ((data.unique_event_id.isin(data.unique_event_id.loc[data.qualifier_id==55])) | (data.unique_event_id.isin(data.unique_event_id.loc[data.qualifier_id==28])))

        if data_shots.shape[0] == 0:
            data_shots['qualifier_ids'] = None
            data_shots['qualifier_values'] = None
            data_shots['qualifier_55_value'] = None
            data_shots = data_shots.drop(columns = ['qualifier_id', 'value'])
            #data_shots['x_end'] = None
            #data_shots['y_end'] = None

        else:
            #we need to pivot the qualifier ids and values such that each row is an event
            #data_output = data_output.groupby([x for x in data_output.columns if x not in ['qualifier_id', 'value']]).apply(self.merge_qualifiers).reset_index(drop = True) #weird behaviour by groupby apply function, we need to use a loop then
            data_output_grouped = data_shots.groupby([x for x in data_shots.columns if x not in ['qualifier_id', 'value']])
            list_output = []
            for index, df in data_output_grouped:
                list_output.append(self.merge_qualifiers(df.reset_index(drop = True)))
            data_shots = pd.concat(list_output)
            #data_output['x_end'] = [float(data_output['qualifier_values'].iloc[i].split(', ')[np.where(np.array([int(y) for y in data_output['qualifier_ids'].iloc[i].split(', ')])==140)[0][0]]) for i in range(data_output.shape[0])]
            #data_output['y_end'] = [float(data_output['qualifier_values'].iloc[i].split(', ')[np.where(np.array([int(y) for y in data_output['qualifier_ids'].iloc[i].split(', ')])==141)[0][0]]) for i in range(data_output.shape[0])]

        data_shots['value'] = data_shots.qualifier_55_value
        data_shots['value'] = data_shots['value'].astype(int)
        data_shots['own_goal'] = [int(28 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        if np.any(data_shots['own_goal']==1):
            for i in data_shots.unique_event_id.loc[data_shots.own_goal==1].tolist():
                related_event_id_og = data.event_id.loc[(data.Time_in_Seconds < data_shots.Time_in_Seconds.loc[data_shots.unique_event_id==i].iloc[0]) & (data.period_id==data_shots.period_id.loc[data_shots.unique_event_id==i].iloc[0])].iloc[-1] #most recent event before the own goal
                data_shots.loc[data_shots.unique_event_id==i, 'value'] = int(related_event_id_og)

        data_shots['related_event_team_id'] = np.where(data_shots['own_goal'] == 0, data_shots['team_id'], np.where(data_shots['team_id']==opta_match_info['home_team_id'], opta_match_info['away_team_id'], opta_match_info['home_team_id']))

        data_shots['headed'] = [int(15 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['other_body_part'] = [int(21 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['big_chance'] = [int(214 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['not_reaching_goal_line'] = [int(153 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['blocked'] = [int(82 in [int(y) for y in x.split(', ')]) & int(101 not in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['saved_off_line'] = [int(82 in [int(y) for y in x.split(', ')]) & int(101 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['deflected'] = [int(133 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_regular_play'] = [int(22 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_fast_break'] = [int(23 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_set_piece'] = [int(24 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_corner'] = [int(25 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_direct_freekick'] = [int(26 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_penalty'] = [int(9 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['from_throw_in'] = [int(160 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
        data_shots['goal'] = np.where(data_shots['type_id'] == 16, 1, 0)
        data_shots['on_target'] = np.where(((data_shots['type_id']==15) | (data_shots['type_id']==16)) & ((data_shots['blocked']==0) | (data_shots['saved_off_line']==1)), 1, 0)
        data_shots['off_target'] = np.where((data_shots['type_id']==13) | (data_shots['type_id']==14), 1, 0)
        #data_shots['chance_missed'] = np.where(data_shots['type_id'] == 60, 1, 0)

        data_shots['direct_shot'] = np.where(data_shots['value'] > 0, 1, 0)



        return data_shots

    @staticmethod
    def only_open_play_crosses(data):# TODO check if it matches new version crosses loop in tracking data.py
        # if we have a pass
        if (data.type_id.unique()[0] == 1) | (data.type_id.unique()[0] == 2):
            # if we have a cross
            #    if 2 in data.qualifier_id.tolist():
            # if the cross does not involve set pieces/corners
            if len(set([5, 6]).intersection(set(data.qualifier_id.tolist()))) == 0:
                data['open_play_cross'] = 1
            # if the cross involves set pieces/corners
            else:
                # if the cross involves a corner
                if (len(set([6]).intersection(set(data.qualifier_id.tolist()))) > 0) & (
                        len(set([5]).intersection(set(data.qualifier_id.tolist()))) == 0):
                    data['open_play_cross'] = 3
                # if the cross involves a set piece
                else:
                    data['open_play_cross'] = 2
        # if the event is not a pass
        else:
            # if the event involves set corners
            if ((6 in data.type_id.tolist()) | (len(set([6]).intersection(set(data.qualifier_id.tolist()))) > 0)) & (
                    len(set([5]).intersection(set(data.qualifier_id.tolist()))) == 0):
                data['open_play_cross'] = 3
            # if the event does not involve corners
            else:
                # if the event is a foul
                if (4 in data.type_id.tolist()) | (13 in data.qualifier_id.tolist()):
                    data['open_play_cross'] = 4
                # if the event is not a foul
                else:
                    # if the event is related to a set piece
                    if len(set([5]).intersection(set(data.qualifier_id.tolist()))) > 0:
                        data['open_play_cross'] = 2
                    # if the event is not related to a set piece
                    else:
                        data['open_play_cross'] = 0
        return data

    @staticmethod
    def cross_label(data_output): #TODO change to cross_label_v4 crosses loop in tracking data.py
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      # if there is cross qualifier then 1
                                                      np.where(data_output['x'] < 60.0, 0,
                                                               # if the pas starts from behind the halfway line then 0
                                                               np.where((data_output['y'] > 100 / 3.0) & (
                                                                       data_output['y'] < 100 / 1.5), 0,
                                                                        # if the pass starts too frontal then 0
                                                                        np.where(data_output['x_end'] < 80.0, 0,
                                                                                 # if the pass ends too behind then 0
                                                                                 np.where((np.abs(
                                                                                     data_output['y'] - 50.0) < np.abs(
                                                                                     data_output['y_end'] - 50.0)) & (
                                                                                                  np.sign(data_output[
                                                                                                              'y'] - 50.0) == np.sign(
                                                                                              data_output[
                                                                                                  'y_end'] - 50.0)), 0,
                                                                                          # if the pass is directed wide then 0
                                                                                          np.where((np.abs(
                                                                                              data_output['y'] -
                                                                                              data_output[
                                                                                                  'y_end']) < 25.0) & (((
                                                                                                                                data_output[
                                                                                                                                    'y'] < 21.1) | (
                                                                                                                                data_output[
                                                                                                                                    'y'] > 78.9)) | (
                                                                                                                               data_output[
                                                                                                                                   'x'] < 83.0)),
                                                                                                   0, 1))))))
        # np.where((data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0, 1))))))) #if the ball ends up too wide then 0

        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where(
                                                          (data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0,
                                                          data_output['Our Cross Qualifier']))
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((data_output['chipped_pass'] == 0) & (
                                                              (data_output['x'] < 75.0) | (
                                                              (data_output['y'] < 0.5 * 21.1) | (
                                                              data_output['y'] > 0.5 * (100 + 78.9)))), 0,
                                                               data_output[
                                                                   'Our Cross Qualifier']))  # low passes should start not too behind or not too wide
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((np.abs(
                                                          data_output['x_end'] - data_output['x']) > 1.05 * np.abs(
                                                          data_output['y_end'] - data_output['y'])) & (
                                                                       data_output['x'] < 70.0) & (
                                                                       data_output['blocked_pass'] == 0), 0,
                                                               data_output['Our Cross Qualifier']))
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((np.abs(
                                                          data_output['x_end'] - data_output['x']) > 0.75 * np.abs(
                                                          data_output['y_end'] - data_output['y'])) & (
                                                                       data_output['x'] < 80.0) & (
                                                                       data_output['x'] >= 70.0) & (
                                                                       data_output['blocked_pass'] == 0), 0,
                                                               data_output[
                                                                   'Our Cross Qualifier']))  # we set an upper bound on the distance travelled on the x axis proportional to the distance travelled on the y axis
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((np.abs(
                                                          data_output['x_end'] - data_output['x']) > 0.5 * np.abs(
                                                          data_output['y_end'] - data_output['y'])) & (
                                                                       data_output['x'] >= 80.0) & (
                                                                       data_output['blocked_pass'] == 0), 0,
                                                               data_output['Our Cross Qualifier']))

        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((data_output['chipped_pass'] == 0) & (
                                                              data_output['pass_situation'] == 'open play') & (
                                                                       (data_output['x_end'] < 83.0) | (
                                                                       (data_output['y_end'] > 78.9) | (
                                                                       data_output['y_end'] < 21.1))), 0,
                                                               data_output[
                                                                   'Our Cross Qualifier']))  # all low passes from open play ending outside the box are not crosses
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((data_output['chipped_pass'] == 0) & (
                                                              data_output['pass_situation'] == 'open play') & (
                                                                       data_output['x'] < 83.0) & (
                                                                       data_output['y'] >= 21.1) & (
                                                                       data_output['y'] <= 78.9), 0,
                                                               data_output[
                                                                   'Our Cross Qualifier']))  # all low passes from open play ending outside the box are not crosses
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((data_output['pass_situation'] != 'open play') & (
                                                              data_output['chipped_pass'] == 0) & (((data_output[
                                                                                                         'y'] < 21.1) | (
                                                                                                            data_output[
                                                                                                                'y'] > 78.9)) | (
                                                                                                           data_output[
                                                                                                               'x'] < 83.0)) & (
                                                                       ((data_output['y_end'] < 100 / 3.0) & (
                                                                               data_output['y'] < 100 / 3.0)) | ((
                                                                                                                         data_output[
                                                                                                                             'y_end'] > 100 / 1.5) & (
                                                                                                                         data_output[
                                                                                                                             'y'] > 100 / 1.5))),
                                                               0,
                                                               data_output[
                                                                   'Our Cross Qualifier']))  # all low passes from set piece situation not being wide enough and ending outside box are not crosses

        # data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1,
        #    np.where((data_output['x'] < 83.0) & (data_output['x_end'] < 83.0), 0,
        #        np.where((data_output['x'] < 83.0) & (data_output['chipped_pass']==0), 0, data_output['Our Cross Qualifier']))) #any pass starting and ending before box horizontal line is not a cross and any low pass starting before the box horizontal line is not a cross as well
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where(((data_output['y_end'] > 63.2) | (
                                                              data_output['y_end'] < 36.8)) & (
                                                                       np.sign(data_output['y'] - 50.0) == np.sign(
                                                                   data_output['y_end'] - 50.0)), 0,
                                                               data_output['Our Cross Qualifier']))
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((((data_output['y'] <= 78.9) & (
                                                              data_output['y'] > 100 / 1.5)) | (
                                                                        (data_output['y'] < 100 / 3.0) & (
                                                                        data_output['y'] >= 21.1))) & (
                                                                       data_output['x'] < 83.0) & (
                                                                       (data_output['y_end'] >= 100 / 1.5) | (
                                                                       data_output['y_end'] <= 100 / 3.0)),
                                                               0, data_output['Our Cross Qualifier']))
        # data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1,
        #    np.where((data_output['chipped_pass']==0) & (data_output['x'] < 94.2) & (data_output['x_end'] < 83.0) & (data_output['pass_situation']=='open play'), 0, data_output['Our Cross Qualifier']))
        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((data_output['pass_situation'] == 'open play') & (
                                                              data_output['x'] < 82.0) & (data_output['x_end'] < 88.5),
                                                               0, data_output['Our Cross Qualifier']))

        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where((data_output['x'] >= 83.0) & (data_output['x'] < 88.5) & (
                                                              data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (
                                                                       np.abs(data_output['y'] - data_output[
                                                                           'y_end']) < 20.0), 0,
                                                               np.where((data_output['x'] >= 88.5) & (
                                                                       data_output['x'] < 94.2) & (
                                                                                data_output['y'] <= 78.9) & (
                                                                                data_output['y'] >= 21.1) & (np.abs(
                                                                   data_output['y'] - data_output['y_end']) < 15.0), 0,
                                                                        np.where((data_output['x'] >= 94.2) & (
                                                                                data_output['y'] <= 78.9) & (
                                                                                         data_output['y'] >= 21.1) & (
                                                                                         np.abs(data_output['y'] -
                                                                                                data_output[
                                                                                                    'y_end']) < 10.0),
                                                                                 0,
                                                                                 data_output['Our Cross Qualifier']))))

        data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier'] == 1, 1,
                                                      np.where(((data_output['Headed Pass'] == 1) | (
                                                              data_output['throw_in_taken'] == 1)), 0, data_output[
                                                                   'Our Cross Qualifier']))  # if the pass is headed then 0

        data_output['cutback'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
        data_output['Our Cross Qualifier'] = np.where(data_output['cutback'] == 1, 1, data_output['Our Cross Qualifier'])
        data_output['cutback'] = np.where(data_output['x'] < 83.0, 0,
                                          np.where((data_output['y'] >= 95.0) | (data_output['y'] <= 5.0), 0,
                                                   np.where(data_output['cutback'] == 1, 1,
                                                            np.where((data_output['Our Cross Qualifier'] == 1) & (
                                                                    data_output['x'] >= 88.5) & (
                                                                             data_output['x'] <= 94.2) & (
                                                                             data_output['y'] >= 0.5 * 21.1) & (
                                                                             data_output['y'] <= 0.5 * 178.9) & (
                                                                             data_output['x_end'] >= 80.0) & (
                                                                             data_output['y_end'] <= 78.9) & (
                                                                             data_output['y_end'] >= 21.1) & (
                                                                             data_output['x'] - data_output[
                                                                         'x_end'] >= 2.0),
                                                                     1,
                                                                     np.where((data_output['Our Cross Qualifier'] == 1) & (
                                                                             data_output['x'] > 94.2) & (
                                                                                      data_output['x_end'] < 94.2) & (
                                                                                      data_output[
                                                                                          'y'] >= 0.5 * 21.1) & (
                                                                                      data_output[
                                                                                          'y'] <= 0.5 * 178.9) & (
                                                                                      data_output['x_end'] >= 80.0) & (
                                                                                      data_output['y_end'] <= 78.9) & (
                                                                                      data_output['y_end'] >= 21.1) & (
                                                                                      data_output['x'] - data_output[
                                                                                  'x_end'] >= 2.0),
                                                                              1, 0)))))
        data_output['cutback'] = np.where((data_output['cutback'] == 1) & (data_output['chipped_pass'] == 1) & (
                data_output['x'] - data_output['x_end'] < 5.0) & (np.array(
            [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]) == 0),
                                          0, data_output['cutback'])

        return data_output

    @staticmethod
    def merge_qualifiers(dat): # TODO check if it matches new version line 96 set_pieces_classification.py
        '''
        :param dat:
        :type dat:
        :return:
        :rtype:
        '''
        dat['qualifier_ids'] = ', '.join([str(int(x)) for x in dat.qualifier_id.tolist()])
        dat['qualifier_values'] = ', '.join([str(x) for x in dat['value'].tolist()])
        if np.any(dat['qualifier_id']==55):
            dat['qualifier_55_value'] = int(dat['value'].loc[dat['qualifier_id']==55].iloc[0])
        else:
            dat['qualifier_55_value'] = 0
        dat = dat.drop(columns=['qualifier_id', 'value']).drop_duplicates()
        return dat
