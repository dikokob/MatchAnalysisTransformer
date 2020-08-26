####this is the function that takes the necessary file paths related to a specific game to output a summary file for that game
import os 

os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts")

from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation

def opta_core_stats_output (path_events, path_match_results, path_formations = '\\\ctgshares\\Drogba\\Analysts\\FB\\formations metadata.xlsx'):
    import pandas as pd
    import numpy as np
    import xmltodict 
    #season_id, competition_id, add t to team id and opp team_id, add f to game_id, add p to player_id, opp team formation id, opp team formation

    #path_events = file_event 
    #path_match_results = file_results
    #path_formations = '\\\ctgshares\\Drogba\\Analysts\\FB\\formations metadata.xlsx'

    opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
    referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results) 

    formations = pd.read_excel(path_formations, index_col = None)
    formations['formation'] = [int(''.join(x.split('-'))) for x in formations.formation]

    #let's consider the players eligible for inclusion - i.e. all the ones involved in the game
    home_players_starters = [int(x) for x in opta_event_data_df.value.loc[(opta_event_data_df.qualifier_id==30) & (opta_event_data_df.type_id==34) & (opta_event_data_df.team_id==home_team_id)].iloc[0].split(',')[:11]]
    home_players_subs_on = [int(x) for x in opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.team_id==home_team_id)].unique().tolist()]
    if len(home_players_subs_on) == 0:
        home_players_subs_on = [-999]
    away_players_starters = [int(x) for x in opta_event_data_df.value.loc[(opta_event_data_df.qualifier_id==30) & (opta_event_data_df.type_id==34) & (opta_event_data_df.team_id==away_team_id)].iloc[0].split(',')[:11]]
    away_players_subs_on = [int(x) for x in opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.team_id==away_team_id)].unique().tolist()]
    if len(away_players_subs_on) == 0:
        away_players_subs_on = [-999]

    players_to_consider = [x for y in [home_players_starters, home_players_subs_on, away_players_starters, away_players_subs_on] for x in y if x != -999]


    #here teh actual loop starts
    summary_list = []

    for player in players_to_consider:
        player_id = player 
        player_name = player_names_raw.player_name.loc[player_names_raw['@uID'] == 'p' + str(player_id)].iloc[0]
        player_team_id = np.where(len(set([player_id]).intersection([x for y in [home_players_starters, home_players_subs_on] for x in y])) == 1, home_team_id, away_team_id).tolist()
        player_team_name = np.where(len(set([player_id]).intersection([x for y in [home_players_starters, home_players_subs_on] for x in y])) == 1, home_team_name, away_team_name).tolist()
        opp_team_id = np.where(player_team_id == home_team_id, away_team_id, home_team_id).tolist()
        opp_team_name = np.where(player_team_name == home_team_name, away_team_name, home_team_name).tolist()
        home_away = np.where(player_team_id == home_team_id, 'Home', 'Away').tolist()
        if home_formation is None:
            home_formation = formations.formation.loc[(formations.formations_id==int(opta_event_data_df['value'].loc[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==130) & (opta_event_data_df.team_id==home_team_id)].iloc[0]))].iloc[0]
        if away_formation is None:
            away_formation = formations.formation.loc[(formations.formations_id==int(opta_event_data_df['value'].loc[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==130) & (opta_event_data_df.team_id==away_team_id)].iloc[0]))].iloc[0]
        team_formation = np.where(player_team_id == home_team_id, home_formation, away_formation).tolist()
        opp_team_formation = np.where(player_team_id == home_team_id, away_formation, home_formation).tolist()
        team_formation_id = formations.formations_id.loc[formations.formation == team_formation].iloc[0]
        opp_team_formation_id = formations.formations_id.loc[formations.formation == opp_team_formation].iloc[0]
        if '@Formation_Place' not in list(players_df_lineup):
            players_initial = np.array([int(x) for x in opta_event_data_df.value.loc[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==30) & (opta_event_data_df.team_id==player_team_id)].iloc[0].split(',')])
            players_position_ids = [int(x) for x in opta_event_data_df.value.loc[(opta_event_data_df.type_id==34) & (opta_event_data_df.qualifier_id==131) & (opta_event_data_df.team_id==player_team_id)].iloc[0].split(',')]
            player_position_id = players_position_ids[np.where(players_initial==player_id)[0][0]]
        else:
            player_position_id = int(players_df_lineup['@Formation_Place'].loc[players_df_lineup['@PlayerRef'] == 'p' + str(player_id)].iloc[0])
        player_start = np.where(len(set([player_id]).intersection([x for y in [home_players_starters, away_players_starters] for x in y])) == 1, 1, 0).tolist()
        player_sub_on = np.where(len(set([player_id]).intersection([x for y in [home_players_subs_on, away_players_subs_on] for x in y])) == 1, 1, 0).tolist()
        player_sub_off = np.where(len(set([player_id]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==18].unique().tolist()))) == 1, 1, 0).tolist()
    
        #aside to deal with position id and any change of formation when a sub on is considered
        if player_sub_on == 1:
            period_id_sub = int(opta_event_data_df['period_id'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==44) & (opta_event_data_df.player_id==player_id)].iloc[0])
            if opta_event_data_df.value.loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==44) & (opta_event_data_df.player_id==player_id)].iloc[0] == 'Goalkeeper':
                player_position_id = 1
                time_sub = float(opta_event_data_df['min'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==44) & (opta_event_data_df.player_id==player_id)].iloc[0])*60.0 + float(opta_event_data_df['sec'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==44) & (opta_event_data_df.player_id==player_id)].iloc[0])
            else:
                # if 292 in opta_event_data_df.qualifier_id.loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.player_id==player_id)].tolist():
                #     player_position_id = int(opta_event_data_df.value.loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==292) & (opta_event_data_df.player_id==player_id)].iloc[0])
                #     time_sub = float(opta_event_data_df['min'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==292) & (opta_event_data_df.player_id==player_id)].iloc[0])*60.0 + float(opta_event_data_df['sec'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==292) & (opta_event_data_df.player_id==player_id)].iloc[0])
                # else:
                player_position_id = int(opta_event_data_df.value.loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==145) & (opta_event_data_df.player_id==player_id)].iloc[0])
                time_sub = float(opta_event_data_df['min'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==145) & (opta_event_data_df.player_id==player_id)].iloc[0])*60.0 + float(opta_event_data_df['sec'].loc[(opta_event_data_df.type_id==19) & (opta_event_data_df.qualifier_id==145) & (opta_event_data_df.player_id==player_id)].iloc[0])
            
            #change formation for team of the player under consideration
            if np.any(opta_event_data_df.type_id[(opta_event_data_df.team_id==player_team_id)].unique() == 40):
                time_formation_difference_is_eligible = []
                for change_id in opta_event_data_df.unique_event_id.loc[(opta_event_data_df.team_id==player_team_id) & (opta_event_data_df.type_id==40)].unique().tolist():
                    period_id_formation_change = int(opta_event_data_df['period_id'].loc[opta_event_data_df.unique_event_id==
                        change_id].unique().tolist()[0])
                    time_difference = float(opta_event_data_df['min'].loc[opta_event_data_df.unique_event_id==
                        change_id].unique().tolist()[0])*60.0 + float(opta_event_data_df['sec'].loc[opta_event_data_df.unique_event_id==
                        change_id].unique().tolist()[0]) - time_sub
                    if (time_difference <= 60.0) & (period_id_formation_change <= period_id_sub): #additional condition is used to make sure extra time in first half is still lower than start of second half
                        time_formation_difference_is_eligible.append(1)
                        if time_difference >= 0.0:
                            player_position_id = int(1 + np.where(np.array([int(x) for x in opta_event_data_df[(opta_event_data_df.unique_event_id==change_id) & (opta_event_data_df.qualifier_id==30)].value.iloc[0].split(', ')]) == player)[0][0])
                    else:
                        time_formation_difference_is_eligible.append(0)
                if np.any(np.asarray(time_formation_difference_is_eligible) == 1):
                    team_formation_id = int(opta_event_data_df.value.loc[(opta_event_data_df.team_id==
                        player_team_id) & (opta_event_data_df.type_id==40) & (opta_event_data_df.qualifier_id==130)].tolist()[np.where(np.asarray(time_formation_difference_is_eligible)==1)[0][-1]])
                    team_formation = formations.formation.loc[formations.formations_id==team_formation_id].iloc[0]
                
            #change formation for opponent team
            if np.any(opta_event_data_df.type_id[(opta_event_data_df.team_id==opp_team_id)].unique() == 40):
                time_formation_difference_is_eligible = []
                for change_id in opta_event_data_df.unique_event_id.loc[(opta_event_data_df.team_id==opp_team_id) & (opta_event_data_df.type_id==40)].unique().tolist():
                    period_id_formation_change = int(opta_event_data_df['period_id'].loc[opta_event_data_df.unique_event_id==
                        change_id].unique().tolist()[0])
                    time_difference = float(opta_event_data_df['min'].loc[opta_event_data_df.unique_event_id==
                        change_id].unique().tolist()[0])*60.0 + float(opta_event_data_df['sec'].loc[opta_event_data_df.unique_event_id==
                        change_id].unique().tolist()[0]) - time_sub
                    if (time_difference <= 60.0) & (period_id_formation_change <= period_id_sub): #additional condition is used to make sure extra time in first half is still lower than start of second half
                        time_formation_difference_is_eligible.append(1)
                        #if time_difference >= 0.0:
                        #    player_position_id = int(1 + np.where(np.array([int(x) for x in opta_event_data_df[(opta_event_data_df.unique_event_id==change_id) & (opta_event_data_df.qualifier_id==30)].value.iloc[0].split(', ')]) == player)[0][0])
                    else:
                        time_formation_difference_is_eligible.append(0)
                if np.any(np.asarray(time_formation_difference_is_eligible) == 1):
                    opp_team_formation_id = int(opta_event_data_df.value.loc[(opta_event_data_df.team_id==
                        opp_team_id) & (opta_event_data_df.type_id==40) & (opta_event_data_df.qualifier_id==130)].tolist()[np.where(np.asarray(time_formation_difference_is_eligible)==1)[0][-1]])
                    opp_team_formation = formations.formation.loc[formations.formations_id==opp_team_formation_id].iloc[0]

        
        # ###passes
        # keypasses = len(opta_event_data_df.unique_event_id.loc[(opta_event_data_df.player_id==player) & (opta_event_data_df.keypass==1)].unique())
        # assists = len(opta_event_data_df.unique_event_id.loc[(opta_event_data_df.player_id==player) & (opta_event_data_df.assist==1)].unique())

        # passes_df = opta_event_data_df.loc[opta_event_data_df.type_id==1]

        # successful_passes = len(passes_df.unique_event_id.loc[(~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==107].unique().tolist())) & (passes_df.outcome==1)].unique())
        # unsuccessful_passes = len(passes_df.unique_event_id.loc[(~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==107].unique().tolist())) & (passes_df.outcome==0)].unique())
        # successful_passes_excl_crosses_corners = len(passes_df.unique_event_id.loc[(~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==124].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==123].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==2].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==107].unique().tolist())) & (passes_df.outcome==1)].unique())
        # unsuccessful_passes_excl_crosses_corners = len(passes_df.unique_event_id.loc[(~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==124].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==123].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==2].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==107].unique().tolist())) & (passes_df.outcome==0)].unique())
        # successful_passes_own_half = len(passes_df.unique_event_id.loc[(~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==124].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==123].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==107].unique().tolist())) & (passes_df.outcome==1) & (passes_df.qualifier_id==140) & ([float(x) < 50.0 for x in passes_df.value.tolist()])].unique())
        # unsuccessful_passes_own_half = len(passes_df.unique_event_id.loc[(~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==124].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==123].unique().tolist())) & (~passes_df.unique_event_id.isin(passes_df.unique_event_id.loc[passes_df.qualifier_id==107].unique().tolist())) & (passes_df.outcome==0) & (passes_df.qualifier_id==140) & ([float(x) < 50.0 for x in passes_df.value.tolist()])].unique())


        summary_list.append(['c' + str(opta_event_data_df['competition_id'].unique()[0]), opta_event_data_df['competition_name'].unique()[0],
            opta_event_data_df['season_id'].unique()[0], opta_event_data_df['season_name'].unique()[0], 
            game_date.split('T')[0], 'f' + str(game_id), 'p' + str(player_id), player_name, 
            't' + str(player_team_id), player_team_name, 't' + str(opp_team_id), opp_team_name, home_away, team_formation_id, team_formation, opp_team_formation_id, opp_team_formation,
            player_position_id, player_start, player_sub_on, player_sub_off])

    summary_df = pd.DataFrame(summary_list, columns = ['Competition ID', 'Competition Name', 'Season ID', 'Season Name',
        'Date', 'Game ID', 'Player ID', 'Player Name',
        'Team ID', 'Team Name', 'Opposition Team ID', 'Opposition Team Name', 'Home/Away', 'Team Formation ID',
        'Team Formation', 'Opposition Team Formation ID', 'Opposition Team Formation', 'Position ID', 
        'Start', 'Substitute On', 'Substitute Off'])


    ###here we add two columns about players being sent off or retiring
    all_player_sent_off = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33]))].unique().tolist()))) == 1, 1, 0).tolist() for x in summary_df['Player ID'].tolist()]
    all_player_retired = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==20].unique().tolist()))) == 1, 1, 0).tolist() for x in summary_df['Player ID'].tolist()]
    all_player_sub_off = summary_df['Substitute Off'].tolist()

    summary_df['Sent Off'] = np.where(np.array(all_player_sub_off)==1, 0, np.array(all_player_sent_off)).tolist()
    summary_df['Retired'] = all_player_retired

    return summary_df

# import time
# start = time.process_time()
# summary_df = opta_summary_output('C:\\Users\\fbettuzzi\\Desktop\\f24-8-2018-987592-eventdetails.xml', 
#      'C:\\Users\\fbettuzzi\\Desktop\\srml-8-2018-f987592-matchresults.xml')  
# print(time.process_time() - start)

#path_folder = '\\\\ctgshares\\Drogba\\API Data Files\\2016-17\\Premier League\\Round 13 All Data'
#path_folder = '\\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Premier League\\Round 9 All Data' 
#path_folder = sys.argv[1]
#sub = 'g1059833'
#sub = 'g1059719'
#sub = 'g855294'
#sub = sys.argv[2]
#sub = 'g1059784'
#event_type = sys.argv[3]
#event_type = 'Set Piece'

# path_events, path_match_results = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if (x.endswith('.xml')) & ('meta' not in x.lower())] #we do not need the meta
# track_path = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if (x.endswith('.dat'))][0]
# track_metadata_path = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if ('meta' in x.lower())][0]