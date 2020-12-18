import os 
import pandas as pd 
import xmltodict
import numpy as np 
import time

#os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies

#from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation

def opta_event_file_manipulation (path_events):
    import xmltodict
    import numpy as np
    import pandas as pd

    try:
        with open(path_events, encoding = 'utf-8') as fd:
            opta_event = xmltodict.parse(fd.read())
    except UnicodeDecodeError:
        with open(path_events, encoding = 'latin-1') as fd:
            opta_event = xmltodict.parse(fd.read())    

    if 'f73' in path_events:
        opta_event['Games'] = opta_event.pop('SoccerFeed')    
    
    #start rearranging the file in a nicer format
    opta_event_data = []
    competition_id = int(opta_event['Games']['Game']['@competition_id'])
    competition_name = opta_event['Games']['Game']['@competition_name']
    season_id = int(opta_event['Games']['Game']['@season_id'])
    season_name = opta_event['Games']['Game']['@season_name']
    game_id = int(opta_event['Games']['Game']['@id'])
    match_day = int(opta_event['Games']['Game']['@matchday'])
    game_date = opta_event['Games']['Game']['@game_date']
    period_1_start = opta_event['Games']['Game']['@period_1_start']
    period_2_start = opta_event['Games']['Game']['@period_2_start']
    away_score = None
    if '@away_score' in list(opta_event['Games']['Game'].keys()):
        away_score = int(opta_event['Games']['Game']['@away_score'])
    away_team_id = int(opta_event['Games']['Game']['@away_team_id'])
    away_team_name = opta_event['Games']['Game']['@away_team_name']
    home_score = None
    if '@home_score' in list(opta_event['Games']['Game'].keys()):
        home_score = int(opta_event['Games']['Game']['@home_score'])
    home_team_id = int(opta_event['Games']['Game']['@home_team_id'])
    home_team_name = opta_event['Games']['Game']['@home_team_name']
    fixture = home_team_name + ' v ' + away_team_name
    result = None
    if (home_score is not None) & (away_score is not None):
        result = str(home_score) + ' - ' + str(away_score)

    #loop over events in the opta file
    for i in range(len(opta_event['Games']['Game']['Event'])):
        unique_event_id = int(opta_event['Games']['Game']['Event'][i]['@id'])
        event_id = int(opta_event['Games']['Game']['Event'][i]['@event_id'])
        type_id = int(opta_event['Games']['Game']['Event'][i]['@type_id'])
        period_id = int(opta_event['Games']['Game']['Event'][i]['@period_id'])
        mins = int(opta_event['Games']['Game']['Event'][i]['@min'])
        sec = int(opta_event['Games']['Game']['Event'][i]['@sec'])
        team_id = int(opta_event['Games']['Game']['Event'][i]['@team_id'])
        outcome = int(opta_event['Games']['Game']['Event'][i]['@outcome'])
        #we need to check conditions for existence of player_id, keypass and assist
        if len(set(list(opta_event['Games']['Game']['Event'][i].keys())).intersection(set(['@player_id']))) == 1:
            player_id = int(opta_event['Games']['Game']['Event'][i]['@player_id'])
        else:
            player_id = None
        if len(set(list(opta_event['Games']['Game']['Event'][i].keys())).intersection(set(['@keypass']))) == 1:
            keypass = 1
        else:
            keypass = 0
        if len(set(list(opta_event['Games']['Game']['Event'][i].keys())).intersection(set(['@assist']))) == 1:
            assist = 1
        else:
            assist = 0
        x = np.round(float(opta_event['Games']['Game']['Event'][i]['@x']), 1)
        y = np.round(float(opta_event['Games']['Game']['Event'][i]['@y']), 1)
        timestamp = opta_event['Games']['Game']['Event'][i]['@timestamp']

        #inner loop over qualifiers - if there is only one qualifier then we go straight with the dict like approach
        #we need first to check whether there are any qualifiers
        if len(set(list(opta_event['Games']['Game']['Event'][i].keys())).intersection(set(['Q']))) == 0:
            qualifier_id = None
            value = None
            opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day, 
                game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name, 
                away_score, away_team_id, away_team_name, fixture, result, 
                unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id, outcome, keypass, assist,
                x, y, qualifier_id, value])
        else:
            if type(opta_event['Games']['Game']['Event'][i]['Q']) != list:
                qualifier_id = int(opta_event['Games']['Game']['Event'][i]['Q']['@qualifier_id'])
                if len(set(list(opta_event['Games']['Game']['Event'][i]['Q'].keys())).intersection(set(['@value']))) == 1:
                    value = opta_event['Games']['Game']['Event'][i]['Q']['@value']
                else:
                    value = None
            
                opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day, 
                    game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name, 
                    away_score, away_team_id, away_team_name, fixture, result, 
                    unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id, outcome, keypass, assist,
                    x, y, qualifier_id, value])
            else:
                for j in range(len(opta_event['Games']['Game']['Event'][i]['Q'])):
                    qualifier_id = int(opta_event['Games']['Game']['Event'][i]['Q'][j]['@qualifier_id'])
                    #check whether '@value' is present or not
                    if len(set(list(opta_event['Games']['Game']['Event'][i]['Q'][j].keys())).intersection(set(['@value']))) == 1: #value present
                        value = opta_event['Games']['Game']['Event'][i]['Q'][j]['@value']
                    else:
                        value = None

                    #append everything
                    opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day, 
                        game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name, 
                        away_score, away_team_id, away_team_name, fixture, result, 
                        unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id, outcome, keypass, assist,
                        x, y, qualifier_id, value])

    #convert the list to a pandas dataframe
    opta_event_data_df = pd.DataFrame(opta_event_data, index = None, columns = ['competition_id', 'competition_name', 'season_id', 'season_name', 'game_id', 'match_day', 
                        'game_date', 'period_1_start', 'period_2_start', 'home_score', 'home_team_id', 'home_team_name', 
                        'away_score', 'away_team_id', 'away_team_name', 'fixture', 'result', 
                        'unique_event_id', 'event_id', 'period_id', 'min', 'sec', 'timestamp', 'type_id', 'player_id', 'team_id', 'outcome', 'keypass', 'assist',
                        'x', 'y', 'qualifier_id', 'value'])

    return (opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name)

while sum(['dummy' in x for y in os.listdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21')) for x in y.split(' ')]) > 0:
    time.sleep(5)
    pass

open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'integration with f73 data dummy.txt', 'w').close()

#get all shot events
def get_shots(path_events, path_match_results):
    import pandas as pd
    import numpy as np 
    import xmltodict

    def merge_qualifiers (dat):
        dat['qualifier_ids'] = ', '.join([str(int(x)) for x in dat.qualifier_id.tolist()])
        dat['qualifier_values'] = ', '.join([str(x) for x in dat['value'].tolist()])
        if np.any(dat['qualifier_id']==55):
            dat['qualifier_55_value'] = int(dat['value'].loc[dat['qualifier_id']==55].iloc[0])
        else:
            dat['qualifier_55_value'] = 0
        dat = dat.drop(columns=['qualifier_id', 'value']).drop_duplicates()
        return dat

    data, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
    referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results)
    data['Time_in_Seconds'] = data['min']*60.0 + data['sec']
    data['player_id'] = data['player_id'].fillna(0)
    #data['player_id'] = ['p' + str(int(x)) for x in data.player_id]
    data['player_id'] = list(map(lambda x: 'p' + str(int(x)), data.player_id))
    data['player_name'] = [player_names_raw.player_name.loc[player_names_raw['@uID']==x].iloc[0] if x in player_names_raw['@uID'].tolist() else None for x in data.player_id]
    

    data_shots = data[((data.period_id==1) | (data.period_id==2) | (data.period_id==3) | (data.period_id==4)) & (data.type_id.isin([13,14,15,16,60]))].reset_index(drop = True)    
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
        #data_output = data_output.groupby([x for x in data_output.columns if x not in ['qualifier_id', 'value']]).apply(merge_qualifiers).reset_index(drop = True) #weird behaviour by groupby apply function, we need to use a loop then
        data_output_grouped = data_shots.groupby([x for x in data_shots.columns if (x not in ['qualifier_id', 'value']) & (data_shots[x].count() > 0)])
        list_output = []
        for index, df in data_output_grouped:
            list_output.append(merge_qualifiers(df.reset_index(drop = True)))
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

    data_shots['related_event_team_id'] = np.where(data_shots['own_goal'] == 0, data_shots['team_id'], np.where(data_shots['team_id']==home_team_id, away_team_id, home_team_id))
    
    data_shots['left_foot'] = [int(72 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
    data_shots['right_foot'] = [int(20 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
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
    data_shots['own_goal'] = [int(28 in [int(y) for y in x.split(', ')]) for x in data_shots['qualifier_ids']]
    data_shots['own_goal'] = np.where((data_shots['own_goal']==1) & (data_shots['goal']==1), 1, 0)
    data_shots['chance_missed'] = np.where(data_shots['type_id'] == 60, 1, 0)

    data_shots['direct_shot'] = np.where(data_shots['value'] > 0, 1, 0)



    return data_shots

#seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
seasons = ['2020-21']
for season in seasons:
    parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}'.format(season)
    #folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x))]
    folders_to_keep = ['Premier League']
    #folders_to_keep = ['Champions League']
    #folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x)) & (x != 'Premier League')]

    for competition in folders_to_keep:

        #shots data from set pieces and crosses
        shots_set_pieces = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Set Pieces with 2nd Phase Output.xlsx'.format(season, competition))
        shots_crosses = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Crosses Output.xlsx'.format(season, competition))
        
        if 'Opta f73 Files' not in os.listdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\{}'.format(season, competition)):
            shots_set_pieces['xG'] = None 
            shots_set_pieces['xGOT'] = None 
            shots_set_pieces['Shot Pressure'] = None 
            shots_set_pieces['Shot Clarity'] = None
            shots_crosses['xG'] = None 
            shots_crosses['xGOT'] = None 
            shots_crosses['Shot Pressure'] = None 
            shots_crosses['Shot Clarity'] = None 
        
        else:
            #f73 data
            path_f73 = '\\\ctgshares\\Drogba\\API Data Files\\{}\\{}\\Opta f73 Files'.format(season, competition)
            data_f73 = pd.concat([opta_event_file_manipulation(os.path.join(path_f73, x))[0] for x in os.listdir(path_f73)]).reset_index(drop=True)

            #get xg and xgot shot data separately
            xg_data = data_f73[data_f73.qualifier_id==321].reset_index(drop=True)
            xgot_data = data_f73[data_f73.qualifier_id==322].reset_index(drop=True)
            shot_pressure_data = data_f73[data_f73.qualifier_id==326].reset_index(drop=True)
            shot_clarity_data = data_f73[data_f73.qualifier_id==327].reset_index(drop=True)
            xg_data['value'] = xg_data['value'].astype(float)
            xgot_data['value'] = xgot_data['value'].astype(float)
            shot_pressure_data['value'] = shot_pressure_data['value'].astype(int)
            shot_clarity_data['value'] = shot_clarity_data['value'].astype(int)

            # #apply the get_shots function to merge with additional shots info
            # competition_folder = os.path.join(parent_folder, competition)
            # subfolders_to_keep = [x for x in os.listdir(competition_folder) if ('spectrum' not in x) & ('f73' not in x)]
            # list_shots = []

            # #we need to check whether we have already reached the game level folders or we are still at a higher level
            # if sum([x.startswith('g') for x in subfolders_to_keep]) > 0: #if true, we already hit the game level folders
            #     game_folder_collections = [competition]
            # else:
            #     game_folder_collections = subfolders_to_keep

            # for folder in game_folder_collections:
            #     if folder == competition: #if true, it means we need to climb a level less 
            #         path_folder = competition_folder
            #     else:
            #         path_folder = os.path.join(competition_folder, folder)
            #     subfolders = os.listdir(path_folder)

            #     for sub in subfolders:
            #         file_event = [os.path.join(path_folder, sub, x) for x in os.listdir(os.path.join(path_folder, sub)) if (x.endswith('.xml')) & ('f24' in x)][0]
            #         file_results = [os.path.join(path_folder, sub, x) for x in os.listdir(os.path.join(path_folder, sub)) if (x.endswith('.xml')) & ('srml' in x)][0]
            #         list_shots.append(get_shots(file_event, file_results))

            # data_event_shots = pd.concat(list_shots).reset_index(drop=True)


            #merge f73 with shots outputs where relevant
            shots_set_pieces = shots_set_pieces.merge(xg_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'xG'}).drop(['unique_event_id'], 
                axis = 1).merge(xgot_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'xGOT'}).drop(['unique_event_id'], 
                axis = 1).merge(shot_pressure_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'Shot Pressure'}).drop(['unique_event_id'], 
                axis = 1).merge(shot_clarity_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'Shot Clarity'}).drop(['unique_event_id'], 
                axis = 1).reset_index(drop=True)

# .merge(data_event_shots[['unique_event_id', 'big_chance', 
#                 'not_reaching_goal_line', 'saved_off_line', 'deflected']], how = 'left', left_on = ['Shot OPTA ID'], 
#                 right_on = ['unique_event_id']).drop(['unique_event_id'], 
#                 axis = 1).

            shots_crosses = shots_crosses.merge(xg_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'xG'}).drop(['unique_event_id'], 
                axis = 1).merge(xgot_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'xGOT'}).drop(['unique_event_id'], 
                axis = 1).merge(shot_pressure_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'Shot Pressure'}).drop(['unique_event_id'], 
                axis = 1).merge(shot_clarity_data[['unique_event_id', 'value']], how = 'left', left_on = ['Shot OPTA ID'], 
                right_on = ['unique_event_id']).rename(columns = {'value': 'Shot Clarity'}).drop(['unique_event_id'], 
                axis = 1).reset_index(drop=True)

# .merge(data_event_shots[['unique_event_id', 'big_chance', 
#                 'not_reaching_goal_line', 'saved_off_line', 'deflected']], how = 'left', left_on = ['Shot OPTA ID'], 
#                 right_on = ['unique_event_id']).drop(['unique_event_id'], 
#                 axis = 1)

            # #here we see that there are two shots (not being own goals or chance missed) that don't have an xG value. These two shots are both not reaching the goal line
            # shots_set_pieces[(shots_set_pieces.xG.isnull()) & (~shots_set_pieces['Shot Outcome'].isin(['Own Goal', 'Chance Missed']))]
            # shots_crosses[(shots_crosses.xG.isnull()) & (~shots_crosses['Shot Outcome'].isin(['Own Goal', 'Chance Missed']))]

            # shots_set_pieces[(shots_set_pieces.xGOT.isnull()) & (shots_set_pieces['Shot Outcome'].isin(['On Target', 'Goal']))]
            # shots_crosses[(shots_crosses.xGOT.isnull()) & (shots_crosses['Shot Outcome'].isin(['On Target', 'Goal']))]


        writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Set Pieces with 2nd Phase Output with f73 Integration.xlsx'.format(season, competition), engine='xlsxwriter')
        #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\set pieces classification\\new folder for Tom\\Outside Target Zone Free Kicks due to distance with ball clause.xlsx', engine='xlsxwriter')
        shots_set_pieces.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        worksheet = writer.sheets['Sheet1']  # pull worksheet object
        for idx, col in enumerate(shots_set_pieces):  # loop through all columns
            series = shots_set_pieces[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()


        writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Crosses Output with f73 Integration.xlsx'.format(season, competition), engine='xlsxwriter')
        #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\set pieces classification\\new folder for Tom\\Outside Target Zone Free Kicks due to distance with ball clause.xlsx', engine='xlsxwriter')
        shots_crosses.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        worksheet = writer.sheets['Sheet1']  # pull worksheet object
        for idx, col in enumerate(shots_crosses):  # loop through all columns
            series = shots_crosses[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()


os.remove('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'integration with f73 data dummy.txt') 