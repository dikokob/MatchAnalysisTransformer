####this is the function that takes the necessary file paths related to a specific game to output a summary file for that game
import os 
import pandas as pd 
import xmltodict
import numpy as np
import json

from scripts_from_fed.opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
from sandbox.crosses_loop_in_tracking_data import crosses_classification

def edge_box_linear(y, x_tresh = 75, y_tresh = 50, x0 = 83):
    if y <= 50:
        y0 = 21.1
    else:
        y0 = 78.9

    b0 = (y_tresh*x0 - x_tresh*y0)/(x0 - x_tresh)
    b1 = (y_tresh - b0)/x_tresh

    x = min([(y - b0)/b1, x0])


    return x


def edge_box_elliptical(y, x_tresh = 75, y0 = 50, x0 = 83, y_tresh = 21.1):
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

def merge_qualifiers(dat: pd.DataFrame) -> pd.DataFrame:
    """extract whether data has qualifier 55

    Args:
        dat (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    dat['qualifier_ids'] = ', '.join([str(int(x)) for x in dat.qualifier_id.tolist()])
    dat['qualifier_values'] = ', '.join([str(x) for x in dat['value'].tolist()])
    if np.any(dat['qualifier_id']==55):
        dat['qualifier_55_value'] = int(dat['value'].loc[dat['qualifier_id']==55].iloc[0])
    else:
        dat['qualifier_55_value'] = 0
    dat = dat.drop(columns=['qualifier_id', 'value']).drop_duplicates()
    return dat

#get all shot events
def get_shots(path_events: str, path_match_results: str) -> pd.DataFrame:
    """Compute the shot events dataframe

    Args:
        path_events (str): path to eventdetails.xml
        path_match_results (str): path to matchresults.xml

    Returns:
        pd.DataFrame: shot events dataframe
    """

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



def corners_classification(path_events, path_match_results, path_track_meta, season, competition):

    #path_events = file_event
    #path_match_results = file_results
    #path_track_meta = file_meta

    opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
    referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results) 
    
    event_type_id = pd.read_excel(os.path.join('data', 'Users','fbettuzzi', 'Desktop', 'Chelsea', 'OPTA','f24 - ID definitions.xlsx'))

    folder_crosses = 'crosses v4'

    ####you can comment this out
    # placing this in the data folder for now.
    advanced_metrics_base_filepath = os.path.join('data','ctgshares', 'Drogba', 'Advanced Data Metrics')
    adv_metrics_season_competition_filepath = os.path.join(advanced_metrics_base_filepath, season, competition)
    # check if any directories with the name "Set pieces & Crosses" are available
    fullpath = os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses')
    if sum(['Set Pieces & Crosses' in x for x in os.listdir(adv_metrics_season_competition_filepath)])==0:
        
        os.mkdir(fullpath)
    
    # check if any directories with the folder crosses name are available
    if sum([folder_crosses in x for x in os.listdir(fullpath)])==0:
        fullpath = os.path.join(fullpath, folder_crosses)
        os.mkdir(fullpath)

    if True:
        fullpath = os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses')
        home_away_detail = "{} v {}".format(home_team_name, away_team_name)
        game_folder_name = 'g{}.xlsx'.format(int(game_id))

        tmp_fullpath = os.path.join(
            fullpath,
            home_away_detail,
            game_folder_name
        )
        data_crosses = crosses_classification(path_events)

        writer = pd.ExcelWriter(
            tmp_fullpath,
            engine='xlsxwriter'
        )
        # print(writer)
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

    
    if sum(['shots' in x for x in os.listdir(os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses'))])==0:
        fullpath = os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses', 'shots')
        os.mkdir(fullpath)

    if True:
    #if sum([home_team_name + ' v ' + away_team_name in x for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces\\shots'.format(season, competition))]) == 0:
        data_shots = get_shots(path_events, path_match_results)

        fullpath = os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses', 'shots')
        home_away_detail = "{} v {}".format(home_team_name, away_team_name)
        game_folder_name = 'g{}.xlsx'.format(int(game_id))
        tmp_fullpath = os.path.join(
            fullpath,
            home_away_detail,
            game_folder_name
        )
        writer = pd.ExcelWriter(
            tmp_fullpath,
            engine='xlsxwriter'
        )
        data_shots.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        print("writer is blank here. Pandas closes the file")
        worksheet = writer.sheets['Sheet1']  # pull worksheet object
        for idx, col in enumerate(data_shots):  # loop through all columns
            series = data_shots[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()
    ####here you can stop commenting out, next line of code is importing the crosses data for the relevant game so you may update the path accordingly

    # make set pieces & cross dir
    fullpath = os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses')
    home_away_detail = "{} v {}".format(home_team_name, away_team_name)
    game_folder_name = 'g{}.xlsx'.format(int(game_id))

    tmp_fullpath = os.path.join(
        fullpath,
        home_away_detail,
        game_folder_name
    )
    data_crosses = pd.read_excel(tmp_fullpath)

    # make set pieces & cross/shots dir
    fullpath = os.path.join(adv_metrics_season_competition_filepath, 'Set Pieces & Crosses', 'shots')
    home_away_detail = "{} v {}".format(home_team_name, away_team_name)
    game_folder_name = 'g{}.xlsx'.format(int(game_id))
    tmp_fullpath = os.path.join(
        fullpath,
        home_away_detail,
        game_folder_name
    )
    data_shots = pd.read_excel(tmp_fullpath)
    
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
        attacking_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==attacking_team_id) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==defending_team_id) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < period_id) | (opta_event_data_df.time_in_seconds < freekick_time_seconds))]['unique_event_id'].unique())    
        defending_team_goals_up_to_freekick_excluded = len(opta_event_data_df[(((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==defending_team_id) & (~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)]))) | ((opta_event_data_df.type_id==16) & (opta_event_data_df.team_id==attacking_team_id) & (opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id[(opta_event_data_df.type_id==16) & (opta_event_data_df.qualifier_id==28)])))) & ((opta_event_data_df.period_id < period_id) | (opta_event_data_df.time_in_seconds < freekick_time_seconds))]['unique_event_id'].unique())    
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
                data_corner_reduced = data_corner[(~data_corner.type_id.isin([6,27,28,43,70]))].reset_index(drop=True)
                if data_corner_reduced.shape[0] == 0:
                    number_events_in_between = 0
                    opta_event_id_between = None
                    opta_type_id_between = None   
                else:
                    number_events_in_between = len(data_corner_reduced.unique_event_id.unique())
                    opta_event_id_between = ', '.join([str(x) for x in data_corner_reduced.unique_event_id.unique()])
                    opta_type_id_between = ', '.join([str(int(x)) for x in data_corner_reduced.drop_duplicates(['unique_event_id']).type_id])
            
            list_events.append(['f' + str(int(game_id)), fixture, attacking_team, defending_team, 't' + str(int(attacking_team_id)), 't' + str(int(defending_team_id)), attacking_team_goals_up_to_freekick_excluded, 
                defending_team_goals_up_to_freekick_excluded, goal_diff_attack_v_defense,
                np.where(goal_diff_attack_v_defense > 0, 'winning', np.where(goal_diff_attack_v_defense == 0, 'drawing', 'losing')).tolist(), np.where(freekick_y_start < 50.0, 'Right', 'Left').tolist(),
                freekick_event_id, period_id, freekick_mins, freekick_secs, freekick_x_start, freekick_y_start, -1, -1, freekick_player_id, freekick_player_name,
                None, None, 'Other', freekick_event_id, freekick_mins, freekick_secs, freekick_x_start, freekick_y_start, -1, -1,
                freekick_player_id, freekick_player_name,
                None, None, outcome_shot,
                None, None, time_between_stop_and_corner, number_events_in_between, opta_event_id_between, 
                None, None, None, None, None, None])

            list_all_shots.append([freekick_event_id, freekick_event_id, freekick_player_id, freekick_player_name, 't' + str(int(attacking_team_id)), 
                attacking_team, 'Corner Shot', outcome_shot, shot_body_part, None])
            
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
            index_longest_pass = np.where((((passes_in_window[:,2] >= 83.0) & ((((passes_in_window[:,3] >= 36.8) & (passes_in_window[:,1] < 50.0)) | (passes_in_window[:,-4]==1)) | (((passes_in_window[:,3] <= 63.2) & (passes_in_window[:,1] > 50)) | (passes_in_window[:,-4]==1))))) | (passes_in_window[:,11]==1) | (passes_in_window[:,-3] > 0) | ((passes_in_window[:,-4] == 1) & (passes_in_window[:,2] >= edge_box_elliptical(passes_in_window[:,3])) & (passes_in_window[:,0] > 88.5) & (np.abs(passes_in_window[:,1] - 50.0) >= 29.0)) | ((passes_in_window[:,-4] == 1) & (passes_in_window[:,9]==freekick_event_id) & (passes_in_window[:,2] >= edge_box_elliptical(passes_in_window[:,3]))))
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
            if passes_in_window[0,2] >= edge_box_elliptical(passes_in_window[0, 3]):
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

        data_corner_time = opta_event_data_df[
            (
                opta_event_data_df.period_id==period_id
            ) & (
                    opta_event_data_df.type_id.isin([6])
            ) & (
                opta_event_data_df.team_id==attacking_team_id
                ) & (
                    opta_event_data_df.time_in_seconds <= freekick_time_seconds
                )
        ]['time_in_seconds'].iloc[-1] #we take most recent event before freekick
        time_between_stop_and_corner = np.round(freekick_time_seconds - data_corner_time, 2)
        data_corner = opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.time_in_seconds > data_corner_time) & (opta_event_data_df.time_in_seconds < freekick_time_seconds)].reset_index(drop=True)
        if len(data_corner.unique_event_id.unique()) == 0:
            number_events_in_between = 0
            opta_event_id_between = None
            opta_type_id_between = None
        else:
            data_corner_reduced = data_corner[(~data_corner.type_id.isin([6,27,28,43,70]))].reset_index(drop=True)
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
            qualifying_shots = data_shots[(data_shots.from_corner==1) & (((data_shots.Time_in_Seconds >= 
                passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.Time_in_Seconds <= 
                10 + passes_in_window[index_longest_pass,-2]*60 + passes_in_window[index_longest_pass,-1]) & (data_shots.period_id==
                period_id) & (data_shots.related_event_team_id==attacking_team_id)) | (((data_shots.value == 
                passes_in_window[index_longest_pass,-5]) | (data_shots.value == 
                passes_in_window[0,-5])) & (data_shots.related_event_team_id == 
                attacking_team_id)))].sort_values(['Time_in_Seconds']).reset_index(drop=True)
            
            if qualifying_shots.shape[0] > 0:
                shot_event_ids = ', '.join([str(int(x)) for x in qualifying_shots.unique_event_id.tolist()])
                shot_player_ids = ', '.join(qualifying_shots.player_id.tolist())
                for shot_event_id in qualifying_shots.unique_event_id.unique():
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



                    list_all_shots.append([freekick_event_id, shot_event_id, shot_player_id, shot_player_name, shot_team_id, 
                        shot_team_name, shot_label, shot_outcome, shot_body_part, aerial_duel_is_shot])

            else:
                shot_event_ids = None 
                shot_player_ids = None

            where_relevant_pass = np.where(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])[0][-1]
            events_after_relevant_pass = opta_event_data_df[(~opta_event_data_df.unique_event_id.isin(opta_event_data_df.unique_event_id.iloc[:(where_relevant_pass+1)].unique())) & (opta_event_data_df.time_in_seconds >= 
                passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]) & (opta_event_data_df.time_in_seconds <=
                10 + passes_in_window[index_longest_pass, -2]*60 + passes_in_window[index_longest_pass,-1]) & (opta_event_data_df.period_id ==
                period_id) & (~opta_event_data_df.type_id.isin([27,28,55,43,70])) & (opta_event_data_df.unique_event_id != 
                freekick_event_id) & (opta_event_data_df.unique_event_id != passes_in_window[index_longest_pass,9])].reset_index(drop=True)

            first_contact_aerial = 0
            description = None
            if events_after_relevant_pass.shape[0] > 0:
                if events_after_relevant_pass.type_id.iloc[0] in [1,2,3,4,7,10,11,13,14,15,16,8,12,41,44,45,49,50,51,52,53,54,56,59,61,74]:
                    if (3 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (15 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()) | (168 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                        first_contact_aerial = 1
                    if events_after_relevant_pass.type_id.iloc[0] not in [4, 44]:
                        if events_after_relevant_pass.type_id.iloc[0] in event_type_id.type_id.tolist():
                            if (events_after_relevant_pass.type_id.iloc[0] == 12) & (15 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                description = 'Headed Clearance'
                            elif (events_after_relevant_pass.type_id.iloc[0] == 16) & (28 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]].qualifier_id.tolist()):
                                description = 'Own Goal'
                            else:
                                description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0]
                        first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                        if np.isnan(events_after_relevant_pass.player_id.iloc[0]):
                            first_contact_player_id = None 
                            first_contact_player_name = None 
                        else:
                            first_contact_player_id = 'p' + str(int(events_after_relevant_pass.player_id.iloc[0]))
                            first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                        first_contact_team_id = 't' + str(int(events_after_relevant_pass.team_id.iloc[0]))
                        first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                            attacking_team, defending_team).tolist()
                    else:
                        if events_after_relevant_pass.type_id.iloc[0] == 44:
                            first_contact_aerial = 1
                            description = 'Aerial Duel Won'
                            first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                            first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['player_id'].iloc[0]))
                            first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==1]['team_id'].iloc[0]))
                            first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                            first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                attacking_team, defending_team).tolist()
                        if events_after_relevant_pass.type_id.iloc[0] == 4:
                            if (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (10 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]                             
                                description = 'Handball'
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                    attacking_team, defending_team).tolist()                        
                            elif (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (313 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                first_contact_type = None 
                                first_contact_player_id = None 
                                first_contact_player_name = None 
                                first_contact_team_id = None 
                                first_contact_team_name = None 
                                first_contact_aerial = None 
                            elif (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.unique_event_id.iloc[0]]['qualifier_id'].tolist()) | (132 in events_after_relevant_pass[events_after_relevant_pass.unique_event_id==events_after_relevant_pass.drop_duplicates(['unique_event_id']).unique_event_id.iloc[1]]['qualifier_id'].tolist()):
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0]                             
                                description = 'Simulation'
                                first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                                first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))   
                                first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                                first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                                    attacking_team, defending_team).tolist()  
                            else:
                                first_contact_type = events_after_relevant_pass.type_id.iloc[0] 
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

                else:
                    first_contact_type = None 
                    first_contact_player_id = None 
                    first_contact_player_name = None 
                    first_contact_team_id = None 
                    first_contact_team_name = None 
                    first_contact_aerial = None 
                    if (passes_in_window[index_longest_pass,5]==2):
                        first_contact_type = 55
                        description = 'Player Caught Offside'
                        first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                        first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])]['team_id'].iloc[0]))
                        first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                        first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                            attacking_team, defending_team).tolist()
                    if (events_after_relevant_pass.type_id.iloc[0] in [5,6]):
                        first_contact_type = events_after_relevant_pass.type_id.iloc[0]
                        description = event_type_id[event_type_id.type_id==events_after_relevant_pass.type_id.iloc[0]]['name'].iloc[0]
                        if np.isnan(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]):
                            first_contact_player_id = None 
                            first_contact_player_name = None 
                        else:
                            first_contact_player_id = 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['player_id'].iloc[0]))
                            first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                        first_contact_team_id = 't' + str(int(events_after_relevant_pass[events_after_relevant_pass.outcome==0]['team_id'].iloc[0]))
                        first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                            attacking_team, defending_team).tolist()

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

                        if (shot_player_ids is not None) & (qualifying_shots.shape[0] > 0):
                            if (successful_player_id_duel == shot_player_ids.split(', ')[0]) & (- events_after_relevant_pass.time_in_seconds.iloc[0] + qualifying_shots.Time_in_Seconds.iloc[0] <= 1) & (15 in list(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==int(shot_event_ids.split(', ')[0])].qualifier_id)):
                                aerial_duel_is_shot = 1

                        if 'p' + str(int(events_after_relevant_pass[events_after_relevant_pass.unique_event_id==aerial_duel_id]['player_id'].iloc[0])) == successful_player_id_duel:
                            list_aerial_duels.append([freekick_event_id, aerial_duel_id, successful_player_id_duel, 
                                successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, 'Successful', unsuccessful_player_id_duel, 
                                unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, 
                                aerial_duel_is_shot])
                        else:
                            list_aerial_duels.append([freekick_event_id, aerial_duel_id, unsuccessful_player_id_duel, 
                                unsuccessful_player_name_duel, unsuccessful_team_id_duel, unsuccessful_team_name_duel, 'Unsuccessful', successful_player_id_duel, 
                                successful_player_name_duel, successful_team_id_duel, successful_team_name_duel, 
                                aerial_duel_is_shot])


            else:
                first_contact_type = None 
                first_contact_player_id = None 
                first_contact_player_name = None 
                first_contact_team_id = None 
                first_contact_team_name = None 
                first_contact_aerial = None
                if (passes_in_window[index_longest_pass,5]==2):
                    first_contact_type = 55
                    description = 'Player Caught Offside'
                    first_contact_player_id = 'p' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9]) & (opta_event_data_df.qualifier_id==7)]['value'].iloc[0]))
                    first_contact_team_id = 't' + str(int(opta_event_data_df[(opta_event_data_df.unique_event_id==passes_in_window[index_longest_pass,9])]['team_id'].iloc[0]))
                    first_contact_player_name = player_names_raw[player_names_raw['@uID']==first_contact_player_id]['player_name'].iloc[0]
                    first_contact_team_name = np.where(first_contact_team_id == 't' + str(int(attacking_team_id)), 
                        attacking_team, defending_team).tolist()


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
            first_contact_type, description, first_contact_player_id, first_contact_player_name, 
            first_contact_team_id, first_contact_team_name, first_contact_aerial, goalkeeper_id, gk_name])

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
        'First Contact Type', 'First Contact Explanation', 'First Contact Player ID', 'First Contact Player Name', 
        'First Contact Team ID', 'First Contact Team Name', 'First Contact Aerial', 'Defending Goalkeeper ID', 'Defending Goalkeeper Name'])
    summary_df['Set Piece Type'] = 'Corner'

    summary_df_all_shots = pd.DataFrame(list_all_shots, columns = 
        ['Set Piece OPTA Event ID', 'Shot OPTA ID', 'Shot Player ID', 'Shot Player Name', 
        'Shot Team ID', 'Shot Team Name', 'Shot Occurrence', 'Shot Outcome', 'Shot Body Part', 'Aerial Duel Is Shot'])

    summary_df_aerial_duels = pd.DataFrame(list_aerial_duels, columns = 
        ['Set Piece OPTA Event ID', 'Aerial Duel OPTA ID', 
        'Aerial Duel Player ID', 'Aerial Duel Player Name', 
        'Aerial Duel Team ID', 'Aerial Duel Team Name', 'Successful/Unsuccessful',
        'Other Aerial Duel Player ID', 'Other Aerial Duel Player Name', 
        'Other Aerial Duel Team ID', 'Other Aerial Duel Team Name',
        'Aerial Duel Is Shot'])

    if np.any(summary_df_aerial_duels['Aerial Duel Is Shot']==1):
        for set_piece in summary_df_aerial_duels[summary_df_aerial_duels['Aerial Duel Is Shot']==1].drop_duplicates(['Set Piece OPTA Event ID'])['Set Piece OPTA Event ID']:
            summary_df_all_shots.loc[(summary_df_all_shots['Set Piece OPTA Event ID']==set_piece) & (summary_df_all_shots['Shot OPTA ID']==summary_df_all_shots[summary_df_all_shots['Set Piece OPTA Event ID']==set_piece]['Shot OPTA ID'].iloc[0]), 'Aerial Duel Is Shot'] = 1
    
    return summary_df.reset_index(drop=True), summary_df_all_shots.reset_index(drop=True), summary_df_aerial_duels.reset_index(drop=True)
