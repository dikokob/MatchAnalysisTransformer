####this is the function that takes the necessary file paths related to a specific game to output a summary file for that game
import os 
import pandas as pd 
import xmltodict
import numpy as np

from scripts_from_fed.opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation


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
