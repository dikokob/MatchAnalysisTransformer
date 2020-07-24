import os 

#os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies
os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")

from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation


def only_open_play_crosses(data):
    import pandas as pd 
    import numpy as np 
    #import xmltodict

    #if we have a pass
    if (data.type_id.unique()[0]==1) | (data.type_id.unique()[0]==2): 
        #if we have a cross
    #    if 2 in data.qualifier_id.tolist():
            #if the cross does not involve set pieces/corners
        if len(set([5,6]).intersection(set(data.qualifier_id.tolist()))) == 0:
            data['open_play_cross'] = 1
        #if the cross involves set pieces/corners
        else:
            #if the cross involves a corner
            if (len(set([6]).intersection(set(data.qualifier_id.tolist()))) > 0) & (len(set([5]).intersection(set(data.qualifier_id.tolist()))) == 0):
                data['open_play_cross'] = 3
            #if the cross involves a set piece
            else:
                data['open_play_cross'] = 2
        #if the pass is not qualified as a cross
    #    else:
            #if the pass does not involve set pieces
            # if len(set([5,6,24,25,26,97]).intersection(set(data.qualifier_id.tolist()))) == 0:
            #     data['open_play_cross'] = 1
            # #if the pass involves set pieces/corners
            # else:
            #     #if the pass involves corners
            #     if (len(set([6,25]).intersection(set(data.qualifier_id.tolist()))) > 0) & (len(set([5,24,26,97]).intersection(set(data.qualifier_id.tolist()))) == 0):
            #         data['open_play_cross'] = 3
            #     #if the pass involves set pieces
            #     else:
            #         data['open_play_cross'] = 2
    #if the event is not a pass
    else:
        #if the event involves set corners
        if ((6 in data.type_id.tolist()) | (len(set([6]).intersection(set(data.qualifier_id.tolist()))) > 0)) & (len(set([5]).intersection(set(data.qualifier_id.tolist()))) == 0):
            data['open_play_cross'] = 3
        #if the event does not involve corners
        else:
            #if the event is a foul
            if (4 in data.type_id.tolist()) | (13 in data.qualifier_id.tolist()):
                data['open_play_cross'] = 4
            #if the event is not a foul
            else:
                #if the event is related to a set piece
                if len(set([5]).intersection(set(data.qualifier_id.tolist()))) > 0:
                    data['open_play_cross'] = 2
                #if the event is not related to a set piece
                else:
                    data['open_play_cross'] = 0
    return data

#group by is done by event id/team id because event counts is done separately by team, not overall
#event_data_open_play_crosses = event_data_filtered[(event_data_filtered.type_id==1) & (event_data_filtered.qualifier_id.isin([2,6,24]))].groupby(['event_id', 'team_id']).apply(only_open_play_crosses)
#event_data_open_play_crosses_final = event_data_open_play_crosses[event_data_open_play_crosses.open_play_cross==1][['event_id', 'period_id', 'team_id', 'player_id', 'outcome', 'keypass', 'assist', 'x', 'y', 'x_updated', 'y_updated', 'x_new', 'y_new', 'Time_in_Seconds', 'total_seconds_in_period']].fillna(value = {'keypass': 0, 'assist': 0}) #replace also missing values with 0 for keypass and assist column


#loop to check which open play crosses are not actually open play
#games = list(map(int, event_data_open_play_crosses[event_data_open_play_crosses.open_play_cross==1].game_id.tolist()))

#we can actually use part of this function to simulate crosses, i.e. see whether certain situations will be classified as crosses or not (pretty much we are using that as a rule-based model, with our arbitrary rules)
import pandas as pd
import numpy as np 
#import xmltodict

def cross_label (data_output):
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, #if there is cross qualifier then 1
        np.where(data_output['x'] < 60.0, 0, #if the pas starts from behind the halfway line then 0
            np.where((data_output['y'] > 100/3.0) & (data_output['y'] < 100/1.5), 0, #if the pass starts too frontal then 0
                np.where(data_output['x_end'] < 79.0, 0, #if the pass ends too behind then 0
                    np.where((np.abs(data_output['y'] - 50.0) < np.abs(data_output['y_end'] - 50.0)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, #if the pass is directed wide then 0
                        np.where((np.abs(data_output['y'] - data_output['y_end']) < 25.0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)), 0, 1))))))
                            #np.where((data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0, 1))))))) #if the ball ends up too wide then 0

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0, data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x'] < 75.0) | ((data_output['y'] < 0.5*21.1) | (data_output['y'] > 0.5*(100+78.9)))), 0, data_output['Our Cross Qualifier'])) #low passes should start not too behind or not too wide
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 1.05*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] < 70.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.75*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] < 80.0) & (data_output['x'] >= 70.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier'])) #we set an upper bound on the distance travelled on the x axis proportional to the distance travelled on the y axis
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.5*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] >= 80.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier']))

    
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (data_output['pass_situation']=='open play') & ((data_output['x_end'] < 83.0) | ((data_output['y_end'] > 78.9) | (data_output['y_end'] < 21.1))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (data_output['pass_situation']=='open play') & (data_output['x'] < 83.0) & (data_output['y'] >= 21.1) & (data_output['y'] <= 78.9), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['pass_situation'] != 'open play') & (data_output['chipped_pass']==0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)) & (((data_output['y_end'] < 100/3.0) & (data_output['y'] < 100/3.0)) | ((data_output['y_end'] > 100/1.5) & (data_output['y'] > 100/1.5))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from set piece situation not being wide enough and ending outside box are not crosses

    #data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
    #    np.where((data_output['x'] < 83.0) & (data_output['x_end'] < 83.0), 0, 
    #        np.where((data_output['x'] < 83.0) & (data_output['chipped_pass']==0), 0, data_output['Our Cross Qualifier']))) #any pass starting and ending before box horizontal line is not a cross and any low pass starting before the box horizontal line is not a cross as well
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['y_end'] > 63.2) | (data_output['y_end'] < 36.8)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, 
            data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((((data_output['y'] <= 78.9) & (data_output['y'] > 100/1.5)) | ((data_output['y'] < 100/3.0) & (data_output['y'] >= 21.1))) & (data_output['x'] < 83.0) & ((data_output['y_end'] >= 100/1.5) | (data_output['y_end'] <= 100/3.0)), 
            0, data_output['Our Cross Qualifier']))
    #data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
    #    np.where((data_output['chipped_pass']==0) & (data_output['x'] < 94.2) & (data_output['x_end'] < 83.0) & (data_output['pass_situation']=='open play'), 0, data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['pass_situation']=='open play') & (data_output['x'] < 82.0) & (data_output['x_end'] < 88.5), 0, data_output['Our Cross Qualifier']))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['x'] >= 83.0) & (data_output['x'] < 88.5) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 20.0), 0, 
            np.where((data_output['x'] >= 88.5) & (data_output['x'] < 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 15.0), 0,
                np.where((data_output['x'] >= 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 10.0), 0, 
                    data_output['Our Cross Qualifier']))))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['Headed Pass']==1) | (data_output['throw_in_taken']==1)), 0, data_output['Our Cross Qualifier'])) # if the pass is headed then 0

    # data_output['cutback_cross'] = np.where(data_output['Our Cross Qualifier']==0, 0, 
    #     np.where(data_output['pass_situation'] != 'open play', 0,
    #         np.where(data_output['chipped_pass']==1, 0, 
    #             np.where(data_output['x'] <= 88.5, 0, 
    #                 np.where((data_output['y'] < 0.5*21.1) | (data_output['y'] > 0.5*(100+78.9)), 0, 
    #                     np.where((data_output['y_end'] > 63.2) | (data_output['y_end'] < 36.8), 0, 
    #                         np.where(data_output['x_end'] < 83, 0, 
    #                             np.where(data_output['x_end'] - data_output['x'] > -5.0, 0, 1))))))))

    data_output['cutback'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['Our Cross Qualifier'] = np.where(data_output['cutback']==1, 1, data_output['Our Cross Qualifier'])
    data_output['cutback'] = np.where(data_output['x'] < 83.0, 0, 
        np.where((data_output['y'] >= 95.0) | (data_output['y'] <= 5.0), 0, 
            np.where(data_output['cutback']==1, 1, 
                np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] >= 88.5) & (data_output['x'] <= 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
                    1, 
                    np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] > 94.2) & (data_output['x_end'] < 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
                        1, 0)))))
    data_output['cutback'] = np.where((data_output['cutback']==1) & (data_output['chipped_pass']==1) & (data_output['x'] - data_output['x_end'] < 5.0) & (np.array([int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']])==0), 
        0, data_output['cutback'])

    return data_output

#dummy_data = pd.Series({'OPTA Cross Qualifier': 0, 'x': 64, 'y': 29, 'x_end': 94, 'y_end': 59, 'blocked_pass': 0, 'pass_situation': 'free kick', 'chipped_pass': 0, 'Headed Pass': 0, 'throw_in_taken': 0, 'qualifier_ids': '0'}).to_frame().T
#cross_label(dummy_data)

def crosses_classification (path_events):
    import pandas as pd
    import numpy as np 
    import xmltodict

    def merge_qualifiers (dat):
        dat['qualifier_ids'] = ', '.join([str(int(x)) for x in dat.qualifier_id.tolist()])
        dat['qualifier_values'] = ', '.join([str(x) for x in dat['value'].tolist()])
        dat = dat.drop(columns=['qualifier_id', 'value']).drop_duplicates()
        return dat

    data, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
    data['Time_in_Seconds'] = data['min']*60.0 + data['sec']
    event_data_open_play_crosses_big = data.groupby(['unique_event_id']).apply(only_open_play_crosses)#.drop_duplicates(['unique_event_id'])

    event_data_open_play_crosses = event_data_open_play_crosses_big.drop_duplicates(['unique_event_id']).reset_index(drop = True)

    # events = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id==1) & (event_data_open_play_crosses_big.qualifier_id==2) & (event_data_open_play_crosses_big.open_play_cross==1)].unique_event_id.tolist()))
    # teams = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id==1) & (event_data_open_play_crosses_big.qualifier_id==2) & (event_data_open_play_crosses_big.open_play_cross==1)].team_id.tolist()))
    # periods = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id==1) & (event_data_open_play_crosses_big.qualifier_id==2) & (event_data_open_play_crosses_big.open_play_cross==1)].period_id.tolist()))

    # events = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id==1) & (~event_data_open_play_crosses_big.unique_event_id.isin(event_data_open_play_crosses_big.unique_event_id.loc[event_data_open_play_crosses_big.qualifier_id==2].tolist())) & (event_data_open_play_crosses_big.qualifier_id==155) & (event_data_open_play_crosses_big.open_play_cross==1) & (event_data_open_play_crosses_big.x >= 100/1.5) & ((event_data_open_play_crosses_big.y > 78.9) | (event_data_open_play_crosses_big.y < 21.1))].unique_event_id.tolist()))
    # teams = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id==1) & (~event_data_open_play_crosses_big.unique_event_id.isin(event_data_open_play_crosses_big.unique_event_id.loc[event_data_open_play_crosses_big.qualifier_id==2].tolist())) & (event_data_open_play_crosses_big.qualifier_id==155) & (event_data_open_play_crosses_big.open_play_cross==1) & (event_data_open_play_crosses_big.x >= 100/1.5) & ((event_data_open_play_crosses_big.y > 78.9) | (event_data_open_play_crosses_big.y < 21.1))].team_id.tolist()))
    # periods = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id==1) & (~event_data_open_play_crosses_big.unique_event_id.isin(event_data_open_play_crosses_big.unique_event_id.loc[event_data_open_play_crosses_big.qualifier_id==2].tolist())) & (event_data_open_play_crosses_big.qualifier_id==155) & (event_data_open_play_crosses_big.open_play_cross==1) & (event_data_open_play_crosses_big.x >= 100/1.5) & ((event_data_open_play_crosses_big.y > 78.9) | (event_data_open_play_crosses_big.y < 21.1))].period_id.tolist()))

    events = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id.isin([1,2])) & (event_data_open_play_crosses_big.open_play_cross==1)].unique_event_id.tolist()))
    teams = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id.isin([1,2])) & (event_data_open_play_crosses_big.open_play_cross==1)].team_id.tolist()))
    periods = list(map(int, event_data_open_play_crosses_big[(event_data_open_play_crosses_big.type_id.isin([1,2])) & (event_data_open_play_crosses_big.open_play_cross==1)].period_id.tolist()))

    for i in range(len(events)):
        current_time = np.round(event_data_open_play_crosses[event_data_open_play_crosses.unique_event_id==events[i]]['Time_in_Seconds'].iloc[0], 2)
        event_data_window = event_data_open_play_crosses[(event_data_open_play_crosses.period_id==periods[i]) & (event_data_open_play_crosses.team_id==teams[i]) & (event_data_open_play_crosses.Time_in_Seconds >= current_time - 10.0) & (event_data_open_play_crosses.Time_in_Seconds < current_time)]
        #if in the previous 10 secs window we have an event related to a set piece or a corner
        if len(set([2,3]).intersection(set(event_data_window.open_play_cross))) > 0:
            #if there is a corner event in the previous 10 secs window
            if np.any(event_data_window.open_play_cross==3):
                event_data_open_play_crosses.loc[(event_data_open_play_crosses.unique_event_id==events[i]), 'open_play_cross'] = 6
            else: 
            #loop over the set pieces events
                for j in range(len(event_data_window[event_data_window.open_play_cross==2]['Time_in_Seconds'])):
                    #current_team_set_piece = event_data_window[event_data_window.open_play_cross==2].team_id.iloc[j]
                    #current_event_set_piece = event_data_window[event_data_window.open_play_cross==2].unique_event_id.iloc[j]
                    current_time_set_piece = np.round(event_data_window[event_data_window.open_play_cross==2]['Time_in_Seconds'].iloc[j], 2)
                    event_data_window_set_piece = event_data_open_play_crosses[(event_data_open_play_crosses.period_id==periods[i]) & (event_data_open_play_crosses.team_id==teams[i]) & (event_data_open_play_crosses.Time_in_Seconds >= current_time_set_piece - 5.0) & (event_data_open_play_crosses.Time_in_Seconds < current_time_set_piece)]
                    #if in the previous 5 secs window we do not have a foul
                    if np.all(event_data_window_set_piece.open_play_cross!=4):
                        if event_data_window[event_data_window.open_play_cross==2]['x'].iloc[j] > 40.0:
                            event_data_open_play_crosses.loc[(event_data_open_play_crosses.unique_event_id==events[i]), 'open_play_cross'] = 5

                    #if we have any foul
                    else:
                        #if all of the fouls is in favour of the defending team
                        if np.all(event_data_window_set_piece[event_data_window_set_piece.open_play_cross==4].outcome==0):
                            if event_data_window[event_data_window.open_play_cross==2]['x'].iloc[j] > 40.0:
                                event_data_open_play_crosses.loc[(event_data_open_play_crosses.unique_event_id==events[i]), 'open_play_cross'] = 5

    #inner join original data with open play crosses data
    # data_output = data.merge(event_data_open_play_crosses[(event_data_open_play_crosses.type_id==1) & (event_data_open_play_crosses.unique_event_id.isin(event_data_open_play_crosses_big.unique_event_id.loc[event_data_open_play_crosses_big.qualifier_id==2].tolist()))][['unique_event_id', 'open_play_cross']].reset_index(drop = True), 
    #     how = 'inner', left_on = ['unique_event_id'], right_on = ['unique_event_id']).sort_values(['period_id', 'min', 'sec'])

    # data_output = data.merge(event_data_open_play_crosses[(event_data_open_play_crosses.type_id==1) & (~event_data_open_play_crosses.unique_event_id.isin(event_data_open_play_crosses_big.unique_event_id.loc[event_data_open_play_crosses_big.qualifier_id==2].tolist())) & (event_data_open_play_crosses.unique_event_id.isin(event_data_open_play_crosses_big.unique_event_id.loc[event_data_open_play_crosses_big.qualifier_id==155].tolist())) & (event_data_open_play_crosses.x >= 100/1.5) & ((event_data_open_play_crosses.y > 78.9) | (event_data_open_play_crosses.y < 21.1))][['unique_event_id', 'open_play_cross']].reset_index(drop = True), 
    #     how = 'inner', left_on = ['unique_event_id'], right_on = ['unique_event_id']).sort_values(['period_id', 'min', 'sec'])

    data_output = data.merge(event_data_open_play_crosses[(event_data_open_play_crosses.type_id.isin([1,2]))][['unique_event_id', 'open_play_cross']].reset_index(drop = True), 
        how = 'inner', left_on = ['unique_event_id'], right_on = ['unique_event_id']).sort_values(['period_id', 'min', 'sec'])

    data_output['pass_situation'] = np.where(data_output['open_play_cross']==1, 'open play', 
        np.where((data_output['open_play_cross']==2) | (data_output['open_play_cross']==5), 'free kick', 'corner'))

    data_output = data_output.drop(columns = ['open_play_cross'])

    if data_output.shape[0] == 0:
        data_output['qualifier_ids'] = None
        data_output['qualifier_values'] = None
        data_output = data_output.drop(columns = ['qualifier_id', 'value']) 
        data_output['x_end'] = None
        data_output['y_end'] = None

    else:
        #we need to pivot the qualifier ids and values such that each row is an event
        #data_output = data_output.groupby([x for x in data_output.columns if x not in ['qualifier_id', 'value']]).apply(merge_qualifiers).reset_index(drop = True) #weird behaviour by groupby apply function, we need to use a loop then
        data_output_grouped = data_output.groupby([x for x in data_output.columns if x not in ['qualifier_id', 'value']])
        list_output = []
        for index, df in data_output_grouped:
            list_output.append(merge_qualifiers(df.reset_index(drop = True)))
        data_output = pd.concat(list_output)
        data_output['x_end'] = [float(data_output['qualifier_values'].iloc[i].split(', ')[np.where(np.array([int(y) for y in data_output['qualifier_ids'].iloc[i].split(', ')])==140)[0][0]]) for i in range(data_output.shape[0])]
        data_output['y_end'] = [float(data_output['qualifier_values'].iloc[i].split(', ')[np.where(np.array([int(y) for y in data_output['qualifier_ids'].iloc[i].split(', ')])==141)[0][0]]) for i in range(data_output.shape[0])]
    
    data_output['freekick_taken'] = [int(5 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['corner_taken'] = [int(6 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['throw_in_taken'] = [int(107 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['blocked_pass'] = [int(236 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['chipped_pass'] = [int(155 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']] #this is the column that tells us whether a pass is high ot low

    #let's use some set of rules to classify by ourselves passes as crosses 
    data_output['OPTA Cross Qualifier'] = [int(2 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['Headed Pass'] = [int(3 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]

    data_output = cross_label(data_output)

    data_output['Attacking Team Name'] = np.where(data_output['team_id']==data_output['home_team_id'], data_output['home_team_name'], data_output['away_team_name'])
    data_output['Defending Team Name'] = np.where(data_output['team_id']==data_output['home_team_id'], data_output['away_team_name'], data_output['home_team_name'])

    #crosses can't be switch of play or through balls
    data_output['cross_to_remove'] = [(len(set([196,4]).intersection(set([int(y) for y in x.split(', ')]))) > 0) for x in data_output['qualifier_ids']]
    data_output['cross_to_remove'] = np.where((data_output['cross_to_remove']==1) & (data_output['corner_taken']==1), 0, data_output['cross_to_remove'])
    #final_df['cross_to_remove'] = np.where(final_df['x']==0,1,final_df['cross_to_remove'])
    data_output['cross_to_remove'] = np.where(data_output['OPTA Cross Qualifier']==1, 0, 
        np.where(data_output['cross_to_remove']==1, 1,
        np.where((data_output['x'] < 70) & (data_output['x_end'] < 83.0) & ((data_output['y_end'] < 36.8) | (data_output['y_end'] > 63.2)), 1, data_output['cross_to_remove'])))

    data_output['out_of_pitch'] = np.where((data_output['x_end'] >= 100.0) | (data_output['y_end'] <= 0) | (data_output['y_end'] >= 100), 1, 0)
    data_output['ending_too_wide'] = np.where(((data_output['y_end'] <= 21) | (data_output['y_end'] >= 79)) & (data_output['y_end'] < 100.0) & (data_output['y_end'] > 0.0) & (data_output['x_end'] < 100.0) & (np.sign(data_output['y'] - 50.0) != np.sign(data_output['y_end'] - 50.0)), 1, 0)

    return data_output[(data_output['Our Cross Qualifier']==1) & (data_output.cross_to_remove==0)].reset_index(drop = True)

#event_data_open_play_crosses_final = event_data_open_play_crosses[event_data_open_play_crosses.open_play_cross==1]





import os 
import time
import pandas as pd
#####run the loop here 
#parent_folder = 'K:\\TK Work\\2018-19\\DVMS\\Weekly Data'
#parent_folder = 'Y:\\Analysts\\TK\\2018-19\\Opta'
parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Premier League'
#parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Champions League'
#parent_folder = 'Y:\\API Data Files\\2018-19\\Premier League'
#folders_to_keep = [x for x in os.listdir(parent_folder) if x.split(' ')[0] == 'Round']
folders_to_keep = [x for x in os.listdir(parent_folder) if 'Round' in x]
#folders_to_keep = [folders_to_keep[-1]]
#latest_folder = max([parent_folder + '\\' + x for x in folders_to_keep], key = os.path.getmtime)
#folders_to_keep = [latest_folder.split('\\')[-1]]
#folders_to_keep = [folders_to_keep[-1]]
#latest_folder = max([parent_folder + '\\' + x for x in folders_to_keep], key = os.path.getmtime)
#folders_to_keep = [latest_folder.split('\\')[-1]]
#parent_folder = 'Y:\\API Data Files\\2019-20\\Super Cup'
#folders_to_keep = [parent_folder]

#list_of_dfs = []
cnt = 0
start_time = time.process_time() #to keep track of time taken
#loop over subfolders
for folder in folders_to_keep:
    #list_of_dfs = []
    path_folder = parent_folder + '\\' + folder
    subfolders = os.listdir(path_folder)
    #files_in_folder = os.listdir(path_folder)
    #path_folder = folder
    #files_in_folder = os.listdir(path_folder)

#     file_meta, file_event, file_results = [path_folder + '\\' + x for x in files_in_folder if x.endswith('xml')]
#     opta_output_final = opta_summary_output_passes(file_event, file_results, file_meta)
#     list_of_dfs.append(opta_output_final)
# final_df = pd.concat(list_of_dfs).sort_values(['Game ID', 'Home/Away', 'Start', 'Position ID'], ascending = [True, False, False, True])
# writer = pd.ExcelWriter('Y:\\Advanced Data Metrics\\2019-20\\Super Cup\\Opta Summaries\\Super Cup summary passes.xlsx', engine='xlsxwriter')
# #writer = pd.ExcelWriter('Y:\\Analysts\\FB\\whole season summary open play goals.xlsx', engine='xlsxwriter')
# #writer = pd.ExcelWriter('Y:\\Analysts\\FB\\whole season summary event ids.xlsx', engine='xlsxwriter')
# #writer = pd.ExcelWriter('Y:\\Analysts\\FB\\whole season summary event ids 2.xlsx', engine='xlsxwriter')
# final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
# worksheet = writer.sheets['Sheet1']  # pull worksheet object
# for idx, col in enumerate(final_df):  # loop through all columns
#     series = final_df[col]
#     max_len = max((
#         series.astype(str).map(len).max(),  # len of largest item
#         len(str(series.name))  # len of column name/header
#         )) + 1  # adding a little extra space
#     worksheet.set_column(idx, idx, max_len)  # set column width
# writer.save()

    #extract game ids from files
    #game_ids = sorted([int(x.split('.')[0]) for x in files_in_folder if x.endswith('.dat')])

    #loop over games
    for sub in subfolders:
    #for game in game_ids:

        if sum([sub in x for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses')])==0:
            list_of_dfs = []
        #files_game = [path_folder + '\\' + x for x in files_in_folder if str(game) in x]
        #keep files relevant to the game - here xml files only as we do not need the physical summaries and tracking data
            if len(os.listdir(path_folder + '\\' + sub)) > 0:
                file_event = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if 'f24' in x][0]
                file_results = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if 'srml' in x][0]
        
                opta_output_final = crosses_classification(file_event)

                #here we process them using a function we have set up that takes as inputs should return a dataframe with the columns of interest
                list_of_dfs.append(opta_output_final)

                cnt += 1
                print ('game id {} done, so {} games have been completed.'.format(int(sub.replace('g', '')), cnt))
                #print ('game id {} done, so {} games have been completed.'.format(game, cnt))
                print ('Cumulative time in seconds is {}.'.format(time.process_time() - start_time))

                final_df = pd.concat(list_of_dfs).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True])

                try:
                    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} {} crosses.xlsx'.format(opta_output_final['fixture'].iloc[0], sub), engine='xlsxwriter')
                except ValueError:
                    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} crosses.xlsx'.format(sub), engine='xlsxwriter')
        
                final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
                worksheet = writer.sheets['Sheet1']  # pull worksheet object
                for idx, col in enumerate(final_df):  # loop through all columns
                    series = final_df[col]
                    max_len = max((
                        series.astype(str).map(len).max(),  # len of largest item
                        len(str(series.name))  # len of column name/header
                        )) + 1  # adding a little extra space
                    worksheet.set_column(idx, idx, max_len)  # set column width
                writer.save()






#get all shot events
def get_shots(path_events):
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
    data['Time_in_Seconds'] = data['min']*60.0 + data['sec']

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
        data_output_grouped = data_shots.groupby([x for x in data_shots.columns if x not in ['qualifier_id', 'value']])
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
    data_shots['goal'] = np.where(data_shots['type_id'] == 16, 1, 0)
    data_shots['on_target'] = np.where(((data_shots['type_id']==15) | (data_shots['type_id']==16)) & ((data_shots['blocked']==0) | (data_shots['saved_off_line']==1)), 1, 0)
    data_shots['off_target'] = np.where((data_shots['type_id']==13) | (data_shots['type_id']==14), 1, 0)
    data_shots['chance_missed'] = np.where(data_shots['type_id'] == 60, 1, 0)

    data_shots['direct_shot'] = np.where(data_shots['value'] > 0, 1, 0)



    return data_shots


import os 
import time
import pandas as pd
#####run the loop here 
parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Premier League'
#parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Champions League'
#parent_folder = 'Y:\\API Data Files\\2018-19\\Premier League'
folders_to_keep = [x for x in os.listdir(parent_folder) if 'Round' in x]
#folders_to_keep = [folders_to_keep[-1]]
#latest_folder = max([parent_folder + '\\' + x for x in folders_to_keep], key = os.path.getmtime)
#folders_to_keep = [latest_folder.split('\\')[-1]]
#folders_to_keep = os.listdir(parent_folder)
#folders_to_keep = [folders_to_keep[-1]]
#latest_folder = max([parent_folder + '\\' + x for x in folders_to_keep], key = os.path.getmtime)
#folders_to_keep = [latest_folder.split('\\')[-1]]
#parent_folder = 'Y:\\API Data Files\\2019-20\\Super Cup'
#folders_to_keep = [parent_folder]

#list_of_dfs = []
cnt = 0
start_time = time.process_time() #to keep track of time taken
#loop over subfolders
for folder in folders_to_keep:
    #list_of_dfs = []
    path_folder = parent_folder + '\\' + folder
    subfolders = os.listdir(path_folder)
    #files_in_folder = os.listdir(path_folder)
    #path_folder = folder
    #files_in_folder = os.listdir(path_folder)

#     file_meta, file_event, file_results = [path_folder + '\\' + x for x in files_in_folder if x.endswith('xml')]
#     opta_output_final = opta_summary_output_passes(file_event, file_results, file_meta)
#     list_of_dfs.append(opta_output_final)
# final_df = pd.concat(list_of_dfs).sort_values(['Game ID', 'Home/Away', 'Start', 'Position ID'], ascending = [True, False, False, True])
# writer = pd.ExcelWriter('Y:\\Advanced Data Metrics\\2019-20\\Super Cup\\Opta Summaries\\Super Cup summary passes.xlsx', engine='xlsxwriter')
# #writer = pd.ExcelWriter('Y:\\Analysts\\FB\\whole season summary open play goals.xlsx', engine='xlsxwriter')
# #writer = pd.ExcelWriter('Y:\\Analysts\\FB\\whole season summary event ids.xlsx', engine='xlsxwriter')
# #writer = pd.ExcelWriter('Y:\\Analysts\\FB\\whole season summary event ids 2.xlsx', engine='xlsxwriter')
# final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
# worksheet = writer.sheets['Sheet1']  # pull worksheet object
# for idx, col in enumerate(final_df):  # loop through all columns
#     series = final_df[col]
#     max_len = max((
#         series.astype(str).map(len).max(),  # len of largest item
#         len(str(series.name))  # len of column name/header
#         )) + 1  # adding a little extra space
#     worksheet.set_column(idx, idx, max_len)  # set column width
# writer.save()

    #extract game ids from files
    #game_ids = sorted([int(x.split('.')[0]) for x in files_in_folder if x.endswith('.dat')])

    #loop over games
    for sub in subfolders:
    #for game in game_ids:

        if sum([sub in x for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots')])==0:
            list_of_dfs = []
        #files_game = [path_folder + '\\' + x for x in files_in_folder if str(game) in x]
        #keep files relevant to the game - here xml files only as we do not need the physical summaries and tracking data
            if len(os.listdir(path_folder + '\\' + sub)) > 0:
                file_event = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if 'f24' in x][0]
                file_results = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if 'srml' in x][0]
        
                opta_output_final = get_shots(file_event)

                #here we process them using a function we have set up that takes as inputs should return a dataframe with the columns of interest
                list_of_dfs.append(opta_output_final)

                cnt += 1
                print ('game id {} done, so {} games have been completed.'.format(int(sub.replace('g', '')), cnt))
                #print ('game id {} done, so {} games have been completed.'.format(game, cnt))
                print ('Cumulative time in seconds is {}.'.format(time.process_time() - start_time))

                final_df = pd.concat(list_of_dfs).sort_values(['game_id', 'period_id', 'min', 'sec'], ascending = [True, True, True, True])

                try:
                    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots\\{} {} shots.xlsx'.format(opta_output_final['fixture'].iloc[0], sub), engine='xlsxwriter')
                except ValueError:
                    writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots\\{} shots.xlsx'.format(sub), engine='xlsxwriter')
        
                final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
                worksheet = writer.sheets['Sheet1']  # pull worksheet object
                for idx, col in enumerate(final_df):  # loop through all columns
                    series = final_df[col]
                    max_len = max((
                        series.astype(str).map(len).max(),  # len of largest item
                        len(str(series.name))  # len of column name/header
                        )) + 1  # adding a little extra space
                    worksheet.set_column(idx, idx, max_len)  # set column width
                writer.save()





#########tracking data loop

import os 
import pandas as pd
#import time 
import numpy as np 
import sys
from datetime import datetime, date, timedelta

os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")
#my_dir = os.getcwd()
#from opta_files_manipulation_functions import opta_event_file_manipulation

#os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies
parent_folder_crosses = '\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces\\crosses'
parent_folder_shots = '\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces\\shots'

#get crosses_classified to take out the real open play crosses
crosses_classified = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces\\crosses classified.xlsx')
all_crosses_file = pd.concat([pd.read_excel(os.path.join(parent_folder_crosses, x)) for x in os.listdir(parent_folder_crosses)]).reset_index(drop=True)
all_shots_file = pd.concat([pd.read_excel(os.path.join(parent_folder_shots, x)) for x in os.listdir(parent_folder_shots)]).reset_index(drop=True)
all_shots_file['Footed'] = list(map(lambda x: int((20 in [int(y) for y in x.split(', ')]) | (72 in [int(y) for y in x.split(', ')])), 
    all_shots_file['qualifier_ids']))

#merge all_crosses_file with crosses_classified to obtain the updated crosses classification we want
all_crosses_file = all_crosses_file.merge(crosses_classified[['OPTA Event ID', 'Cross Type', 'Side', 'Early/Lateral/Deep']], how = 'inner', 
    left_on = ['unique_event_id'], right_on = ['OPTA Event ID']).drop(['OPTA Event ID', 
    'pass_situation'], axis = 1).sort_values(['game_id', 'period_id', 'min', 'sec']).reset_index(drop=True)

# all_crosses_file = pd.concat([pd.read_excel(os.path.join(parent_folder_crosses, os.listdir(parent_folder_crosses)[i])) for i in range(len(os.listdir(parent_folder_crosses)))]).reset_index(drop = True)
# all_crosses_file['cutback'] = np.where((all_crosses_file['cutback']==1) & (all_crosses_file['chipped_pass']==1) & (all_crosses_file['x'] - all_crosses_file['x_end'] < 5.0) & (np.array([int(195 in [int(y) for y in x.split(', ')]) for x in all_crosses_file['qualifier_ids']])==0), 
#     0, all_crosses_file['cutback'])

# for game_id in all_crosses_file.game_id.unique():
#     try:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} {} crosses.xlsx'.format(all_crosses_file[all_crosses_file.game_id==game_id]['fixture'].iloc[0], 'g' + str(int(game_id))), engine='xlsxwriter')
#     except ValueError:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} crosses.xlsx'.format('g' + str(int(game_id))), engine='xlsxwriter')
#     final_df = all_crosses_file[all_crosses_file.game_id==game_id].reset_index(drop=True)    
#     final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
#     worksheet = writer.sheets['Sheet1']  # pull worksheet object
#     for idx, col in enumerate(final_df):  # loop through all columns
#         series = final_df[col]
#         max_len = max((
#             series.astype(str).map(len).max(),  # len of largest item
#             len(str(series.name))  # len of column name/header
#             )) + 1  # adding a little extra space
#         worksheet.set_column(idx, idx, max_len)  # set column width
#     writer.save()


# #import and update only most recent crosses and shots
# modification_times = [os.path.getmtime(y) for y in [os.path.join(parent_folder_crosses, x) for x in os.listdir(parent_folder_crosses)]]
# modification_dates = [[int(y) for y in date.fromtimestamp(x).strftime('%Y-%m-%d').split('-')] for x in modification_times]
# days_ago = [(date.today() - date(x[0], x[1], x[2])).days for x in modification_dates]

# modification_times_shots = [os.path.getmtime(y) for y in [os.path.join(parent_folder_shots, x) for x in os.listdir(parent_folder_shots)]]
# modification_dates_shots = [[int(y) for y in date.fromtimestamp(x).strftime('%Y-%m-%d').split('-')] for x in modification_times_shots]
# days_ago_shots = [(date.today() - date(x[0], x[1], x[2])).days for x in modification_dates_shots]

# all_crosses_file = pd.concat([pd.read_excel(os.path.join(parent_folder_crosses, os.listdir(parent_folder_crosses)[i])) for i in range(len(os.listdir(parent_folder_crosses))) if days_ago[i]==0]).reset_index(drop = True)
# #all_crosses_file['out_of_pitch'] = np.where((all_crosses_file['x_end'] >= 100.0) | (all_crosses_file['y_end'] <= 0) | (all_crosses_file['y_end'] >= 100), 1, 0)
# #all_crosses_file['ending_too_wide'] = np.where((all_crosses_file['y_end'] <= 21) | (all_crosses_file['y_end'] >= 79), 1, 0)
# all_shots_file = pd.concat([pd.read_excel(os.path.join(parent_folder_shots, os.listdir(parent_folder_shots)[i])) for i in range(len(os.listdir(parent_folder_shots))) if days_ago_shots[i]==0]).reset_index(drop = True)
#all_shots_file['direct_shot'] = np.where(all_shots_file['value'] > 0, 1, 0)
 #if days_ago_shots[i]==0

#before merging, we need to loop over shots and see whether they are within 5 secs after a cross
for i in range(all_shots_file.shape[0]):
    if (all_shots_file.value.iloc[i] == 0):
        if np.any((all_shots_file.Time_in_Seconds.iloc[i] > all_crosses_file[(all_crosses_file.game_id == all_shots_file.game_id.iloc[i]) & (all_crosses_file.period_id == all_shots_file.period_id.iloc[i]) & ((all_crosses_file.team_id == all_shots_file.related_event_team_id.iloc[i]))].Time_in_Seconds) & (all_shots_file.Time_in_Seconds.iloc[i] <= all_crosses_file[(all_crosses_file.game_id == all_shots_file.game_id.iloc[i]) & (all_crosses_file.period_id == all_shots_file.period_id.iloc[i]) & ((all_crosses_file.team_id == all_shots_file.related_event_team_id.iloc[i]))].Time_in_Seconds + 5.0)):
            
            all_shots_file.loc[i,'value'] = all_crosses_file[(all_crosses_file.game_id == all_shots_file.game_id.iloc[i]) & (all_crosses_file.period_id == all_shots_file.period_id.iloc[i]) & ((all_crosses_file.team_id == all_shots_file.related_event_team_id.iloc[i])) & (all_crosses_file.Time_in_Seconds < all_shots_file.Time_in_Seconds.iloc[i]) & (all_crosses_file.Time_in_Seconds >= all_shots_file.Time_in_Seconds.iloc[i] - 5.0)].event_id.iloc[-1] #we take the most recent cross before the shot if applicable
    print ('row {} processed'.format(i))

# #recreate shots files updated
# for game_id in all_shots_file.game_id.unique():
#     try:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots\\{} {} shots.xlsx'.format(all_shots_file[all_shots_file.game_id==game_id]['fixture'].iloc[0], 'g' + str(int(game_id))), engine='xlsxwriter')
#     except ValueError:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots\\{} shots.xlsx'.format('g' + str(int(game_id))), engine='xlsxwriter')
#         #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\All Passes Events Classified Champions League.xlsx', engine='xlsxwriter')
#     final_df = all_shots_file[all_shots_file.game_id==game_id].reset_index(drop=True)
#     final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
#     worksheet = writer.sheets['Sheet1']  # pull worksheet object
#     for idx, col in enumerate(final_df):  # loop through all columns
#         series = final_df[col]
#         max_len = max((
#             series.astype(str).map(len).max(),  # len of largest item
#             len(str(series.name))  # len of column name/header
#             )) + 1  # adding a little extra space
#         worksheet.set_column(idx, idx, max_len)  # set column width
#     writer.save()
#     print ('{} exported.'.format(final_df.fixture.iloc[0]))


#we join crosses and shots
columns_merge_crosses = ['competition_id', 'competition_name', 'season_id', 'season_name',
       'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
       'home_score', 'home_team_id', 'home_team_name', 'away_score',
       'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'team_id', 'event_id']

columns_merge_shots = ['competition_id', 'competition_name', 'season_id', 'season_name',
       'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
       'home_score', 'home_team_id', 'home_team_name', 'away_score',
       'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'related_event_team_id', 'value']

crosses_and_shots = all_crosses_file.merge(all_shots_file, how = 'left', 
    left_on = columns_merge_crosses, right_on = columns_merge_shots, suffixes = ('', '_shot')) #there can be potentially multiple shots from the same cross. For instance, we have here 1 corner leeaing to two shots

#we retain only open play and 2nd phase crosses
crosses_and_shots = crosses_and_shots[(crosses_and_shots['Cross Type'] == 'Open Play') | (np.array(list(map(lambda x: '2nd Phase' in x, 
    crosses_and_shots['Cross Type']))))].reset_index(drop=True)

#crosses_and_shots['pass_situation'] = np.where((crosses_and_shots['from_regular_play']==1) | (crosses_and_shots['from_fast_break']==1), 
#    'open play', np.where((crosses_and_shots['from_set_piece']==1) | (crosses_and_shots['from_direct_freekick']==1), 'free kick', 
#            np.where(crosses_and_shots['from_corner']==1, 'corner', crosses_and_shots['pass_situation']))) #we use this for further refinement knowing which situation a shot is related to. Clearly, when there is no shot we need to rely on our rule

#we want to add an indicator stating whether there is at least a shot in a specific cross. This can be slightly different from simply having number of shots over number of crosses as there can be multiple shots from a single cross.
#e.g. 2 shots out of 2 crosses can be either 2 shots occurring from one cross only (so 1 out of 2 crosses would yield a shooting situation) or 1 shot per cross occurring (leading to 2 out of 2 crosses giving a shooting situation)
#crosses_and_shots['at_least_a_shot'] = crosses_and_shots[['unique_event_id']].merge(crosses_and_shots.groupby(['unique_event_id'])['unique_event_id_shot'].nunique().reset_index(), how = 'inner')['unique_event_id_shot']

# crosses_and_shots_to_export = crosses_and_shots.drop_duplicates(['unique_event_id']).reset_index(drop=True)

# for game_id in crosses_and_shots_to_export.game_id.unique():
#     try:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} {} crosses.xlsx'.format(crosses_and_shots_to_export[crosses_and_shots_to_export.game_id==game_id]['fixture'].iloc[0], 'g' + str(int(game_id))), engine='xlsxwriter')
#     except ValueError:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} crosses.xlsx'.format('g' + str(int(game_id))), engine='xlsxwriter')
#         #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\All Passes Events Classified Champions League.xlsx', engine='xlsxwriter')
#     final_df = crosses_and_shots_to_export.loc[crosses_and_shots_to_export.game_id==game_id, :'ending_too_wide'].reset_index(drop=True)
#     final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
#     worksheet = writer.sheets['Sheet1']  # pull worksheet object
#     for idx, col in enumerate(final_df):  # loop through all columns
#         series = final_df[col]
#         max_len = max((
#             series.astype(str).map(len).max(),  # len of largest item
#             len(str(series.name))  # len of column name/header
#             )) + 1  # adding a little extra space
#         worksheet.set_column(idx, idx, max_len)  # set column width
#     writer.save()
#     print ('{} exported.'.format(final_df.fixture.iloc[0]))

#####up to here we have created new crosses and shots files and updated them as well. 








####section about game/team level summaries about crosses and shots from them

#reimport back
#all_crosses_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses')]).reset_index(drop = True)
# all_crosses_file['ending_too_wide'] = np.where(((all_crosses_file['y_end'] <= 21) | (all_crosses_file['y_end'] >= 79)) & (all_crosses_file['y_end'] < 100.0) & (all_crosses_file['y_end'] > 0.0) & (all_crosses_file['x_end'] < 100.0) & (np.sign(all_crosses_file['y'] - 50.0) != np.sign(all_crosses_file['y_end'] - 50.0)), 1, 0)

# data_output = all_crosses_file
# data_output['cutback'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
# data_output['Our Cross Qualifier'] = np.where(data_output['cutback']==1, 1, data_output['Our Cross Qualifier'])
# data_output['cutback'] = np.where(data_output['x'] < 83.0, 0, 
#     np.where((data_output['y'] >= 95.0) | (data_output['y'] <= 5.0), 0, 
#         np.where(data_output['cutback']==1, 1, 
#             np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] >= 88.5) & (data_output['x'] <= 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
#                 1, 
#                 np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] > 94.2) & (data_output['x_end'] < 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
#                     1, 0)))))
# data_output['cutback'] = np.where((data_output['cutback']==1) & (data_output['chipped_pass']==1) & (data_output['x'] - data_output['x_end'] < 5.0) & (np.array([int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']])==0), 
#     0, data_output['cutback'])

# for game_id in all_crosses_file.game_id.unique():
#     try:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} {} crosses.xlsx'.format(all_crosses_file[all_crosses_file.game_id==game_id]['fixture'].iloc[0], 'g' + str(int(game_id))), engine='xlsxwriter')
#     except ValueError:
#         writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses\\{} crosses.xlsx'.format('g' + str(int(game_id))), engine='xlsxwriter')
#         #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\All Passes Events Classified Champions League.xlsx', engine='xlsxwriter')
#     final_df = all_crosses_file.loc[all_crosses_file.game_id==game_id, :'ending_too_wide'].reset_index(drop=True)
#     final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
#     worksheet = writer.sheets['Sheet1']  # pull worksheet object
#     for idx, col in enumerate(final_df):  # loop through all columns
#         series = final_df[col]
#         max_len = max((
#             series.astype(str).map(len).max(),  # len of largest item
#             len(str(series.name))  # len of column name/header
#             )) + 1  # adding a little extra space
#         worksheet.set_column(idx, idx, max_len)  # set column width
#     writer.save()
#     print ('{} exported.'.format(final_df.fixture.iloc[0]))


# all_crosses_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses')]).reset_index(drop = True)
# all_shots_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots')]).reset_index(drop = True)
# #we join crosses and shots
# columns_merge_crosses = ['competition_id', 'competition_name', 'season_id', 'season_name',
#        'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
#        'home_score', 'home_team_id', 'home_team_name', 'away_score',
#        'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'team_id', 'event_id']

# columns_merge_shots = ['competition_id', 'competition_name', 'season_id', 'season_name',
#        'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
#        'home_score', 'home_team_id', 'home_team_name', 'away_score',
#        'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'related_event_team_id', 'value']

# crosses_and_shots = all_crosses_file.merge(all_shots_file, how = 'left', 
#     left_on = columns_merge_crosses, right_on = columns_merge_shots, suffixes = ('', '_shot'))

#crosses_and_shots['at_least_a_shot'] = crosses_and_shots[['unique_event_id']].merge(crosses_and_shots.groupby(['unique_event_id'])['unique_event_id_shot'].nunique().reset_index(), how = 'inner')['unique_event_id_shot']
crosses_and_shots['game_id'] = list(map(lambda x: 'f' + str(int(x)), crosses_and_shots['game_id']))
crosses_and_shots['player_id'] = list(map(lambda x: 'p' + str(int(x)), crosses_and_shots['player_id']))
crosses_and_shots['player_id_shot'] = ['p' + str(int(x)) if ~np.isnan(x) else np.nan for x in crosses_and_shots['player_id_shot']]

dummy_games = crosses_and_shots[['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name']].drop_duplicates()
dummy_games = pd.concat([dummy_games]*2).sort_values('game_id')
dummy_games['Attacking Team Name'] = [dummy_games['home_team_name'].iloc[i] if i % 2 == 0 else dummy_games['away_team_name'].iloc[i] for i in range(dummy_games.shape[0])]
dummy_games['Defending Team Name'] = [dummy_games['away_team_name'].iloc[i] if i % 2 == 0 else dummy_games['home_team_name'].iloc[i] for i in range(dummy_games.shape[0])]
dummy_games['merger'] = 1
pass_situation = pd.DataFrame({'merger': [1]*len(crosses_and_shots['Cross Type'].unique()), 'Cross Type': list(crosses_and_shots['Cross Type'].unique())})
cross_location = pd.DataFrame({'merger': [1]*len(crosses_and_shots['Early/Lateral/Deep'].unique()), 'Early/Lateral/Deep': list(crosses_and_shots['Early/Lateral/Deep'].unique())})
cross_side = pd.DataFrame({'merger': [1]*len(crosses_and_shots['Side'].unique()), 'Side': list(crosses_and_shots['Side'].unique())})
dummy_games = dummy_games.merge(pass_situation, 
    how = 'inner').merge(cross_side, 
    how = 'inner').merge(cross_location, how = 'inner').drop(['merger'], axis = 1)

#for x in [z for y in [crosses_and_shots.loc[:,'unique_event_id':'ending_too_wide'].columns.tolist(), ['at_least_a_shot']] for z in y]:
#    if crosses_and_shots[x].dtype != 'object':
#        crosses_and_shots[x] = np.where(crosses_and_shots[x] >= 1, crosses_and_shots['unique_event_id'], np.nan)


opta_core_stats = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Opta Summaries Player Level\\Premier League 2019-20 core stats.xlsx')


game_level_summaries = dummy_games.merge(crosses_and_shots.replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'unique_event_id': 'nunique', 
    'unique_event_id_shot': 'count', 'on_target': 'sum', 'goal': 'sum', 'headed': 'sum', 'Footed': 'sum', 
    'direct_shot': 'sum'}).reset_index(), how = 'left').fillna(0)

#we now need to add some more aggregations based on multiple conditions, not nice to do within a groupby operation

####direct shots
game_level_summaries['Direct Shots On Target'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==1].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'on_target': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Goals From Direct Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==1].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'goal': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]

game_level_summaries['Headed Direct Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==1].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'headed': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Headed Direct Shots On Target'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==1) & (crosses_and_shots.headed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'on_target': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Goals From Headed Direct Shots'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==1) & (crosses_and_shots.headed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'goal': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]

game_level_summaries['Footed Direct Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==1].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'Footed': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Footed Direct Shots On Target'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==1) & (crosses_and_shots.Footed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'on_target': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Goals From Footed Direct Shots'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==1) & (crosses_and_shots.Footed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'goal': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]


###delayed shots
game_level_summaries['Delayed Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==0].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'unique_event_id_shot': 'count'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Delayed Shots On Target'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==0].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'on_target': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Goals From Delayed Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==0].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'goal': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]

game_level_summaries['Headed Delayed Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==0].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'headed': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Headed Delayed Shots On Target'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==0) & (crosses_and_shots.headed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'on_target': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Goals From Headed Delayed Shots'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==0) & (crosses_and_shots.headed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'goal': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]

game_level_summaries['Footed Delayed Shots'] = dummy_games.merge(crosses_and_shots[crosses_and_shots.direct_shot==0].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'Footed': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Footed Delayed Shots On Target'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==0) & (crosses_and_shots.Footed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'on_target': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]
game_level_summaries['Goals From Footed Delayed Shots'] = dummy_games.merge(crosses_and_shots[(crosses_and_shots.direct_shot==0) & (crosses_and_shots.Footed==1)].replace(0, np.nan).groupby(['game_id', 'fixture', 
    'home_team_name', 'away_team_name', 'Attacking Team Name', 
    'Defending Team Name', 'Cross Type', 'Side', 'Early/Lateral/Deep']).agg({'goal': 'sum'}).reset_index(), how = 'left').fillna(0).iloc[:,-1]

game_level_summaries = game_level_summaries.rename(columns = 
    {'unique_event_id': 'Crosses', 'unique_event_id_shot': 'Shots', 'on_target': 'Shots On Target', 'goal': 'Goals',
    'headed': 'Headed Shots', 'Footed': 'Footed Shots', 
    'direct_shot': 'Direct Shots'})

# #####team level crosses and cutback crosses with shooting info 
# game_level_summaries = dummy_games.merge(crosses_and_shots.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_name', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'Cross Type']).agg({'unique_event_id': 'nunique',
#  'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
#  'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
#  'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
#  'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
#  'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
#  'saved_off_line': 'sum', 
#  'deflected': 'sum', 
#  'goal': 'sum', 'on_target': 'sum', 
#  'off_target': 'sum', 'blocked': 'sum', 
#  'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left').fillna(0)


    
# for x in game_level_summaries.columns:
#     if ('float' in str(game_level_summaries[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries[x] = game_level_summaries[x].astype('int64') 

# #game_level_summaries[(game_level_summaries['Defending Team Name']=='Chelsea') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]
# #game_level_summaries[(game_level_summaries['Attacking Team Name']=='Manchester City') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]


# #do the same but just for cutbacks
# game_level_summaries_cutbacks = dummy_games.merge(crosses_and_shots[crosses_and_shots.cutback > 0].reset_index(drop=True).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_name', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'pass_situation']).agg({'unique_event_id': 'nunique',
#  'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
#  'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
#  'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
#  'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
#  'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
#  'saved_off_line': 'sum', 
#  'deflected': 'sum', 
#  'goal': 'sum', 'on_target': 'sum', 
#  'off_target': 'sum', 'blocked': 'sum', 
#  'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left').fillna(0)
    
# for x in game_level_summaries_cutbacks.columns:
#     if ('float' in str(game_level_summaries_cutbacks[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_cutbacks[x] = game_level_summaries_cutbacks[x].astype('int64') 

#game_level_summaries_cutbacks[(game_level_summaries_cutbacks['Defending Team Name']=='Chelsea') & (game_level_summaries_cutbacks.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]
#game_level_summaries_cutbacks[(game_level_summaries_cutbacks['Attacking Team Name']=='Manchester City') & (game_level_summaries_cutbacks.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]


######player level crosses and cutbacks with shooting info
game_level_summaries_crosser = dummy_games.merge(opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID', 'Player Name', 'Time Played', 'Time In Possession', 'Time Out Of Possession']], 
    how = 'inner', left_on = ['game_id', 'Attacking Team Name', 'Defending Team Name'], right_on = ['Game ID', 'Team Name', 'Opposition Team Name']).merge(crosses_and_shots.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id', 'pass_situation']).agg({'unique_event_id': 'nunique',
 'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
 'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
 'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
 'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
 'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
 'saved_off_line': 'sum', 
 'deflected': 'sum', 
 'goal': 'sum', 'on_target': 'sum', 
 'off_target': 'sum', 'blocked': 'sum', 
 'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left', left_on = [x for y in [dummy_games.columns.tolist(), ['Player ID']] for x in y], 
 right_on = [x for y in [dummy_games.columns.tolist(), ['player_id']] for x in y]).fillna(0).drop(['home_team_id', 'away_team_id', 'player_id'], axis = 1)

#opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Player ID', 'Player Name']]
    
# for x in game_level_summaries_crosser.columns:
#     if ('float' in str(game_level_summaries_crosser[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_crosser[x] = game_level_summaries_crosser[x].astype('int64') 

#game_level_summaries[(game_level_summaries['Defending Team Name']=='Chelsea') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]
#game_level_summaries[(game_level_summaries['Attacking Team Name']=='Manchester City') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]


#do the same but just for cutbacks
# game_level_summaries_crosser_cutbacks = dummy_games.merge(opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID', 'Player Name', 'Time Played', 'Time In Possession', 'Time Out Of Possession']], 
#     how = 'inner', left_on = ['game_id', 'Attacking Team Name', 'Defending Team Name'], right_on = ['Game ID', 'Team Name', 'Opposition Team Name']).merge(crosses_and_shots[crosses_and_shots.cutback > 0].reset_index(drop=True).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id', 'pass_situation']).agg({'unique_event_id': 'nunique',
#  'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
#  'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
#  'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
#  'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
#  'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
#  'saved_off_line': 'sum', 
#  'deflected': 'sum', 
#  'goal': 'sum', 'on_target': 'sum', 
#  'off_target': 'sum', 'blocked': 'sum', 
#  'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left', left_on = [x for y in [dummy_games.columns.tolist(), ['Player ID']] for x in y], 
#  right_on = [x for y in [dummy_games.columns.tolist(), ['player_id']] for x in y]).fillna(0).drop(['home_team_id', 'away_team_id', 'player_id'], axis = 1)
    
# for x in game_level_summaries_crosser_cutbacks.columns:
#     if ('float' in str(game_level_summaries_crosser_cutbacks[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_crosser_cutbacks[x] = game_level_summaries_crosser_cutbacks[x].astype('int64') 



game_level_summaries_shooter = dummy_games.merge(opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID', 'Player Name', 'Time Played', 'Time In Possession', 'Time Out Of Possession']], 
    how = 'inner', left_on = ['game_id', 'Attacking Team Name', 'Defending Team Name'], right_on = ['Game ID', 'Team Name', 'Opposition Team Name']).merge(crosses_and_shots.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id_shot', 'pass_situation']).agg({'unique_event_id': 'nunique',
 'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
 'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
 'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
 'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
 'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
 'saved_off_line': 'sum', 
 'deflected': 'sum', 
 'goal': 'sum', 'on_target': 'sum', 
 'off_target': 'sum', 'blocked': 'sum', 
 'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left', left_on = [x for y in [dummy_games.columns.tolist(), ['Player ID']] for x in y], 
 right_on = [x for y in [dummy_games.columns.tolist(), ['player_id_shot']] for x in y]).fillna(0).drop(['home_team_id', 'away_team_id', 'player_id_shot'], axis = 1)

#opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Player ID', 'Player Name']]
    
# for x in game_level_summaries_shooter.columns:
#     if ('float' in str(game_level_summaries_shooter[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_shooter[x] = game_level_summaries_shooter[x].astype('int64') 

#game_level_summaries[(game_level_summaries['Defending Team Name']=='Chelsea') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]
#game_level_summaries[(game_level_summaries['Attacking Team Name']=='Manchester City') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]


#do the same but just for cutbacks
# game_level_summaries_shooter_cutbacks = dummy_games.merge(opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID', 'Player Name', 'Time Played', 'Time In Possession', 'Time Out Of Possession']], 
#     how = 'inner', left_on = ['game_id', 'Attacking Team Name', 'Defending Team Name'], right_on = ['Game ID', 'Team Name', 'Opposition Team Name']).merge(crosses_and_shots[crosses_and_shots.cutback>0].reset_index(drop=True).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id_shot', 'pass_situation']).agg({'unique_event_id': 'nunique',
#  'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
#  'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
#  'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
#  'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
#  'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
#  'saved_off_line': 'sum', 
#  'deflected': 'sum', 
#  'goal': 'sum', 'on_target': 'sum', 
#  'off_target': 'sum', 'blocked': 'sum', 
#  'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left', left_on = [x for y in [dummy_games.columns.tolist(), ['Player ID']] for x in y], 
#  right_on = [x for y in [dummy_games.columns.tolist(), ['player_id_shot']] for x in y]).fillna(0).drop(['home_team_id', 'away_team_id', 'player_id_shot'], axis = 1)
    
# for x in game_level_summaries_shooter_cutbacks.columns:
#     if ('float' in str(game_level_summaries_shooter_cutbacks[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_shooter_cutbacks[x] = game_level_summaries_shooter_cutbacks[x].astype('int64') 



from itertools import permutations

f_ids = lambda x : pd.DataFrame(list(permutations(x.values,2)), 
                            columns=['Player ID Crosser','Player ID Shooter'])
#f_names = lambda x : pd.DataFrame(list(permutations(x.values,2)), 
#                            columns=['Player Name Crosser','Player Name Shooter'])
opta_core_stats_interacting_ids = opta_core_stats.groupby(['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away'])['Player ID'].apply(f_ids).reset_index().drop(['level_4'], axis = 1)
#opta_core_stats_interacting_names = opta_core_stats.groupby(['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away'])['Player Name'].apply(f_names).reset_index().drop(['level_4'], axis = 1)

opta_core_stats_interacting_cross = opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 
'Player ID', 'Player Name', 'Time Played', 'Time In Possession', 'Time Out Of Possession']].merge(opta_core_stats_interacting_ids, 
    how = 'inner', left_on = ['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID'], 
    right_on = ['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID Crosser'])

opta_core_stats_interacting_shoot = opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 
'Player ID', 'Player Name', 'Time Played', 'Time In Possession', 'Time Out Of Possession']].merge(opta_core_stats_interacting_ids, 
    how = 'inner', left_on = ['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID'], 
    right_on = ['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID Shooter'])

opta_core_stats_interacting = opta_core_stats_interacting_cross.drop(['Player ID'], axis = 1).merge(opta_core_stats_interacting_shoot.drop(['Player ID'], axis = 1), 
    how = 'inner', left_on = ['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID Crosser', 'Player ID Shooter'], 
    right_on = ['Game ID', 'Team Name', 'Opposition Team Name', 'Home/Away', 'Player ID Crosser', 'Player ID Shooter'], suffixes = (' Crosser', ' Shooter')).sort_values(['Game ID', 'Team Name'])


game_level_summaries_crosser_shooter = dummy_games.merge(opta_core_stats_interacting, 
    how = 'inner', left_on = ['game_id', 'Attacking Team Name', 'Defending Team Name'], right_on = ['Game ID', 'Team Name', 'Opposition Team Name']).merge(crosses_and_shots.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id', 'player_id_shot', 'pass_situation']).agg({'unique_event_id': 'nunique',
 'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
 'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
 'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
 'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
 'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
 'saved_off_line': 'sum', 
 'deflected': 'sum', 
 'goal': 'sum', 'on_target': 'sum', 
 'off_target': 'sum', 'blocked': 'sum', 
 'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left', left_on = [x for y in [dummy_games.columns.tolist(), ['Player ID Crosser', 'Player ID Shooter']] for x in y], 
 right_on = [x for y in [dummy_games.columns.tolist(), ['player_id', 'player_id_shot']] for x in y]).fillna(0).drop(['home_team_id', 'away_team_id', 'player_id', 'player_id_shot'], axis = 1)

#opta_core_stats[['Game ID', 'Team Name', 'Opposition Team Name', 'Player ID', 'Player Name']]
    
# for x in game_level_summaries_crosser_shooter.columns:
#     if ('float' in str(game_level_summaries_crosser_shooter[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_crosser_shooter[x] = game_level_summaries_crosser_shooter[x].astype('int64') 

# #game_level_summaries[(game_level_summaries['Defending Team Name']=='Chelsea') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]
# #game_level_summaries[(game_level_summaries['Attacking Team Name']=='Manchester City') & (game_level_summaries.pass_situation=='open play')][['fixture', 'unique_event_id', 'outcome', 'blocked_pass', 'out_of_pitch', 'ending_too_wide', 'cutback', 'unique_event_id_shot', 'at_least_a_shot', 'big_chance', 'goal', 'on_target', 'off_target', 'blocked', 'chance_missed', 'direct_shot']]


# #do the same but just for cutbacks
# game_level_summaries_crosser_shooter_cutbacks = dummy_games.merge(opta_core_stats_interacting, 
#     how = 'inner', left_on = ['game_id', 'Attacking Team Name', 'Defending Team Name'], right_on = ['Game ID', 'Team Name', 'Opposition Team Name']).merge(crosses_and_shots[crosses_and_shots.cutback>0].reset_index(drop=True).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id', 'player_id_shot', 'pass_situation']).agg({'unique_event_id': 'nunique',
#  'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
#  'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
#  'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
#  'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
#  'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
#  'saved_off_line': 'sum', 
#  'deflected': 'sum', 
#  'goal': 'sum', 'on_target': 'sum', 
#  'off_target': 'sum', 'blocked': 'sum', 
#  'chance_missed': 'sum', 'direct_shot': 'sum'}).reset_index(), how = 'left', left_on = [x for y in [dummy_games.columns.tolist(), ['Player ID Crosser', 'Player ID Shooter']] for x in y], 
#  right_on = [x for y in [dummy_games.columns.tolist(), ['player_id', 'player_id_shot']] for x in y]).fillna(0).drop(['home_team_id', 'away_team_id', 'player_id', 'player_id_shot'], axis = 1)
    
# for x in game_level_summaries_crosser_shooter_cutbacks.columns:
#     if ('float' in str(game_level_summaries_crosser_shooter_cutbacks[x].dtype)) & ('time' not in x.lower()):
#         game_level_summaries_crosser_shooter_cutbacks[x] = game_level_summaries_crosser_shooter_cutbacks[x].astype('int64') 






def dfs_tabs(df_list, sheet_list, file_name):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.to_excel(writer, sheet_name=sheet, index = False) 
        worksheet = writer.sheets[sheet]  # pull worksheet object
        for idx, col in enumerate(dataframe):  # loop through all columns
            series = dataframe[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width  
    writer.save()

dfs_tabs([game_level_summaries, game_level_summaries_cutbacks], ['all_crosses', 'cutbacks'], '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses and shots summaries\\team level crosses summaries.xlsx')
dfs_tabs([game_level_summaries_crosser, game_level_summaries_crosser_cutbacks], ['all_crosses', 'cutbacks'], '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses and shots summaries\\crosser level crosses summaries.xlsx')
dfs_tabs([game_level_summaries_shooter, game_level_summaries_shooter_cutbacks], ['all_crosses', 'cutbacks'], '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses and shots summaries\\shooter level crosses summaries.xlsx')
for game_id in game_level_summaries_crosser_shooter['game_id'].unique():
    if game_id not in str(os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses and shots summaries\\crosser shooter level')):
        dfs_tabs([game_level_summaries_crosser_shooter[game_level_summaries_crosser_shooter.game_id==game_id].reset_index(drop=True), 
            game_level_summaries_crosser_shooter_cutbacks[game_level_summaries_crosser_shooter_cutbacks.game_id==game_id].reset_index(drop=True)], 
            ['all_crosses', 'cutbacks'], '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses and shots summaries\\crosser shooter level\\{} {} crosser shooter level crosses summaries.xlsx'.format(game_level_summaries_crosser_shooter[game_level_summaries_crosser_shooter.game_id==game_id].fixture.iloc[0], game_id))
        print ('{} completed.'.format(game_level_summaries_crosser_shooter[game_level_summaries_crosser_shooter.game_id==game_id].fixture.iloc[0]))



#here we have the loop through open play crosses in tracking data to give the context not provided by event data alone
# import os 

# #os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies
# os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")

# from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation

import os 
import time
import pandas as pd
import sys
import numpy as np 


os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")
my_dir = os.getcwd()
#from tracking_data_manipulation_function import tracking_data_manipulation, tracking_data_manipulation_new, time_possession
#from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
#from opta_core_stats_output_function import opta_core_stats_output

#event_type = 'Set Piece'
#event_type = 'Cross'

seasons = ['2019-20']
#seasons = ['2016-17', '2017-18', '2018-19']
#competitions = ['Champions League', 'FA Cup', 'League Cup', 'Super Cup']
#competitions = ['Premier League']
competitions = ['FA Cup']
event_types = ['Set Piece', 'Cross']
#season = '2019-20'
#competition = 'Champions League'

for season in seasons:
    #competitions = [x for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}'.format(season)) if ('cup' in x.lower()) | ('league' in x.lower())]
    for competition in competitions:
        if 'tracking data set pieces' not in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses'.format(season, competition)):
            os.mkdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data set pieces'.format(season, competition))
        if 'tracking data crosses' not in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses'.format(season, competition)):
            os.mkdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data crosses'.format(season, competition))
        for event_type in event_types:
            if event_type == 'Set Piece':
                #tracking_data_events = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data set pieces'.format(season,competition), x)) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data set pieces'.format(season,competition)) if '~' not in x])
                all_crosses_file = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Set Pieces with 2nd Phase Output.xlsx'.format(season,competition))
                game_ids = [x.replace('f','g') for x in all_crosses_file.game_id.unique()]
            if event_type =='Cross':
                #tracking_data_events = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data crosses'.format(season,competition), x)) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data crosses'.format(season,competition)) if '~' not in x])
                all_crosses_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\crosses v4'.format(season,competition), x)) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\crosses v4'.format(season,competition)) if '~' not in x])
                game_ids = ['g' + str(int(x)) for x in all_crosses_file.game_id.unique()]


            parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}\\{}'.format(season, competition)
            subfolders_to_keep = [x for x in os.listdir(parent_folder) if ('spectrum' not in x) & ('f73' not in x)]

            #we need to check whether we have already reached the game level folders or we are still at a higher level
            if sum([x.startswith('g') for x in subfolders_to_keep]) > 0: #if true, we already hit the game level folders
                game_folder_collections = [competition]
            else:
                game_folder_collections = subfolders_to_keep

            #loop over subfolders
            for folder in game_folder_collections:
                if folder == competition: #if true, it means we need to climb a level less 
                    path_folder = parent_folder
                else:
                    path_folder = os.path.join(parent_folder, folder)
                subfolders = os.listdir(path_folder)

                for sub in subfolders:

                    if (event_type=='Set Piece') & (sub in game_ids) & (len(os.listdir(path_folder + '\\' + sub)) > 0) & (sub not in str(os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data set pieces'.format(season,competition)))):

                        os.system('{} {} {} {} {} {} {}'.format(sys.executable, '"' + os.path.join(my_dir, 'fully comprehensive script for crosses handling v2.py') + '"', '"' + path_folder + '"', sub, '"' + event_type + '"', '"' + season + '"', '"' + competition + '"'))

                    if (event_type=='Cross') & (sub in game_ids) & (len(os.listdir(path_folder + '\\' + sub)) > 0) & (sub not in str(os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\tracking data crosses'.format(season,competition)))):

                        os.system('{} {} {} {} {} {} {}'.format(sys.executable, '"' + os.path.join(my_dir, 'fully comprehensive script for crosses handling v2.py') + '"', '"' + path_folder + '"', sub, '"' + event_type + '"', '"' + season + '"', '"' + competition + '"'))

#& (sub not in str(os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\Chelsea crosses')))

all_crosses_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\crosses v4', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\crosses v4') if '~' not in x])
all_crosses_file_tracking = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\tracking data crosses', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\tracking data crosses') if '~' not in x])

#we need to quickly check how the missing crosses are distributed across the games
number_crosses = all_crosses_file.groupby(['fixture', 'game_id']).agg({'unique_event_id': 'count'}).reset_index().rename(columns = {'game_id': 'Game ID', 
    'unique_event_id': 'OPTA Event ID'})
number_crosses['Game ID'] = list(map(lambda x: 'f' + str(int(x)), number_crosses['Game ID'])) 
number_processed_crosses = all_crosses_file_tracking.groupby(['Game ID']).agg({'OPTA Event ID': 'count'}).reset_index()
number_crosses_merged = number_crosses.merge(number_processed_crosses, how = 'inner', left_on = ['Game ID'], 
    right_on = ['Game ID'], suffixes=(' Effective', ' Processed')).sort_values(['Game ID']).reset_index(drop=True)
number_crosses_merged['Unprocessed Crosses'] = number_crosses_merged['OPTA Event ID Effective'] - number_crosses_merged['OPTA Event ID Processed'] 
number_crosses_merged.sort_values(['Unprocessed Crosses'], ascending = [False])
(number_crosses_merged['Unprocessed Crosses'] > 0).value_counts()


all_crosses_file = pd.read_excel('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\Set Pieces with 2nd Phase Output.xlsx')
all_crosses_file['unique_event_id'] = np.where(all_crosses_file['Relevant OPTA Event ID'].isnull(), 
    all_crosses_file['OPTA Event ID'], all_crosses_file['Relevant OPTA Event ID'])
all_crosses_file_tracking = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\tracking data set pieces', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\2019-20\\Premier League\\Set Pieces & Crosses\\tracking data set pieces') if '~' not in x])

#we need to quickly check how the missing crosses are distributed across the games

number_crosses = all_crosses_file.groupby(['Fixture', 'game_id']).agg({'unique_event_id': 'count'}).reset_index().rename(columns = {'game_id': 'Game ID', 
    'unique_event_id': 'OPTA Event ID'})
#number_crosses['Game ID'] = list(map(lambda x: 'f' + str(int(x)), number_crosses['Game ID'])) 
number_processed_crosses = all_crosses_file_tracking.groupby(['Game ID']).agg({'OPTA Event ID': 'count'}).reset_index()
number_crosses_merged = number_crosses.merge(number_processed_crosses, how = 'inner', left_on = ['Game ID'], 
    right_on = ['Game ID'], suffixes=(' Effective', ' Processed')).sort_values(['Game ID']).reset_index(drop=True)
number_crosses_merged['Unprocessed Crosses'] = number_crosses_merged['OPTA Event ID Effective'] - number_crosses_merged['OPTA Event ID Processed'] 
number_crosses_merged.sort_values(['Unprocessed Crosses'], ascending = [False])
(number_crosses_merged['Unprocessed Crosses'] > 0).value_counts()

####import back crosses from tracking data and plain opta crosses and apply some additional filtering, before getting information around 
# import os 
# import pandas as pd
# #import time 
# import numpy as np 

# #os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")
# #from opta_files_manipulation_functions import opta_event_file_manipulation

# #these are open play crosses for and against Chelsea. 
# chelsea_crosses_tracking = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\Chelsea crosses', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\Chelsea crosses')]).reset_index(drop = True)


# #reimport back 
# all_crosses_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses')]).reset_index(drop = True)
# all_shots_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots')]).reset_index(drop = True)
# #we join crosses and shots
# columns_merge_crosses = ['competition_id', 'competition_name', 'season_id', 'season_name',
#        'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
#        'home_score', 'home_team_id', 'home_team_name', 'away_score',
#        'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'team_id', 'event_id']

# columns_merge_shots = ['competition_id', 'competition_name', 'season_id', 'season_name',
#        'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
#        'home_score', 'home_team_id', 'home_team_name', 'away_score',
#        'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'related_event_team_id', 'value']

# crosses_and_shots = all_crosses_file.merge(all_shots_file, how = 'left', 
#     left_on = columns_merge_crosses, right_on = columns_merge_shots, suffixes = ('', '_shot'))

# crosses_and_shots['at_least_a_shot'] = crosses_and_shots[['unique_event_id']].merge(crosses_and_shots.groupby(['unique_event_id'])['unique_event_id_shot'].nunique().reset_index(), how = 'inner')['unique_event_id_shot']


# #merge Chelsea tracking information with OPTA info on shots from crosses
# merged_chelsea = crosses_and_shots.merge(chelsea_crosses_tracking, how = 'inner', 
#     left_on = ['unique_event_id'], right_on = ['OPTA Event ID'])
# #merged_chelsea['ratio_attack_vs_defense'] = merged_chelsea['Number Of Attacking Players In Box']/merged_chelsea['Number Of Defending Players In Box']


# #try to see an example - e.g. potential correlation between number of people in the box and occurrence of any shot
# merged_chelsea_unique = merged_chelsea.drop_duplicates(['unique_event_id']).reset_index(drop=True)#.fillna(0)
# merged_chelsea_unique['at_least_a_shot'] = np.where(merged_chelsea_unique['at_least_a_shot'] > 0, 1, 0)
# #merged_chelsea_unique[['Number Of Attacking Players In Box', 'Number Of Defending Players In Box', 'ratio_attack_vs_defense', 'at_least_a_shot', 'goal']].corr()

# #we might want to get presence in the attacking box relative to the number of crosses made when each player is on the pitch (the so called 'exposure'). For the moment, we focus only on Chelsea attacking crosses
# merged_chelsea_unique_attacking = merged_chelsea_unique[merged_chelsea_unique['Attacking Team Name']=='Chelsea'].reset_index(drop=True)

# os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")

# from tracking_data_manipulation_function import tracking_data_manipulation, tracking_data_manipulation_new, time_possession
# from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
# from opta_core_stats_output_function import opta_core_stats_output

#from matplotlib import pyplot as plt 

#plt.plot()




import os 
import time
import pandas as pd
import numpy as np 

def dfs_tabs(df_list, sheet_list, file_name):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.to_excel(writer, sheet_name=sheet, index = False) 
        worksheet = writer.sheets[sheet]  # pull worksheet object
        for idx, col in enumerate(dataframe):  # loop through all columns
            series = dataframe[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width  
    writer.save()

#os.chdir("C:\\Users\\fbettuzzi\\Desktop\\Chelsea\\automation scripts")
os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts")
from tracking_data_manipulation_function import tracking_data_manipulation_new, time_possession
from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
from opta_core_stats_output_function import opta_core_stats_output

all_crosses_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA crosses') if 'Chelsea' in x]).reset_index(drop = True)
all_shots_file = pd.concat([pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots', x)) for x in os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots') if 'Chelsea' in x]).reset_index(drop = True)
game_ids = ['g' + str(int(x)) for x in all_crosses_file.game_id.unique()]

#####run the loop here 
parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Premier League'
#parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\2019-20\\Champions League'
#parent_folder = 'Y:\\API Data Files\\2018-19\\Premier League'
folders_to_keep = [x for x in os.listdir(parent_folder) if 'Round' in x]
#folders_to_keep = [folders_to_keep[-1]]
#latest_folder = max([parent_folder + '\\' + x for x in folders_to_keep], key = os.path.getmtime)
#folders_to_keep = [latest_folder.split('\\')[-1]]
#folders_to_keep = os.listdir(parent_folder)
#folders_to_keep = [folders_to_keep[-1]]
#latest_folder = max([parent_folder + '\\' + x for x in folders_to_keep], key = os.path.getmtime)
#folders_to_keep = [latest_folder.split('\\')[-1]]
#parent_folder = 'Y:\\API Data Files\\2019-20\\Super Cup'
#folders_to_keep = [parent_folder]

#list_of_dfs = []
cnt = 0
start_time = time.process_time() #to keep track of time taken
#loop over subfolders
#folder = 'Round 19 All Data'
for folder in folders_to_keep:
    #list_of_dfs = []
    path_folder = parent_folder + '\\' + folder
    subfolders = [x for x in os.listdir(path_folder) if x in game_ids]

    #loop over games
    #sub = 'g1059884'
    for sub in subfolders:
        if sub not in str(os.listdir('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\final crosses elaborations')):
    #for game in game_ids:
        #list_of_dfs = []
        #files_game = [path_folder + '\\' + x for x in files_in_folder if str(game) in x]
        #keep files relevant to the game - here xml files only as we do not need the physical summaries and tracking data
            file_events = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if 'f24' in x][0]
            file_results = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if 'srml' in x][0]
            file_tracking_data = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if x.endswith('.jsonl')][0]
            file_tracking_meta = [path_folder + '\\' + sub + '\\' + x for x in os.listdir(path_folder + '\\' + sub) if x.endswith('.json')][0]
            file_tracking_crosses = pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\Chelsea crosses', '{} {}.xlsx'.format(all_crosses_file[all_crosses_file.game_id==int(sub.replace('g',''))].fixture.iloc[0], sub)))
        #referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(file_results)
            try:
                track_players_df, player_names_df = tracking_data_manipulation_new(file_tracking_data, file_tracking_meta)
            except OSError:
                continue

            time_possession_df = time_possession(track_players_df)
            #time_possession_df['game_id'] = time_possession_df['game_id'].astype(int)
            time_possession_df['Player_ID'] = time_possession_df['Player_ID'].astype(int)
            time_possession_df['Player_ID'] = ['p' + str(x) for x in time_possession_df['Player_ID']]
            opta_output = opta_core_stats_output(file_events, file_results)
            #join the two dfs and replace columns
            opta_output_final =opta_output.merge(time_possession_df, how = 'inner', left_on = ['Player ID'], 
                right_on = ['Player_ID']).drop(['Player_ID'], axis = 1)
            opta_output_final['Time Played'] = opta_output_final['time played']
            opta_output_final['Time In Possession'] = opta_output_final['time in possession for the team']
            opta_output_final['Time Out Of Possession'] = opta_output_final['time out possession for the team']
            opta_output_final.drop(['time played', 'time in possession for the team', 'time out possession for the team'], axis = 1,
                inplace = True)
        #opta_output_final_chelsea = opta_output_final[opta_output_final['Team Name']=='Chelsea'].reset_index(drop=True)

        #merged_chelsea_unique_game = merged_chelsea_unique[merged_chelsea_unique.game_id==int(sub.replace('g', ''))].reset_index(drop=True)

            track_players_df['Player_ID'] = ['p' + str(int(x)) for x in track_players_df['Player_ID']]
            track_players_df['game_id'] = track_players_df['game_id'].astype(int)
            track_players_df['Team Name'] = np.where(track_players_df.Is_Home_Away=='home', opta_output_final[(opta_output_final['Home/Away']=='Home')]['Team Name'].iloc[0], opta_output_final[(opta_output_final['Home/Away']=='Away')]['Team Name'].iloc[0])
            track_players_df['Time'] = np.where(track_players_df['Period_ID']==2, track_players_df['Time'] + 45*60.0, track_players_df['Time'])
        
            #number of crosses to which each player has been exposed attackingwise
            all_crosses_file['Time_in_Seconds'] = all_crosses_file['Time_in_Seconds'].astype(float)

            columns_merge_crosses = ['competition_id', 'competition_name', 'season_id', 'season_name',
               'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
               'home_score', 'home_team_id', 'home_team_name', 'away_score',
               'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'team_id', 'event_id']

            columns_merge_shots = ['competition_id', 'competition_name', 'season_id', 'season_name',
               'game_id', 'match_day', 'game_date', 'period_1_start', 'period_2_start',
               'home_score', 'home_team_id', 'home_team_name', 'away_score',
               'away_team_id', 'away_team_name', 'fixture', 'result', 'period_id', 'related_event_team_id', 'value']


            merged_opta_track = all_crosses_file.merge(file_tracking_crosses, how = 'inner', left_on = ['unique_event_id'], right_on = ['OPTA Event ID']).merge(all_shots_file,
                how = 'left', left_on = columns_merge_crosses, right_on = columns_merge_shots, suffixes = ('', '_shot'))

            merged_opta_track['at_least_a_shot'] = merged_opta_track[['unique_event_id']].merge(merged_opta_track.groupby(['unique_event_id'])['unique_event_id_shot'].nunique().reset_index(), how = 'inner')['unique_event_id_shot']

            for x in ['unique_event_id', 'keypass', 'assist', 'freekick_taken', 'corner_taken', 'throw_in_taken', 'blocked_pass', 'chipped_pass', 'cutback', 'out_of_pitch', 'ending_too_wide', 'at_least_a_shot', 'Number Of Opponents Within 3 Meters From The Crosser']:
                if merged_opta_track[x].dtype != 'object':
                    merged_opta_track[x] = np.where(merged_opta_track[x] >= 1, merged_opta_track['unique_event_id'], np.nan)


            #number of times each player has been in the box attackingwise
            merged_opta_track_attack_box = merged_opta_track.assign(player_id_in_box = merged_opta_track['Attacking Player IDs In Box'].str.split('; '))
            merged_opta_track_attack_box = merged_opta_track_attack_box.explode('player_id_in_box')

            merged_opta_track_attack_box['player_id'] = ['p' + str(int(x)) for x in merged_opta_track_attack_box['player_id']]
            merged_opta_track_attack_box['player_id_shot'] = ['p' + str(int(x)) if ~np.isnan(x) else np.nan for x in merged_opta_track_attack_box['player_id_shot']]


            #here we get number of crosses to which each player has been exposed as well as number of times each player has been in the attacking box
            crosses_exposure = opta_output_final.merge(pd.DataFrame(merged_opta_track.merge(track_players_df, how = 'inner', left_on = ['game_id', 'period_id', 'Time_in_Seconds', 'Attacking Team Name'], 
                right_on = ['game_id', 'Period_ID', 'Time', 'Team Name']).groupby(['Attacking Team Name', 
                'Player_ID']).game_id.count()).reset_index().rename(columns = {'game_id': 'number_crosses_made_by_the_team'}), how = 'left', 
            left_on = ['Player ID'], right_on = ['Player_ID']).drop(['Attacking Team Name', 'Player_ID'], axis = 1).fillna(0)


            crosses_presence = opta_output_final.merge(merged_opta_track_attack_box.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id_in_box']).agg({'unique_event_id': 'nunique',
             'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
             'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
             'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
             'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
             'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
             'saved_off_line': 'sum', 
             'deflected': 'sum', 
             'goal': 'sum', 'on_target': 'sum', 
             'off_target': 'sum', 'blocked': 'sum', 
             'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index().rename(columns = {'unique_event_id': 'number_crosses_in_box'}), 
             how = 'left', 
            left_on = ['Player ID'], right_on = ['player_id_in_box']).fillna(0)
            #crosses_presence['number_times_in_box'] = crosses_presence['number_times_in_box'].astype(int) 
            crosses_presence = crosses_presence.drop(['Attacking Team Name', 'Defending Team Name', 'player_id_in_box', 'game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name'], 
                axis = 1)

        
            #we need to account for games where there are no shots from crosses
            if merged_opta_track_attack_box[merged_opta_track_attack_box.player_id_shot.notnull()].shape[0] == 0:

                crosses_shots = opta_output_final.merge(merged_opta_track_attack_box.drop_duplicates(['unique_event_id', 'unique_event_id_shot', 'player_id_shot']).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id_shot']).agg({'unique_event_id': 'nunique',
                 'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
                 'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
                 'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
                 'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
                 'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
                 'saved_off_line': 'sum', 
                 'deflected': 'sum', 
                 'goal': 'sum', 'on_target': 'sum', 
                 'off_target': 'sum', 'blocked': 'sum', 
                 'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index().rename(columns = {'unique_event_id': 'number_crosses_concluded'}), 
                 how = 'left', 
                 left_on = ['Player ID'], right_on = ['player_id_shot']).fillna(0)

            else:
                crosses_shots = opta_output_final.merge(merged_opta_track_attack_box[merged_opta_track_attack_box.player_id_shot.notnull()].drop_duplicates(['unique_event_id', 'unique_event_id_shot', 'player_id_shot']).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'player_id_shot']).agg({'unique_event_id': 'nunique',
                'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
                'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
                'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
                'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
                'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
                'saved_off_line': 'sum', 
                'deflected': 'sum', 
                'goal': 'sum', 'on_target': 'sum', 
                'off_target': 'sum', 'blocked': 'sum', 
                'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index().rename(columns = {'unique_event_id': 'number_crosses_concluded'}), 
                how = 'left', 
                left_on = ['Player ID'], right_on = ['player_id_shot']).fillna(0)
        #crosses_shots['number_times_in_box'] = crosses_shots['number_times_in_box'].astype(int) 
        
            crosses_shots = crosses_shots.drop(['Attacking Team Name', 'Defending Team Name', 'player_id_shot', 'game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name'], 
                axis = 1)

            crosses_exposure_and_presence_and_shots = crosses_exposure.merge(crosses_presence, how = 'left').merge(crosses_shots, how = 'inner', 
                left_on = crosses_exposure.loc[:,:'Time Out Of Possession'].columns.tolist(), right_on = crosses_exposure.loc[:,:'Time Out Of Possession'].columns.tolist(), 
                suffixes = ('_occurring_while_in_box', '_actively_participating_in'))

            for x in crosses_exposure_and_presence_and_shots.columns:
                if ('float' in str(crosses_exposure_and_presence_and_shots[x].dtype)) & ('time' not in x.lower()):
                    crosses_exposure_and_presence_and_shots[x] = crosses_exposure_and_presence_and_shots[x].astype('int64') 
        #final_df['number_times_in_box'] = final_df['number_times_in_box'].astype(int) 
        #final_df[['Player Name', 'number_crosses_made_by_the_team', 'number_times_in_box']]




        #here we focus on getting info around the crossing player - basically the same we did at team level for the excel sheet around general crosses and cutbacks. But this can include additional columns from the tracking data info
            crosses_made_and_shots = opta_output_final.merge(merged_opta_track.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'Player Crosser ID', 'Player Crosser Name']).agg({'unique_event_id': 'nunique',
             'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
             'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
             'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
             'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
             'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
             'saved_off_line': 'sum', 
             'deflected': 'sum', 
             'goal': 'sum', 'on_target': 'sum', 
             'off_target': 'sum', 'blocked': 'sum', 
             'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index(), how = 'left', left_on = ['Player ID', 'Player Name'], right_on = ['Player Crosser ID', 'Player Crosser Name']).fillna(0)
    
            for x in crosses_made_and_shots.columns:
                if ('float' in str(crosses_made_and_shots[x].dtype)) & ('time' not in x.lower()):
                    crosses_made_and_shots[x] = crosses_made_and_shots[x].astype('int64') 


            crosses_made_and_shots = crosses_made_and_shots.drop(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'Player Crosser ID', 'Player Crosser Name'], axis = 1)
            crosses_made_and_shots = crosses_made_and_shots.rename(columns = {'unique_event_id': 'number_crosses_made'})



            #here we get the finest level of detail - crosser/player in box level. It is a final combination of player crossing vs players in box/shooting interaction
            n_players = opta_output_final.groupby(['Team ID', 'Team Name'])['Player ID'].count().to_frame().reset_index().rename(columns = {'Player ID': 'n_players'})
            dummy_interactions_team_1 = pd.concat([opta_output_final[opta_output_final['Team ID']==n_players['Team ID'].iloc[0]]]*n_players.n_players.iloc[0])
            dummy_interactions_team_2 = pd.concat([opta_output_final[opta_output_final['Team ID']==n_players['Team ID'].iloc[1]]]*n_players.n_players.iloc[1])
            dummy_interactions_team_1['Player ID crossing'] = np.repeat(dummy_interactions_team_1['Player ID'].unique(), repeats = n_players.n_players.iloc[0])
            dummy_interactions_team_1['Player Name crossing'] = np.repeat(dummy_interactions_team_1['Player Name'].unique(), repeats = n_players.n_players.iloc[0])
            dummy_interactions_team_2['Player ID crossing'] = np.repeat(dummy_interactions_team_2['Player ID'].unique(), repeats = n_players.n_players.iloc[1])
            dummy_interactions_team_2['Player Name crossing'] = np.repeat(dummy_interactions_team_2['Player Name'].unique(), repeats = n_players.n_players.iloc[1])
            #dummy_interactions_team_1 = dummy_interactions_team_1.reset_index(drop=True)
            #dummy_interactions_team_2 = dummy_interactions_team_2.reset_index(drop=True)
            for x in dummy_interactions_team_1.loc[:,'Team Formation ID':'Time Out Of Possession'].columns:
                dummy_interactions_team_1['X'] = np.repeat(dummy_interactions_team_1.drop_duplicates(['Player ID'])[x], repeats = n_players.n_players.iloc[0]).tolist()
                dummy_interactions_team_2['X'] = np.repeat(dummy_interactions_team_2.drop_duplicates(['Player ID'])[x], repeats = n_players.n_players.iloc[1]).tolist()
                dummy_interactions_team_1 = dummy_interactions_team_1.rename(columns = {'X': '{} crossing'.format(x)})
                dummy_interactions_team_2 = dummy_interactions_team_2.rename(columns = {'X': '{} crossing'.format(x)})

            dummy_interactions = pd.concat([dummy_interactions_team_1, dummy_interactions_team_2]).reset_index(drop=True)


            crosses_exposure_both = opta_output_final.merge(pd.DataFrame(merged_opta_track.merge(track_players_df, how = 'inner', left_on = ['game_id', 'period_id', 'Time_in_Seconds', 'Attacking Team Name'], 
                right_on = ['game_id', 'Period_ID', 'Time', 'Team Name']).groupby(['Attacking Team Name', 
                'Player Crosser ID', 'Player Crosser Name', 'Player_ID']).game_id.count()).reset_index().rename(columns = {'game_id': 'number_crosses_made_by_the_player'}), how = 'left', 
            left_on = ['Player ID'], right_on = ['Player_ID']).drop(['Attacking Team Name', 'Player_ID'], axis = 1).merge(dummy_interactions, 
            how = 'right', left_on = [x for y in [opta_output_final.columns.tolist(), ['Player Crosser ID', 'Player Crosser Name']] for x in y], 
            right_on = [x for y in [opta_output_final.columns.tolist(), ['Player ID crossing', 'Player Name crossing']] for x in y], suffixes = (' exposed', ' crossing')).drop(['Player Crosser ID', 'Player Crosser Name'], axis = 1).fillna(0)
            crosses_exposure_both = crosses_exposure_both[crosses_exposure_both['Player Name crossing'] != crosses_exposure_both['Player Name']].reset_index(drop=True)


            crosses_presence_both = opta_output_final.merge(merged_opta_track_attack_box.replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'Player Crosser ID', 'Player Crosser Name', 'player_id_in_box']).agg({'unique_event_id': 'nunique',
             'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
             'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
             'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
             'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
             'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
             'saved_off_line': 'sum', 
             'deflected': 'sum', 
             'goal': 'sum', 'on_target': 'sum', 
             'off_target': 'sum', 'blocked': 'sum', 
             'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index().rename(columns = {'unique_event_id': 'number_crosses_in_box'}), 
             how = 'left', 
             left_on = ['Player ID'], right_on = ['player_id_in_box']).fillna(0).merge(dummy_interactions, 
             how = 'right', left_on = [x for y in [opta_output_final.columns.tolist(), ['Player Crosser ID', 'Player Crosser Name']] for x in y], 
            right_on = [x for y in [opta_output_final.columns.tolist(), ['Player ID crossing', 'Player Name crossing']] for x in y], suffixes = (' exposed', ' crossing')).drop(['Player Crosser ID', 'Player Crosser Name'], axis = 1).fillna(0)
            #crosses_presence['number_times_in_box'] = crosses_presence['number_times_in_box'].astype(int) 
            crosses_presence_both = crosses_presence_both.drop(['Attacking Team Name', 'Defending Team Name', 'player_id_in_box', 'game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name'], 
                axis = 1)
            crosses_presence_both = crosses_presence_both[(crosses_presence_both['Player Name crossing'] != crosses_presence_both['Player Name'])].reset_index(drop=True)


            if merged_opta_track_attack_box[merged_opta_track_attack_box.player_id_shot.notnull()].shape[0] == 0:

                merged_opta_track_attack_box.loc[0,'player_id_shot'] = merged_opta_track_attack_box[(merged_opta_track_attack_box['Player Crosser ID'] != merged_opta_track_attack_box['Player Crosser ID'].iloc[0]) & (merged_opta_track_attack_box['Team Name'] == merged_opta_track_attack_box['Team Name'].iloc[0])]['Player Crosser ID'].unique()[0] 

                crosses_shots_both = opta_output_final.merge(merged_opta_track_attack_box.drop_duplicates(['unique_event_id', 'unique_event_id_shot', 'player_id_shot']).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'Player Crosser ID', 'Player Crosser Name', 'player_id_shot']).agg({'unique_event_id': 'nunique',
                 'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
                 'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
                 'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
                 'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
                 'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
                 'saved_off_line': 'sum', 
                 'deflected': 'sum', 
                 'goal': 'sum', 'on_target': 'sum', 
                 'off_target': 'sum', 'blocked': 'sum', 
                 'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index().rename(columns = {'unique_event_id': 'number_crosses_concluded'}), 
                 how = 'left', 
                left_on = ['Player ID'], right_on = ['player_id_shot']).fillna(0).merge(dummy_interactions, 
                how = 'right', left_on = [x for y in [opta_output_final.columns.tolist(), ['Player Crosser ID', 'Player Crosser Name']] for x in y], 
                right_on = [x for y in [opta_output_final.columns.tolist(), ['Player ID crossing', 'Player Name crossing']] for x in y], suffixes = (' exposed', ' crossing')).drop(['Player Crosser ID', 'Player Crosser Name'], axis = 1).fillna(0)
        
            else:

                crosses_shots_both = opta_output_final.merge(merged_opta_track_attack_box[merged_opta_track_attack_box.player_id_shot.notnull()].drop_duplicates(['unique_event_id', 'unique_event_id_shot', 'player_id_shot']).replace(0, np.nan).groupby(['game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name', 'Attacking Team Name', 'Defending Team Name', 'Player Crosser ID', 'Player Crosser Name', 'player_id_shot']).agg({'unique_event_id': 'nunique',
                 'outcome': 'nunique', 'keypass': 'nunique', 'assist': 'nunique', 'freekick_taken': 'nunique', 'corner_taken': 'nunique', 
                 'blocked_pass': 'nunique', 'out_of_pitch': 'nunique', 'ending_too_wide': 'nunique', 'chipped_pass': 'nunique',  'cutback': 'nunique', 
                 'unique_event_id_shot': 'count', 'at_least_a_shot': 'nunique', 'outcome_shot': 'sum', 
                 'own_goal': 'sum', 'headed': 'sum', 'other_body_part': 'sum', 
                 'big_chance': 'sum', 'not_reaching_goal_line': 'sum', 
                 'saved_off_line': 'sum', 
                 'deflected': 'sum', 
                 'goal': 'sum', 'on_target': 'sum', 
                 'off_target': 'sum', 'blocked': 'sum', 
                 'chance_missed': 'sum', 'direct_shot': 'sum', 'Number Of Opponents Within 3 Meters From The Crosser': 'nunique'}).reset_index().rename(columns = {'unique_event_id': 'number_crosses_concluded'}), 
                 how = 'left', 
                left_on = ['Player ID'], right_on = ['player_id_shot']).fillna(0).merge(dummy_interactions, 
                how = 'right', left_on = [x for y in [opta_output_final.columns.tolist(), ['Player Crosser ID', 'Player Crosser Name']] for x in y], 
                right_on = [x for y in [opta_output_final.columns.tolist(), ['Player ID crossing', 'Player Name crossing']] for x in y], suffixes = (' exposed', ' crossing')).drop(['Player Crosser ID', 'Player Crosser Name'], axis = 1).fillna(0)
        
            #crosses_shots['number_times_in_box'] = crosses_shots['number_times_in_box'].astype(int) 
            crosses_shots_both = crosses_shots_both.drop(['Attacking Team Name', 'Defending Team Name', 'player_id_shot', 'game_id', 'fixture', 'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name'], 
                axis = 1)
            crosses_shots_both = crosses_shots_both[(crosses_shots_both['Player Name crossing'] != crosses_shots_both['Player Name'])].reset_index(drop=True)


            columns_to_merge_on = [x for y in [crosses_exposure_both.loc[:,:'Time Out Of Possession'].columns.tolist(), crosses_exposure_both.loc[:,'Player ID crossing':'Time Out Of Possession crossing'].columns.tolist()] for x in y]
            crosses_exposure_and_presence_and_shots_both = crosses_exposure_both.merge(crosses_presence_both, how = 'inner').merge(crosses_shots_both, how = 'inner', 
                left_on = columns_to_merge_on, right_on = columns_to_merge_on, 
                suffixes = ('_occurring_while_in_box', '_actively_participating_in'))

            for x in crosses_exposure_and_presence_and_shots_both.columns:
                if ('float' in str(crosses_exposure_and_presence_and_shots_both[x].dtype)) & ('time' not in x.lower()):
                    crosses_exposure_and_presence_and_shots_both[x] = crosses_exposure_and_presence_and_shots_both[x].astype('int64') 
            #final_df['number_times_in_box'] = final_df['number_times_in_box'].astype(int) 
            #final_df[['Player Name', 'number_crosses_made_by_the_team', 'number_times_in_box']]

            dfs_tabs([crosses_exposure_and_presence_and_shots, 
                crosses_made_and_shots, 
                crosses_exposure_and_presence_and_shots_both], 
                ['players_in_box', 'crossers', 'crossers_and_players_in_box'], 
                '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\final crosses elaborations\\{} {} player level data.xlsx'.format(all_crosses_file[all_crosses_file.game_id==int(sub.replace('g',''))].fixture.iloc[0], sub))

            print ('game id {} completed.'.format(sub))
        # try:
        #     writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots\\{} {} shots.xlsx'.format(opta_output_final['fixture'].iloc[0], sub), engine='xlsxwriter')
        # except ValueError:
        #     writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\OPTA shots\\{} shots.xlsx'.format(sub), engine='xlsxwriter')
        # #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\All Passes Events Classified Champions League.xlsx', engine='xlsxwriter')
        # final_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        # worksheet = writer.sheets['Sheet1']  # pull worksheet object
        # for idx, col in enumerate(final_df):  # loop through all columns
        #     series = final_df[col]
        #     max_len = max((
        #         series.astype(str).map(len).max(),  # len of largest item
        #         len(str(series.name))  # len of column name/header
        #         )) + 1  # adding a little extra space
        #     worksheet.set_column(idx, idx, max_len)  # set column width
        # writer.save()