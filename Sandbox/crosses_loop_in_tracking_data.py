import os
import pandas as pd 
import numpy as np
from scripts_from_fed.opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation

def only_open_play_crosses(data):
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

def cross_label (data_output):
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, #if there is cross qualifier then 1
        np.where(data_output['x'] < 75.0, 0, #if the pas starts from behind the halfway line then 0
            np.where((data_output['y'] > 100/3.0) & (data_output['y'] < 100/1.5), 0, #if the pass starts too frontal then 0
                np.where(data_output['x_end'] < 79.0, 0, #if the pass ends too behind then 0
                    np.where((np.abs(data_output['y'] - 50.0) < np.abs(data_output['y_end'] - 50.0)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, #if the pass is directed wide then 0
                        np.where((np.abs(data_output['y'] - data_output['y_end']) < 25.0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)), 0, 1))))))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0, data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x'] < 75.0) | ((data_output['y'] < 0.5*21.1) | (data_output['y'] > 0.5*(100+78.9)))), 0, data_output['Our Cross Qualifier'])) #low passes should start not too behind or not too wide
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.75*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] < 83.0) & (data_output['x'] >= 75.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier'])) #we set an upper bound on the distance travelled on the x axis proportional to the distance travelled on the y axis
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.5*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] >= 83.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier']))

    
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x_end'] < 83.0) | ((data_output['y_end'] > 78.9) | (data_output['y_end'] < 21.1))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (data_output['x'] < 83.0) & (data_output['y'] >= 21.1) & (data_output['y'] <= 78.9), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)) & (((data_output['y_end'] < 100/3.0) & (data_output['y'] < 100/3.0)) | ((data_output['y_end'] > 100/1.5) & (data_output['y'] > 100/1.5))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from set piece situation not being wide enough and ending outside box are not crosses
    
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['y_end'] > 63.2) | (data_output['y_end'] < 36.8)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, 
            data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((((data_output['y'] <= 78.9) & (data_output['y'] > 100/1.5)) | ((data_output['y'] < 100/3.0) & (data_output['y'] >= 21.1))) & (data_output['x'] < 83.0) & ((data_output['y_end'] >= 100/1.5) | (data_output['y_end'] <= 100/3.0)), 
            0, data_output['Our Cross Qualifier']))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['x'] >= 83.0) & (data_output['x'] < 88.5) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 20.0), 0, 
            np.where((data_output['x'] >= 88.5) & (data_output['x'] < 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 15.0), 0,
                np.where((data_output['x'] >= 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 10.0), 0, 
                    data_output['Our Cross Qualifier']))))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['Headed Pass']==1) | (data_output['throw_in_taken']==1)), 0, data_output['Our Cross Qualifier'])) # if the pass is headed then 0

    data_output['cutback'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['Our Cross Qualifier'] = np.where(data_output['cutback']==1, 1, data_output['Our Cross Qualifier'])
    data_output['cutback'] = np.where(data_output['x'] < 83.0, 0, 
        np.where((data_output['y'] >= 95.0) | (data_output['y'] <= 5.0), 0, 
            np.where(data_output['cutback']==1, 1, 
                np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] >= 88.5) & (data_output['x'] <= 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
                    1, 
                    np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] > 94.2) & (data_output['x_end'] < 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
                        1, 0)))))

    return data_output



def cross_label_v3 (data_output):
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, #if there is cross qualifier then 1
        np.where(data_output['x'] < 75.0, 0, #if the pas starts from behind the halfway line then 0
            np.where((data_output['y'] > 25) & (data_output['y'] < 75), 0, #if the pass starts too frontal then 0
                np.where(data_output['x_end'] < 79.0, 0, #if the pass ends too behind then 0
                    np.where((np.abs(data_output['y'] - 50.0) < np.abs(data_output['y_end'] - 50.0)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, #if the pass is directed wide then 0
                        np.where((np.abs(data_output['y'] - data_output['y_end']) < 25.0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)), 0, 1))))))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0, data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x'] < 75.0) | ((data_output['y'] < 0.5*21.1) | (data_output['y'] > 0.5*(100+78.9)))), 0, data_output['Our Cross Qualifier'])) #low passes should start not too behind or not too wide
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.75*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] < 83.0) & (data_output['x'] >= 75.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier'])) #we set an upper bound on the distance travelled on the x axis proportional to the distance travelled on the y axis
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.5*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] >= 83.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier']))

    #new
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1,
        np.where((data_output['x'] <= 83) & (data_output['x_end'] <= 88.5) & (data_output['chipped_pass']==0), 0, data_output['Our Cross Qualifier']))
    
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x_end'] < 83.0) | ((data_output['y_end'] > 78.9) | (data_output['y_end'] < 21.1))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (data_output['x'] < 83.0) & (data_output['y'] >= 21.1) & (data_output['y'] <= 78.9), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)) & (((data_output['y_end'] < 100/3.0) & (data_output['y'] < 100/3.0)) | ((data_output['y_end'] > 100/1.5) & (data_output['y'] > 100/1.5))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from set piece situation not being wide enough and ending outside box are not crosses

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['y_end'] > 63.2) | (data_output['y_end'] < 36.8)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, 
            data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((((data_output['y'] <= 78.9) & (data_output['y'] > 100/1.5)) | ((data_output['y'] < 100/3.0) & (data_output['y'] >= 21.1))) & (data_output['x'] < 83.0) & ((data_output['y_end'] >= 100/1.5) | (data_output['y_end'] <= 100/3.0)), 
            0, data_output['Our Cross Qualifier']))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['x'] >= 83.0) & (data_output['x'] < 88.5) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 20.0), 0, 
            np.where((data_output['x'] >= 88.5) & (data_output['x'] < 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 15.0), 0,
                np.where((data_output['x'] >= 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 10.0), 0, 
                    data_output['Our Cross Qualifier']))))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['Headed Pass']==1) | (data_output['throw_in_taken']==1)), 0, data_output['Our Cross Qualifier'])) # if the pass is headed then 0

    data_output['cutback'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['Our Cross Qualifier'] = np.where(data_output['cutback']==1, 1, data_output['Our Cross Qualifier'])
    data_output['cutback'] = np.where(data_output['x'] < 83.0, 0, 
        np.where((data_output['y'] >= 95.0) | (data_output['y'] <= 5.0), 0, 
            np.where(data_output['cutback']==1, 1, 
                np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] >= 88.5) & (data_output['x'] <= 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
                    1, 
                    np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] > 94.2) & (data_output['x_end'] < 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 2.0), 
                        1, 0)))))

    return data_output



def cross_label_v4 (data_output):
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, #if there is cross qualifier then 1
        np.where(data_output['x'] < 75.0, 0, #if the pas starts from behind the halfway line then 0
            np.where((data_output['y'] > 100/3.0) & (data_output['y'] < 100/1.5), 0, #if the pass starts too frontal then 0
                np.where(data_output['x_end'] < 79.0, 0, #if the pass ends too behind then 0
                    np.where((np.abs(data_output['y'] - 50.0) < np.abs(data_output['y_end'] - 50.0)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, #if the pass is directed wide then 0
                        np.where((np.abs(data_output['y'] - data_output['y_end']) < 25.0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)), 0, 1))))))
                            
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['y_end'] < 18.1) | (data_output['y_end'] > 81.9), 0, data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x'] < 75.0) | ((data_output['y'] < 0.5*21.1) | (data_output['y'] > 0.5*(100+78.9)))), 0, data_output['Our Cross Qualifier'])) #low passes should start not too behind or not too wide
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.75*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] < 83.0) & (data_output['x'] >= 75.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier'])) #we set an upper bound on the distance travelled on the x axis proportional to the distance travelled on the y axis
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((np.abs(data_output['x_end'] - data_output['x']) > 0.5*np.abs(data_output['y_end'] - data_output['y'])) & (data_output['x'] >= 83.0) & (data_output['blocked_pass']==0), 0, data_output['Our Cross Qualifier']))

    #new
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1,
        np.where((data_output['x'] <= 83) & (data_output['x_end'] <= 88.5) & (data_output['chipped_pass']==0), 0, data_output['Our Cross Qualifier']))
    
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & ((data_output['x_end'] < 83.0) | ((data_output['y_end'] > 78.9) | (data_output['y_end'] < 21.1))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (data_output['x'] < 83.0) & (data_output['y'] >= 21.1) & (data_output['y'] <= 78.9), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from open play ending outside the box are not crosses
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['chipped_pass']==0) & (((data_output['y'] < 21.1) | (data_output['y'] > 78.9)) | (data_output['x'] < 83.0)) & (((data_output['y_end'] < 100/3.0) & (data_output['y'] < 100/3.0)) | ((data_output['y_end'] > 100/1.5) & (data_output['y'] > 100/1.5))), 0, 
            data_output['Our Cross Qualifier'])) #all low passes from set piece situation not being wide enough and ending outside box are not crosses

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['y_end'] > 63.2) | (data_output['y_end'] < 36.8)) & (np.sign(data_output['y'] - 50.0) == np.sign(data_output['y_end'] - 50.0)), 0, 
            data_output['Our Cross Qualifier']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((((data_output['y'] <= 78.9) & (data_output['y'] > 100/1.5)) | ((data_output['y'] < 100/3.0) & (data_output['y'] >= 21.1))) & (data_output['x'] < 83.0) & ((data_output['y_end'] >= 100/1.5) | (data_output['y_end'] <= 100/3.0)), 
            0, data_output['Our Cross Qualifier']))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where((data_output['x'] >= 83.0) & (data_output['x'] < 88.5) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 20.0), 0, 
            np.where((data_output['x'] >= 88.5) & (data_output['x'] < 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 15.0), 0,
                np.where((data_output['x'] >= 94.2) & (data_output['y'] <= 78.9) & (data_output['y'] >= 21.1) & (np.abs(data_output['y'] - data_output['y_end']) < 10.0), 0, 
                    data_output['Our Cross Qualifier']))))

    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(((data_output['Headed Pass']==1) | (data_output['throw_in_taken']==1)), 0, data_output['Our Cross Qualifier'])) # if the pass is headed then 0

    data_output['cutback'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]
    data_output['Our Cross Qualifier'] = np.where(data_output['cutback']==1, 1, data_output['Our Cross Qualifier'])
    data_output['cutback'] = np.where(data_output['x'] < 83.0, 0, 
        np.where((data_output['y'] >= 95.0) | (data_output['y'] <= 5.0), 0, 
            np.where(data_output['cutback']==1, 1, 
                np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] >= 88.5) & (data_output['x'] <= 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 3.0), 
                    1, 
                    np.where((data_output['Our Cross Qualifier']==1) & (data_output['x'] > 94.2) & (data_output['x_end'] < 94.2) & (data_output['y'] >= 0.5*21.1) & (data_output['y'] <= 0.5*178.9) & (data_output['x_end'] >= 80.0) & (data_output['y_end'] <= 78.9) & (data_output['y_end'] >= 21.1) & (data_output['x'] - data_output['x_end'] >= 3.0), 
                        1, 0)))))

    #make sure we count new cutbacks when the ball ends up within the small box width
    data_output['cutback'] = np.where((data_output['cutback']==1) & ((data_output['y_end'] < 35.0) | (data_output['y_end'] > 65)) & (np.array([int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']])==0), 
        0, data_output['cutback'])
    data_output['cutback'] = np.where((data_output['cutback']==1) & (data_output['chipped_pass']==1) & (data_output['x'] - data_output['x_end'] < 9.0) & (np.array([int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']])==0), 
        0, data_output['cutback'])
    data_output['Our Cross Qualifier'] = np.where(data_output['cutback']==1, 1, data_output['Our Cross Qualifier'])

    def y_threshold (x,y):
        x0 = 75.0
        x1 = 83.0
        if x<= x1:
            if y < 50.0:
                y0 = 25
                y1 = 100/3.0
            else:
                y0 = 75
                y1 = 100/1.5
            slope = (x1 - x0)/(y1 - y0)
            intercept = y0 - slope*x0
            y_threshold = intercept + slope*x
            if ((y <= y_threshold) & (y < 50)) | ((y >= y_threshold) & (y > 50)):
                is_cross = 1
            else:
                is_cross = 0

        else:
            is_cross = 1

        return is_cross

    data_output['y_threshold'] = list(map(lambda x,y: y_threshold(x,y), data_output['x'], data_output['y']))
    data_output['Our Cross Qualifier'] = np.where(data_output['OPTA Cross Qualifier']==1, 1, 
        np.where(data_output['y_threshold']==0, 0, data_output['Our Cross Qualifier']))
    data_output = data_output.drop(['y_threshold'], axis = 1)


    return data_output



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
        data_output_grouped = data_output.groupby([x for x in data_output.columns if (x not in ['qualifier_id', 'value']) & (data_output[x].count() > 0)])
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

    data_output = cross_label_v4(data_output)

    data_output['Attacking Team Name'] = np.where(data_output['team_id']==data_output['home_team_id'], data_output['home_team_name'], data_output['away_team_name'])
    data_output['Defending Team Name'] = np.where(data_output['team_id']==data_output['home_team_id'], data_output['away_team_name'], data_output['home_team_name'])

    #crosses can't be switch of play or through balls
    data_output['cross_to_remove'] = [(len(set([196,4]).intersection(set([int(y) for y in x.split(', ')]))) > 0) for x in data_output['qualifier_ids']]
    data_output['cross_to_remove'] = np.where((data_output['cross_to_remove']==1) & (data_output['corner_taken']==1), 0, data_output['cross_to_remove'])

    data_output['cross_to_remove'] = np.where(data_output['OPTA Cross Qualifier']==1, 0, 
        np.where(data_output['cross_to_remove']==1, 1,
        np.where((data_output['x'] < 83) & (data_output['x_end'] < 83.0), 1, data_output['cross_to_remove'])))

    data_output['out_of_pitch'] = np.where((data_output['x_end'] >= 100.0) | (data_output['y_end'] <= 0) | (data_output['y_end'] >= 100), 1, 0)
    data_output['ending_too_wide'] = np.where(((data_output['y_end'] <= 21) | (data_output['y_end'] >= 79)) & (data_output['y_end'] < 100.0) & (data_output['y_end'] > 0.0) & (data_output['x_end'] < 100.0) & (np.sign(data_output['y'] - 50.0) != np.sign(data_output['y_end'] - 50.0)), 1, 0)

    data_output = data_output[(data_output['Our Cross Qualifier']==1) & (data_output.cross_to_remove==0)].reset_index(drop=True)

    data_output = data_output.sort_values(['period_id', 'Time_in_Seconds']).reset_index(drop=True)
    data_output['cross_within_3_secs_after_another_cross'] = 0
    for i in range(2, data_output.shape[0]):
        if ((data_output.Time_in_Seconds.iloc[i] - data_output.Time_in_Seconds.iloc[i-1] <= 3) & (data_output.period_id.iloc[i] == 
            data_output.period_id.iloc[i-1])) | (data[(data.period_id==
            data_output.period_id.iloc[i]) & (data.Time_in_Seconds < data_output.Time_in_Seconds.iloc[i]) & (data.unique_event_id != 
            data_output.unique_event_id.iloc[i])].drop_duplicates(['unique_event_id']).unique_event_id.iloc[-1] == 
            data_output.unique_event_id.iloc[i-1]):

            data_output.loc[data_output.unique_event_id==data_output.unique_event_id.iloc[i], 'cross_within_3_secs_after_another_cross'] = 1

    data_output['cross_to_remove'] = np.where(data_output['OPTA Cross Qualifier']==1, 0, 
        np.where(data_output['cross_to_remove']==1, 1, 
            np.where((data_output['cross_within_3_secs_after_another_cross']==1) & (data_output['x'] >= 83) & (data_output['y'] <= 75) & (data_output['y'] >= 25) & (data_output['cutback']==0), 1, 
                data_output['cross_to_remove'])))

    data_output['OPTA Pull Back Qualifier'] = [int(195 in [int(y) for y in x.split(', ')]) for x in data_output['qualifier_ids']]


    return data_output[(data_output['Our Cross Qualifier']==1) & (data_output.cross_to_remove==0)].drop(['cross_within_3_secs_after_another_cross'], axis = 1).reset_index(drop = True)
