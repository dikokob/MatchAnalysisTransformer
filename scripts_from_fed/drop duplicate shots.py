####we use this script only to remove duplicates from shots 

import os 
import pandas as pd 
import xmltodict
import numpy as np 
import time

# os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies

# from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation

while sum(['dummy' in x for y in os.listdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21')) for x in y.split(' ')]) > 0:
    time.sleep(5)
    pass

open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'drop duplicate shots dummy.txt', 'w').close()

seasons = ['2020-21']
#seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21']
#seasons = ['2019-20']
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

        shots_set_pieces_unique = shots_set_pieces.drop_duplicates().reset_index(drop=True)
        if 'index' in shots_set_pieces_unique.columns.tolist():
            shots_set_pieces_unique = shots_set_pieces_unique.drop(['index'], axis = 1)
        shots_crosses_unique = shots_crosses.drop_duplicates().reset_index(drop=True)
        if 'index' in shots_crosses_unique.columns.tolist():
            shots_crosses_unique = shots_crosses_unique.drop(['index'], axis = 1)

        shots_set_pieces_unique_id = shots_set_pieces.drop_duplicates('Shot OPTA ID').reset_index(drop=True)
        shots_crosses_unique_id = shots_crosses.drop_duplicates('Shot OPTA ID').reset_index(drop=True)

        print ('Number of set piece shots is {} while number of set piece unique shots is {} and number of set piece unique id shots is {} for {} {}'.format(shots_set_pieces.shape[0], 
            shots_set_pieces_unique.shape[0], shots_set_pieces_unique_id.shape[0], competition, season))
        print ('Number of cross shots is {} while number of cross unique shots is {} and number of cross unique id shots is {} for {} {}'.format(shots_crosses.shape[0], 
            shots_crosses_unique.shape[0], shots_crosses_unique_id.shape[0], competition, season))

        writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Set Pieces with 2nd Phase Output.xlsx'.format(season, competition), engine='xlsxwriter')
        #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\set pieces classification\\new folder for Tom\\Outside Target Zone Free Kicks due to distance with ball clause.xlsx', engine='xlsxwriter')
        shots_set_pieces_unique.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        worksheet = writer.sheets['Sheet1']  # pull worksheet object
        for idx, col in enumerate(shots_set_pieces_unique):  # loop through all columns
            series = shots_set_pieces_unique[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()


        writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Shots from Crosses Output.xlsx'.format(season, competition), engine='xlsxwriter')
        #writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\set pieces classification\\new folder for Tom\\Outside Target Zone Free Kicks due to distance with ball clause.xlsx', engine='xlsxwriter')
        shots_crosses_unique.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        worksheet = writer.sheets['Sheet1']  # pull worksheet object
        for idx, col in enumerate(shots_crosses_unique):  # loop through all columns
            series = shots_crosses_unique[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()


os.remove('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'drop duplicate shots dummy.txt') 