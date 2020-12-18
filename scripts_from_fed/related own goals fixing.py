import os 
import time
import pandas as pd
import numpy as np

os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies

from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
from opta_core_stats_output_function import opta_core_stats_output
from tracking_data_manipulation_function import tracking_data_manipulation_new, time_possession


while sum(['dummy' in x for y in os.listdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21')) for x in y.split(' ')]) > 0:
    time.sleep(5)
    pass

open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'related own goals fixing.txt', 'w').close()


core_stats_paths = []
core_stats_data = []



####European Leagues
parent_folder = '\\\ctgshares\\Drogba\\Other Leagues'

#list_of_dfs = []
#start_time = time.process_time() #to keep track of time taken
#loop over subfolders
for league in os.listdir(parent_folder):
    if league == 'scripts':
        continue
    
    path_league = parent_folder + '\\' + league
    #path_folder = folder
    seasons = os.listdir(path_league)

    #loop over games
    #for season in seasons:
    for season in ['2018-19', '2019-20', '2020-21']:

        path_league_season = os.path.join(path_league, season)

        if len(os.listdir(os.path.join(path_league_season, 'outputs'))) == 0:
            continue

        list_of_files = [y for y in os.listdir(os.path.join(path_league_season, 'outputs', 'Set Pieces & Crosses')) if (y.endswith('.xlsx')) & ('~' not in y) & ('counter' not in y.lower())]
        for x in list_of_files:
            if x.lower() in ['Shots From Set Pieces Output.xlsx'.lower(), 'Shots From Set Pieces with 2nd Phase Output.xlsx'.lower(), 'Shots from Set Pieces with 2nd Phase Output with f73 Integration.xlsx'.lower(), 'Shots From Crosses Output.xlsx'.lower(), 'Shots from Crosses Output with f73 Integration.xlsx'.lower()]:
                final_df = pd.read_excel(os.path.join(path_league_season, 'outputs', 'Set Pieces & Crosses', x))
                if final_df[final_df['Shot Outcome']=='Own Goal'].shape[0] > 0:
                    for shot_id in final_df[final_df['Shot Outcome']=='Own Goal']['Shot OPTA ID'].tolist():
                        if (final_df[final_df['Shot OPTA ID']==shot_id]['First Contact Shot'].iloc[0] == 1) & (final_df[final_df['Shot OPTA ID']==shot_id]['Shot Occurrence'].iloc[0] == 'Delayed'):
                            final_df.loc[final_df['Shot OPTA ID']==shot_id, 'Shot Occurrence'] = 'Related'

                    writer = pd.ExcelWriter(os.path.join(path_league_season, 'outputs', 'Set Pieces & Crosses', x), engine='xlsxwriter')
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

                    print ('file {} modified and exported for {} {} \n.'.format(x, league, season))


###English Competitions
seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21']

for season in seasons:
    parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}'.format(season)
    folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x))]
    #folders_to_keep = ['Premier League']

    for competition in folders_to_keep:
        #seasons_list.append(season)
        #competitions_list.append(competition)
        competition_folder = os.path.join(parent_folder, competition)
        subfolders_to_keep = [x for x in os.listdir(competition_folder) if ('spectrum' not in x) & ('f73' not in x)]

        list_of_files = [y for y in os.listdir(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}'.format(season, competition), 'Set Pieces & Crosses')) if (y.endswith('.xlsx')) & ('~' not in y) & ('counter' not in y.lower())]
        for x in list_of_files:
            if x.lower() in ['Shots From Set Pieces Output.xlsx'.lower(), 'Shots From Set Pieces with 2nd Phase Output.xlsx'.lower(), 'Shots from Set Pieces with 2nd Phase Output with f73 Integration.xlsx'.lower(), 'Shots From Crosses Output.xlsx'.lower(), 'Shots from Crosses Output with f73 Integration.xlsx'.lower()]:
                final_df = pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}'.format(season, competition), 'Set Pieces & Crosses', x))
                if final_df[final_df['Shot Outcome']=='Own Goal'].shape[0] > 0:
                    for shot_id in final_df[final_df['Shot Outcome']=='Own Goal']['Shot OPTA ID'].tolist():
                        if (final_df[final_df['Shot OPTA ID']==shot_id]['First Contact Shot'].iloc[0] == 1) & (final_df[final_df['Shot OPTA ID']==shot_id]['Shot Occurrence'].iloc[0] == 'Delayed'):
                            final_df.loc[final_df['Shot OPTA ID']==shot_id, 'Shot Occurrence'] = 'Related'

                    writer = pd.ExcelWriter(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}'.format(season, competition), 'Set Pieces & Crosses', x), engine='xlsxwriter')
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

                    print ('file {} modified and exported for {} {} \n.'.format(x, competition, season))



os.remove('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'related own goals fixing.txt') 