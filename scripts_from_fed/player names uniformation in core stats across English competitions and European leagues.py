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

open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'player names uniformation dummy.txt', 'w').close()


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

        core_stats_path = [os.path.join(path_league_season, 'outputs', x) for x in os.listdir(os.path.join(path_league_season, 'outputs')) if ('core stats' in x.lower()) & ('team' not in x.lower()) & ('~' not in x)][0]
        core_stats_data.append(pd.read_excel(core_stats_path))
        core_stats_paths.append(core_stats_path)



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

        core_stats_path = [os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Opta Summaries Player Level'.format(season, competition), 
            x) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Opta Summaries Player Level'.format(season, competition)) if ('core stats' in x.lower()) & ('~' not in x)][0]
        core_stats_df = pd.read_excel(core_stats_path)
        if core_stats_df.shape[1] == 27:
            core_stats_data.append(core_stats_df)
            core_stats_paths.append(core_stats_path)



core_stats_data_concatenated = pd.concat(core_stats_data, axis = 0).reset_index(drop=True)
if len(core_stats_data_concatenated['Player ID'].unique()) != core_stats_data_concatenated[['Player ID', 'Player Name']].drop_duplicates().shape[0]:
    for player_id in core_stats_data_concatenated['Player ID'].unique():
        if len(core_stats_data_concatenated[core_stats_data_concatenated['Player ID']==player_id]['Player Name'].unique()) > 1:
            print ('{} has at least two names.'.format(player_id))
            for x in core_stats_data:
                if player_id in x['Player ID'].tolist():
                    x.loc[x['Player ID']==player_id, 'Player Name'] = core_stats_data_concatenated[(core_stats_data_concatenated['Player ID']==
                        player_id) & (core_stats_data_concatenated['Season ID'] == 
                        max(core_stats_data_concatenated[core_stats_data_concatenated['Player ID']==player_id]['Season ID']))]['Player Name'].unique()[-1]

    for final_df, core_stats_path in zip(core_stats_data, core_stats_paths):
        writer = pd.ExcelWriter(core_stats_path, engine='xlsxwriter')
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

        print ('core stats exported to {}.'.format(core_stats_path))




####domestic competitions set pieces and crosses names
seasons = ['2016-17', '2017-18', '2018-19', '2019-20', '2020-21']

#list_of_dfs = []
#start_time = time.process_time() #to keep track of time taken
#loop over subfolders

for season in seasons:
    parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}'.format(season)
    folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x))]
    #folders_to_keep = ['Champions League']

    for competition in folders_to_keep:
        #seasons_list.append(season)
        #competitions_list.append(competition)
        competition_folder = os.path.join(parent_folder, competition)
        subfolders_to_keep = [x for x in os.listdir(competition_folder) if ('spectrum' not in x) & ('f73' not in x)]

        core_stats_path = [os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Opta Summaries Player Level'.format(season, competition), 
            x) for x in os.listdir('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Opta Summaries Player Level'.format(season, competition)) if ('core stats' in x.lower()) & ('~' not in x)][0]
        core_stats_data = pd.read_excel(core_stats_path).drop_duplicates(['Player ID']).reset_index(drop=True)
        if core_stats_data.shape[1] < 27:
            continue

        list_of_files = [y for y in os.listdir(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}'.format(season, competition), 'Set Pieces & Crosses')) if (y.endswith('.xlsx')) & ('~' not in y) & ('counter' not in y.lower())]
        for x in list_of_files:
            final_df = pd.read_excel(os.path.join('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}'.format(season, competition), 'Set Pieces & Crosses', x))
            if x.lower() in ['Set Pieces Output.xlsx'.lower(), 'Set Pieces with 2nd Phase Output.xlsx'.lower()]:
                columns_ids = ['Player ID', 'Relevant Player ID', 'First Contact Player ID', 'Defending Goalkeeper ID']
                columns_names = ['Player Name', 'Relevant Player Name', 'First Contact Player Name', 'Defending Goalkeeper Name']
            if x.lower() in ['Crosses Output.xlsx'.lower()]:
                columns_ids = ['Player ID', 'First Contact Player ID', 'Defending Goalkeeper ID']
                columns_names = ['Player Name', 'First Contact Player Name', 'Defending Goalkeeper Name']
            if x.lower() in ['Aerial Duels From Set Pieces Output.xlsx'.lower(), 'Aerial Duels From Crosses Output.xlsx'.lower()]:
                columns_ids = ['Aerial Duel Player ID', 'Other Aerial Duel Player ID']
                columns_names = ['Aerial Duel Player Name', 'Other Aerial Duel Player Name']
            if x.lower() in ['Shots From Set Pieces Output.xlsx'.lower(), 'Shots From Set Pieces with 2nd Phase Output.xlsx'.lower(), 'Shots from Set Pieces with 2nd Phase Output with f73 Integration.xlsx'.lower(), 'Shots From Crosses Output.xlsx'.lower(), 'Shots from Crosses Output with f73 Integration.xlsx'.lower()]:
                columns_ids = ['Shot Player ID']
                columns_names = ['Shot Player Name']
            if x.lower() in ['Tracking Data Info from Set Pieces Output.xlsx'.lower(), 'Tracking Data Info from Crosses Output.xlsx'.lower()]:
                columns_ids = ['Player ID']
                columns_names = ['Player Name']

            original_columns = final_df.columns.tolist()
            print ('dimensions of {} for {} {} are {} x {}'.format(x, competition, season, final_df.shape[0], final_df.shape[1]))

            for col_id, col_name in zip(columns_ids, columns_names):
                final_df = final_df.merge(core_stats_data[['Player ID', 'Player Name']], how = 'left', 
                    left_on = [col_id], right_on = ['Player ID'], suffixes = ('', ' new'))
                final_df[col_name] = final_df[final_df.columns.tolist()[-1]]
                if ('Player ID new' in final_df.columns.tolist()) | ('Player Name new' in final_df.columns.tolist()):
                    if 'Player ID new' in final_df.columns.tolist():
                        final_df = final_df.drop('Player ID new', axis = 1).reset_index(drop=True)
                    if 'Player Name new' in final_df.columns.tolist():
                        final_df = final_df.drop('Player Name new', axis = 1).reset_index(drop=True)
                else:
                    if col_id != 'Player ID':
                        final_df = final_df.drop(['Player ID'], axis = 1).reset_index(drop=True)
                    if col_name != 'Player Name':
                        final_df = final_df.drop(['Player Name'], axis = 1).reset_index(drop=True)

            print ('new dimensions of {} for {} {} are {} x {}'.format(x, competition, season, final_df.shape[0], final_df.shape[1]))
            print ('number of common columns of {} for {} {} are {}'.format(x, competition, season, len(set(original_columns).intersection(set(final_df.columns.tolist())))))
            print ('number of member to member equal columns of {} for {} {} are {}'.format(x, competition, season, sum(np.array(original_columns) == np.array(final_df.columns.tolist()))))


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



####European Leagues set pieces & crosses names
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

        core_stats_path = [os.path.join(path_league_season, 'outputs', x) for x in os.listdir(os.path.join(path_league_season, 'outputs')) if ('core stats' in x.lower()) & ('team' not in x.lower()) & ('~' not in x)][0]
        core_stats_data = pd.read_excel(core_stats_path).drop_duplicates(['Player ID']).reset_index(drop=True)

        list_of_files = [y for y in os.listdir(os.path.join(path_league_season, 'outputs', 'Set Pieces & Crosses')) if (y.endswith('.xlsx')) & ('~' not in y) & ('counter' not in y.lower())]
        for x in list_of_files:
            final_df = pd.read_excel(os.path.join(path_league_season, 'outputs', 'Set Pieces & Crosses', x))
            if x.lower() in ['Set Pieces Output.xlsx'.lower(), 'Set Pieces with 2nd Phase Output.xlsx'.lower()]:
                columns_ids = ['Player ID', 'Relevant Player ID', 'First Contact Player ID', 'Defending Goalkeeper ID']
                columns_names = ['Player Name', 'Relevant Player Name', 'First Contact Player Name', 'Defending Goalkeeper Name']
            if x.lower() in ['Crosses Output.xlsx'.lower()]:
                columns_ids = ['Player ID', 'First Contact Player ID', 'Defending Goalkeeper ID']
                columns_names = ['Player Name', 'First Contact Player Name', 'Defending Goalkeeper Name']
            if x.lower() in ['Aerial Duels From Set Pieces Output.xlsx'.lower(), 'Aerial Duels From Crosses Output.xlsx'.lower()]:
                columns_ids = ['Aerial Duel Player ID', 'Other Aerial Duel Player ID']
                columns_names = ['Aerial Duel Player Name', 'Other Aerial Duel Player Name']
            if x.lower() in ['Shots From Set Pieces Output.xlsx'.lower(), 'Shots From Set Pieces with 2nd Phase Output.xlsx'.lower(), 'Shots from Set Pieces with 2nd Phase Output with f73 Integration.xlsx'.lower(), 'Shots From Crosses Output.xlsx'.lower(), 'Shots from Crosses Output with f73 Integration.xlsx'.lower()]:
                columns_ids = ['Shot Player ID']
                columns_names = ['Shot Player Name']
            if x.lower() in ['Tracking Data Info from Set Pieces Output.xlsx'.lower(), 'Tracking Data Info from Crosses Output.xlsx'.lower()]:
                columns_ids = ['Player ID']
                columns_names = ['Player Name']

            original_columns = final_df.columns.tolist()
            print ('dimensions of {} for {} {} are {} x {}'.format(x, league, season, final_df.shape[0], final_df.shape[1]))

            for col_id, col_name in zip(columns_ids, columns_names):
                final_df = final_df.merge(core_stats_data[['Player ID', 'Player Name']], how = 'left', 
                    left_on = [col_id], right_on = ['Player ID'], suffixes = ('', ' new'))
                final_df[col_name] = final_df[final_df.columns.tolist()[-1]]
                if ('Player ID new' in final_df.columns.tolist()) | ('Player Name new' in final_df.columns.tolist()):
                    if 'Player ID new' in final_df.columns.tolist():
                        final_df = final_df.drop('Player ID new', axis = 1).reset_index(drop=True)
                    if 'Player Name new' in final_df.columns.tolist():
                        final_df = final_df.drop('Player Name new', axis = 1).reset_index(drop=True)
                else:
                    if col_id != 'Player ID':
                        final_df = final_df.drop(['Player ID'], axis = 1).reset_index(drop=True)
                    if col_name != 'Player Name':
                        final_df = final_df.drop(['Player Name'], axis = 1).reset_index(drop=True)

            print ('new dimensions of {} for {} {} are {} x {}'.format(x, league, season, final_df.shape[0], final_df.shape[1]))
            print ('number of common columns of {} for {} {} are {}'.format(x, league, season, len(set(original_columns).intersection(set(final_df.columns.tolist())))))
            print ('number of member to member equal columns of {} for {} {} are {}'.format(x, league, season, sum(np.array(original_columns) == np.array(final_df.columns.tolist()))))


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



os.remove('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format('2020-21') + '\\' + 'player names uniformation dummy.txt') 