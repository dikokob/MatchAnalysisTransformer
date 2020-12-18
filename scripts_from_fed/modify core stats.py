#here we set up the loop through the folders from past season
import os 
import time
import pandas as pd
import numpy as np 

os.chdir("\\\ctgshares\\Drogba\\Analysts\\FB\\automation scripts") #directory where the function lies

from opta_files_manipulation_functions import opta_event_file_manipulation, match_results_file_manipulation
from opta_core_stats_output_function import opta_core_stats_output
#from tracking_data_manipulation_function import tracking_data_manipulation_new, time_possession

def modify_core_stats (core_stats, competition, season):

    parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}'.format(season)
    competition_folder = os.path.join(parent_folder, competition)
    subfolders_to_keep = [x for x in os.listdir(competition_folder) if ('spectrum' not in x) & ('f73' not in x)]

    #core_stats['Sent Off'] = 0
    #core_stats['Retired'] = 0
    core_stats['Time Played Calculated From Tracking Data'] = 'Yes'

    cnt = 0
    for game in core_stats['Game ID'].unique():

        if sum([x.startswith('g') for x in subfolders_to_keep]) > 0: #if true, we already hit the game level folders
            game_folder_collections = [competition]
        else:
            game_folder_collections = subfolders_to_keep

        for folder in game_folder_collections:
            if folder == competition: #if true, it means we need to climb a level less 
                path_folder = competition_folder
            else:
                path_folder = os.path.join(competition_folder, folder)
            subfolders = os.listdir(path_folder)

            for sub in subfolders:
                if sub.replace('g','f') == game:
                    path_events = [os.path.join(path_folder, sub, x) for x in os.listdir(os.path.join(path_folder, sub)) if (x.endswith('.xml')) & ('f24' in x)][0]
                    path_match_results = [os.path.join(path_folder, sub, x) for x in os.listdir(os.path.join(path_folder, sub)) if (x.endswith('.xml')) & ('srml' in x)][0] 
                    #current_game_id = sub
                    break

        opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
        referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results) 
        opta_event_data_df = opta_event_data_df[opta_event_data_df.period_id <= 4].reset_index(drop=True)
        opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']

        all_player_ids = core_stats[core_stats['Game ID']==game]['Player ID'].tolist()
        all_player_names = core_stats[core_stats['Game ID']==game]['Player Name'].tolist()
        all_player_team_id = core_stats[core_stats['Game ID']==game]['Team ID'].tolist()
        all_player_team_name = core_stats[core_stats['Game ID']==game]['Team Name'].tolist()
        all_player_start = core_stats[core_stats['Game ID']==game]['Start'].tolist()
        all_player_sub_on = core_stats[core_stats['Game ID']==game]['Substitute On'].tolist()
        all_player_sub_off = core_stats[core_stats['Game ID']==game]['Substitute Off'].tolist()
        all_player_sent_off = core_stats[core_stats['Game ID']==game]['Sent Off'].tolist()
        all_player_retired = core_stats[core_stats['Game ID']==game]['Retired'].tolist()
        all_player_off_pitch = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==77].unique().tolist()))) == 1, 1, 0).tolist() for x in all_player_ids]
        all_player_back_on_pitch = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==21].unique().tolist()))) == 1, 1, 0).tolist() for x in all_player_ids]

        #all_player_sent_off = np.where(np.array(all_player_sub_off)==1, 0, np.array(all_player_sent_off)).tolist()
        #core_stats.loc[core_stats['Game ID']==game, 'Sent Off'] = all_player_sent_off
        #core_stats.loc[core_stats['Game ID']==game, 'Retired'] = all_player_retired
        #core_stats.loc[core_stats['Game ID']==game, 'Off Pitch'] = all_player_off_pitch


        if sum(core_stats[core_stats['Game ID']==game]['Time Played'].isnull()) == 0:
            continue

        else:
            for i in range(len(all_player_ids)):
                if (np.isnan(core_stats[(core_stats['Game ID']==game) & (core_stats['Player ID']==all_player_ids[i])]['Time Played'].iloc[0])) | (core_stats[(core_stats['Game ID']==game) & (core_stats['Player ID']==all_player_ids[i])]['Time Played'].iloc[0] is None):
                    if all_player_start[i] == 1:
                        time_player_in = 0
                        period_player_in = 1
                    else: 
                        time_player_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['time_in_seconds'].iloc[0]
                        period_player_in = opta_event_data_df[(opta_event_data_df.type_id==19) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['period_id'].iloc[0]

                
                    if (all_player_sub_off[i] + all_player_sent_off[i] + all_player_retired[i] >=1):
                    
                        if all_player_sub_off[i] == 1:
                            time_player_out = opta_event_data_df[(opta_event_data_df.type_id==18) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['time_in_seconds'].iloc[0]
                            period_player_out = opta_event_data_df[(opta_event_data_df.type_id==18) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['period_id'].iloc[0]

                        elif all_player_retired[i] == 1:
                            time_player_out = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['time_in_seconds'].iloc[0]
                            period_player_out = opta_event_data_df[(opta_event_data_df.type_id==20) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['period_id'].iloc[0]


                        elif all_player_sent_off[i] == 1:
                            time_player_out = opta_event_data_df[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33])) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['time_in_seconds'].iloc[0]
                            period_player_out = opta_event_data_df[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33])) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['period_id'].iloc[0]

                        else:
                            time_player_out = None 
                            period_player_out = None 

                    else:
                        time_player_out = opta_event_data_df[(opta_event_data_df.period_id==opta_event_data_df.period_id.max()) & (opta_event_data_df.type_id==30)]['time_in_seconds'].max()
                        period_player_out = opta_event_data_df.period_id.max()

                    if (all_player_off_pitch[i] == 1) & (all_player_back_on_pitch[i] == 1):
                        time_player_off_temp = opta_event_data_df[(opta_event_data_df.type_id==77) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['time_in_seconds'].iloc[0]
                        period_player_off_temp = opta_event_data_df[(opta_event_data_df.type_id==77) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['period_id'].iloc[0]
                        time_player_back = opta_event_data_df[(opta_event_data_df.type_id==21) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['time_in_seconds'].iloc[0]
                        period_player_back = opta_event_data_df[(opta_event_data_df.type_id==21) & (opta_event_data_df.team_id==int(all_player_team_id[i].replace('t',''))) & (opta_event_data_df.player_id==int(all_player_ids[i].replace('p','')))]['period_id'].iloc[0]

                    else:
                        time_player_off_temp = None
                        period_player_off_temp = None
                        time_player_back = None
                        period_player_back = None

                    

                    if period_player_out==period_player_in:
                        time_played = (time_player_out - time_player_in)
                        if time_player_off_temp is not None:
                            time_played -= (time_player_back - time_player_off_temp)

                    else:
                        time_played = 0
                        for period_id in np.linspace(period_player_in, period_player_out, period_player_out - period_player_in + 1).tolist():
                            if period_id == period_player_in:
                                time_played += (opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id==30)]['time_in_seconds'].max() - time_player_in)
                            elif period_id == period_player_out:
                                time_played += (time_player_out - opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id==32)]['time_in_seconds'].min())
                            else:
                                time_played += (opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id==30)]['time_in_seconds'].max() - opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id==32)]['time_in_seconds'].min())

                            if time_player_off_temp is not None:
                                if period_player_off_temp == period_player_back:
                                    if period_player_off_temp == period_id:
                                        time_played -= (time_player_back - time_player_off_temp)
                                else:
                                    if period_player_off_temp == period_id:
                                        time_played -= (opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id==30)]['time_in_seconds'].max() - time_player_off_temp)
                                    if period_player_back == period_id:
                                        time_played -= (time_player_back - opta_event_data_df[(opta_event_data_df.period_id==period_id) & (opta_event_data_df.type_id==32)]['time_in_seconds'].min())

                    core_stats.loc[(core_stats['Game ID']==game) & (core_stats['Player ID']==all_player_ids[i]), 'Time Played'] = time_played
                    core_stats.loc[(core_stats['Game ID']==game) & (core_stats['Player ID']==all_player_ids[i]), 'Time Played Calculated From Tracking Data'] = 'No'
        
        cnt += 1
        print ('game id {} completed, hence {} games have been processed for {} {}.'.format(game, cnt, competition, season))
    
    return core_stats











###loop
seasons = ['2020-21']
#seasons = ['2016-17', '2017-18', '2018-19']
for season in seasons:
    parent_folder = '\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}'.format(season)
    #folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x))]
    #folders_to_keep = ['Premier League']
    folders_to_keep = ['Champions League']
    #folders_to_keep = [x for x in os.listdir(parent_folder) if (('League' in x) | ('Cup' in x)) & (x != 'Premier League')]

    for competition in folders_to_keep:

        core_stats = pd.concat([pd.read_excel(os.path.join(parent_folder, competition, 'Opta Summaries Player Level', x)) for x in os.listdir(os.path.join(parent_folder, competition, 'Opta Summaries Player Level')) if ('core stats' in x) & ('~' not in x)], sort = False).reset_index(drop=True)

        if '{} g'.format(competition) in str(os.listdir(os.path.join(parent_folder, competition, 'Opta Summaries Player Level'))):
            for x in [os.path.join(parent_folder, competition, 'Opta Summaries Player Level', y) for y in os.listdir(os.path.join(parent_folder, competition, 'Opta Summaries Player Level')) if '{} g'.format(competition) in y]:
                os.remove(os.path.join(parent_folder, competition, 'Opta Summaries Player Level', x))
        
        final_df = modify_core_stats(core_stats, competition, season)

        writer = pd.ExcelWriter(os.path.join(parent_folder, competition, 'Opta Summaries Player Level\\{} {} core stats.xlsx'.format(competition, season)), engine='xlsxwriter')
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