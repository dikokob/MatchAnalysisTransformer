import pandas as pd
import numpy as np
import logging
import os
import json
import time
import xmltodict


class TrackingDataTransformer:
    config = None
    logger = None
    data_source = None
    match_date = None

    def __init__(self):
        #self.config = config
        self.logger = logging.getLogger('{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))

    def transform(self, df_second_phase_set_pieces, df_crosses_output, df_opta_core_stats,
                  df_opta_events, df_tracking_data_crosses, df_tracking_data_crosses, df_tracking_data_set_pieces):
        """[summary]

        Args:
            df_second_phase_set_pieces (pd.DataFrame): [track_players_df]
            df_crosses_output (pd.DataFrame): [crosses v4]
            df_opta_core_stats (pd.DataFrame): [Crosses Output]
            df_second_phase_set_pieces (pd.DataFrame): [Set Pieces with 2nd Phase Output]
            df_player_names_raw (pd.DataFrame): [player_names_raw]
            players_df_lineup (pd.DataFrame): [players_df_lineup]
            opta_match_info (dict): [opta_event_file_manipulation, is a dict see SecondSpectrumTransformer]
            match_info (dict): [match meta_data, is a dict see SecondSpectrumTransformer]
            df_opta_events (pd.DataFrame): [event_data opta_event_file_manipulation, see SecondSpectrumTransformer]

        Returns:
            pd.DataFrame: [tracking data set pieces]
            pd.DataFrame: [tracking data crosses]


        """


        file_names = ['Set Pieces with 2nd Phase Output', 'Crosses Output']

        for file_name in file_names:

            ###import crosses alongside set pieces and corners and start to work out
            #path_crosses = '\\\ctgshares\\Drogba\\Analysts\\FB\\2019-20\\set pieces classification\\crosses'
            path_crosses = os.path.join(parent_folder, competition, 'Set Pieces & Crosses\\{}.xlsx'.format(file_name))
            data_crosses = pd.read_excel(path_crosses)

            if 'set pieces' in file_name.lower():
                data_crosses = data_crosses[(~data_crosses['Relevant OPTA Event ID'].duplicated()) | data_crosses['Relevant OPTA Event ID'].isnull()].reset_index(drop=True)
                writer = pd.ExcelWriter(path_crosses, engine='xlsxwriter')
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

                data_crosses_old = pd.read_excel(os.path.join(parent_folder, competition, 'Set Pieces & Crosses\\Set Pieces Output.xlsx'))
                data_crosses_old = data_crosses_old[(~data_crosses_old['Relevant OPTA Event ID'].duplicated()) | data_crosses_old['Relevant OPTA Event ID'].isnull()].reset_index(drop=True)
                writer = pd.ExcelWriter(os.path.join(parent_folder, competition, 'Set Pieces & Crosses\\Set Pieces Output.xlsx'), engine='xlsxwriter')
                data_crosses_old.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
                worksheet = writer.sheets['Sheet1']  # pull worksheet object
                for idx, col in enumerate(data_crosses_old):  # loop through all columns
                    series = data_crosses_old[col]
                    max_len = max((
                        series.astype(str).map(len).max(),  # len of largest item
                        len(str(series.name))  # len of column name/header
                    )) + 1  # adding a little extra space
                    worksheet.set_column(idx, idx, max_len)  # set column width
                writer.save()

            if 'crosses' in file_name.lower():
                path_crosses_tracking = os.path.join(parent_folder, competition, 'Set Pieces & Crosses\\tracking data crosses')
            if 'set pieces' in file_name.lower():
                path_crosses_tracking = os.path.join(parent_folder, competition, 'Set Pieces & Crosses\\tracking data set pieces')

            data_crosses_tracking = pd.concat([pd.read_excel(os.path.join(path_crosses_tracking,x)) for x in os.listdir(path_crosses_tracking)], axis = 0).reset_index(drop=True)

            if data_crosses.shape[0] > len(data_crosses['OPTA Event ID'].unique()):
                data_crosses = data_crosses.drop_duplicates(['OPTA Event ID']).reset_index(drop=True)
                writer = pd.ExcelWriter(path_crosses, engine='xlsxwriter')
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
                data_crosses = pd.read_excel(path_crosses)

            tracking_data_output(data_crosses, data_crosses_tracking, data_core_stats, season, competition)

            if 'Set Pieces' in file_name:
                file_name = 'Set Pieces'
            if 'Crosses' in file_name:
                file_name = 'Crosses'
            data_events = pd.read_excel(os.path.join(parent_folder,
                                                     competition, 'Set Pieces & Crosses',
                                                     'Tracking Data Info From {} Output.xlsx'.format(file_name)))
            data_events_grouped = data_events.groupby(list(data_events)[0])['Player ID'].count().reset_index()

            print ('Range of players in box is {} - {} for {} in {} {}. Number of {} is {} and number of tracking {} is {}.'.format(min(data_events_grouped['Player ID']),
                                                                                                                                    max(data_events_grouped['Player ID']), file_name, competition, season, file_name, data_crosses.shape[0], file_name, data_events_grouped.shape[0]))

    def tracking_data_output(self, data, data_tracking, df_opta_core_stats, df_opta_events):
        """[summary]

        Args:
            data (pd.DataFrame): []
            data_tracking (pd.DataFrame): []
            df_opta_core_stats (pd.DataFrame): []
            df_opta_events (pd.DataFrame): []

        Returns:
            pd.DataFrame: []


        """

        #prep
        opta_event_data_df = df_opta_events
        core_stats = df_opta_core_stats

        #fed scripts start
        if 'Set Piece Type' in list(data.columns):
            data['Set Piece OPTA Event ID'] = list(data['OPTA Event ID'])
            data['OPTA Event ID'] = np.where(data['Relevant OPTA Event ID'].isnull(), data['OPTA Event ID'],
                                             data['Relevant OPTA Event ID'])


        list_tracking_info = []
        cnt = 0
        start_time = time.process_time()
        current_game_id = 'g0'
        '''
        parent_folder = '\\\ctgshares\\Drogba\\API Data Files\\{}'.format(season)
        competition_folder = os.path.join(parent_folder, competition)
        subfolders_to_keep = [x for x in os.listdir(competition_folder) if ('spectrum' not in x) & ('f73' not in x)]
        '''
        for cross_id in data['OPTA Event ID']:
            if 'Set Piece Type' in list(data.columns):
                if data[data['OPTA Event ID']==cross_id]['Set Piece OPTA Event ID'].iloc[0] == data[data['OPTA Event ID']==cross_id]['Relevant OPTA Event ID'].iloc[0]:
                    freekick_pass_is_relevant = 'Yes'
                else:
                    freekick_pass_is_relevant = 'No'
            set_piece_id = data[data['OPTA Event ID']==cross_id]['Set Piece OPTA Event ID'].iloc[0]
            att_team_id = data[data['OPTA Event ID']==cross_id]['Attacking Team ID'].iloc[0]
            att_team_name = data[data['OPTA Event ID']==cross_id]['Attacking Team'].iloc[0]
            def_team_id = data[data['OPTA Event ID']==cross_id]['Defending Team ID'].iloc[0]
            def_team_name = data[data['OPTA Event ID']==cross_id]['Defending Team'].iloc[0]
            crosser_id = data[data['OPTA Event ID']==cross_id]['Player ID'].iloc[0]
            crosser_name = data[data['OPTA Event ID']==cross_id]['Player Name'].iloc[0]
            freekick_mins = data[data['OPTA Event ID']==cross_id]['min'].iloc[0]
            freekick_secs = data[data['OPTA Event ID']==cross_id]['sec'].iloc[0]
            freekick_period_id = data[data['OPTA Event ID']==cross_id]['period_id'].iloc[0]

            if 'Set Piece Type' in list(data.columns):
                if cross_id == data[data['OPTA Event ID']==cross_id]['Relevant OPTA Event ID'].iloc[0]:
                    crosser_id = data[data['OPTA Event ID']==cross_id]['Relevant Player ID'].iloc[0]
                    crosser_name = data[data['OPTA Event ID']==cross_id]['Relevant Player Name'].iloc[0]
                else:
                    crosser_id = data[data['OPTA Event ID']==cross_id]['Player ID'].iloc[0]
                    crosser_name = data[data['OPTA Event ID']==cross_id]['Player Name'].iloc[0]

            core_stats_reduced = core_stats[(core_stats['Game ID']==data[data['OPTA Event ID']==cross_id]['game_id'].iloc[0])].reset_index(drop = True)


            if data[data['OPTA Event ID']==cross_id]['game_id'].iloc[0] != current_game_id.replace('g','f'):
                '''
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
                        if sub.replace('g','f') == data[data['OPTA Event ID']==cross_id]['game_id'].iloc[0]:
                            path_events = [os.path.join(path_folder, sub, x) for x in os.listdir(os.path.join(path_folder, sub)) if (x.endswith('.xml')) & ('f24' in x)][0]
                            path_match_results = [os.path.join(path_folder, sub, x) for x in os.listdir(os.path.join(path_folder, sub)) if (x.endswith('.xml')) & ('srml' in x)][0]
                            current_game_id = sub
                            break

                opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name = opta_event_file_manipulation(path_events)
                referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw = match_results_file_manipulation(path_match_results)
                '''
                opta_event_data_df['time_in_seconds'] = opta_event_data_df['min']*60.0 + opta_event_data_df['sec']
                opta_event_data_df = opta_event_data_df[opta_event_data_df.period_id <= 4].reset_index(drop=True)

            if cross_id in data_tracking['OPTA Event ID'].tolist():
                #number_attack_in_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Number Of Attacking Players In Box'].iloc[0]
                #number_defend_in_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Number Of Defending Players In Box'].iloc[0]
                max_height_ball = data_tracking[data_tracking['OPTA Event ID']==cross_id]['max_height_ball'].iloc[0]
                end_height_ball = data_tracking[data_tracking['OPTA Event ID']==cross_id]['end_height_ball'].iloc[0]
                avg_speed_ball = data_tracking[data_tracking['OPTA Event ID']==cross_id]['avg_speed_ball'].iloc[0]
                avg_speed_ball_at_max_height = data_tracking[data_tracking['OPTA Event ID']==cross_id]['avg_speed_ball_at_max_height'].iloc[0]
                max_speed_ball = data_tracking[data_tracking['OPTA Event ID']==cross_id]['max_speed_ball'].iloc[0]
                speed_ball_at_end = data_tracking[data_tracking['OPTA Event ID']==cross_id]['speed_ball_at_end'].iloc[0]
                tot_frames = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Total Frames In Window'].iloc[0]
                start_frame = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Start Frame'].iloc[0]
                end_frame = data_tracking[data_tracking['OPTA Event ID']==cross_id]['End Frame'].iloc[0]

                attacking_player_ids_in_box = [None]
                attacking_player_names_in_box = [None]
                attacking_players_in_box_x = [None]
                attacking_players_in_box_y = [None]
                attacking_players_in_box_x_start = [None]
                attacking_players_in_box_y_start = [None]
                attacking_players_in_box_x_end = [None]
                attacking_players_in_box_y_end = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box'].iloc[0]) == str:
                    attacking_player_ids_in_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box'].iloc[0].split('; ')
                    attacking_player_names_in_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names In Box'].iloc[0].split('; ')
                    attacking_players_in_box_x = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position'].iloc[0]).split('; ')]
                    attacking_players_in_box_y = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position'].iloc[0]).split('; ')]
                    attacking_players_in_box_x_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Starting X Position'].iloc[0]).split('; ')]
                    attacking_players_in_box_y_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Starting Y Position'].iloc[0]).split('; ')]
                    attacking_players_in_box_x_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Ending X Position'].iloc[0]).split('; ')]
                    attacking_players_in_box_y_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Ending Y Position'].iloc[0]).split('; ')]

                attacking_player_ids_in_box_first_part = [None]
                attacking_player_names_in_box_first_part = [None]
                attacking_players_in_box_x_first_part = [None]
                attacking_players_in_box_y_first_part = [None]
                attacking_players_in_box_first_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box First Part'].iloc[0]) == str:
                    attacking_player_ids_in_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box First Part'].iloc[0].split('; ')
                    attacking_player_names_in_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names In Box First Part'].iloc[0].split('; ')
                    attacking_players_in_box_x_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position First Part'].iloc[0]).split('; ')]
                    attacking_players_in_box_y_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position First Part'].iloc[0]).split('; ')]
                    attacking_players_in_box_first_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Frames First Part Downsampled'].iloc[0]).split(';')]

                attacking_player_ids_in_box_second_part = [None]
                attacking_player_names_in_box_second_part = [None]
                attacking_players_in_box_x_second_part = [None]
                attacking_players_in_box_y_second_part = [None]
                attacking_players_in_box_second_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box Second Part'].iloc[0]) == str:
                    attacking_player_ids_in_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box Second Part'].iloc[0].split('; ')
                    attacking_player_names_in_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names In Box Second Part'].iloc[0].split('; ')
                    attacking_players_in_box_x_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position Second Part'].iloc[0]).split('; ')]
                    attacking_players_in_box_y_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position Second Part'].iloc[0]).split('; ')]
                    attacking_players_in_box_second_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Frames Second Part Downsampled'].iloc[0]).split(';')]

                attacking_player_ids_in_box_third_part = [None]
                attacking_player_names_in_box_third_part = [None]
                attacking_players_in_box_x_third_part = [None]
                attacking_players_in_box_y_third_part = [None]
                attacking_players_in_box_third_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box Third Part'].iloc[0]) == str:
                    attacking_player_ids_in_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs In Box Third Part'].iloc[0].split('; ')
                    attacking_player_names_in_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names In Box Third Part'].iloc[0].split('; ')
                    attacking_players_in_box_x_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position Third Part'].iloc[0]).split('; ')]
                    attacking_players_in_box_y_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position Third Part'].iloc[0]).split('; ')]
                    attacking_players_in_box_third_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Frames Third Part Downsampled'].iloc[0]).split(';')]


                defending_player_ids_in_box = [None]
                defending_player_names_in_box = [None]
                defending_players_in_box_x = [None]
                defending_players_in_box_y = [None]
                defending_players_in_box_x_start = [None]
                defending_players_in_box_y_start = [None]
                defending_players_in_box_x_end = [None]
                defending_players_in_box_y_end = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box'].iloc[0]) == str:
                    defending_player_ids_in_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box'].iloc[0].split('; ')
                    defending_player_names_in_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names In Box'].iloc[0].split('; ')
                    defending_players_in_box_x = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position'].iloc[0]).split('; ')]
                    defending_players_in_box_y = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position'].iloc[0]).split('; ')]
                    defending_players_in_box_x_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Starting X Position'].iloc[0]).split('; ')]
                    defending_players_in_box_y_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Starting Y Position'].iloc[0]).split('; ')]
                    defending_players_in_box_x_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Ending X Position'].iloc[0]).split('; ')]
                    defending_players_in_box_y_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Ending Y Position'].iloc[0]).split('; ')]

                defending_player_ids_in_box_first_part = [None]
                defending_player_names_in_box_first_part = [None]
                defending_players_in_box_x_first_part = [None]
                defending_players_in_box_y_first_part = [None]
                defending_players_in_box_first_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box First Part'].iloc[0]) == str:
                    defending_player_ids_in_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box First Part'].iloc[0].split('; ')
                    defending_player_names_in_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names In Box First Part'].iloc[0].split('; ')
                    defending_players_in_box_x_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position First Part'].iloc[0]).split('; ')]
                    defending_players_in_box_y_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position First Part'].iloc[0]).split('; ')]
                    defending_players_in_box_first_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Frames First Part Downsampled'].iloc[0]).split(';')]

                defending_player_ids_in_box_second_part = [None]
                defending_player_names_in_box_second_part = [None]
                defending_players_in_box_x_second_part = [None]
                defending_players_in_box_y_second_part = [None]
                defending_players_in_box_second_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box Second Part'].iloc[0]) == str:
                    defending_player_ids_in_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box Second Part'].iloc[0].split('; ')
                    defending_player_names_in_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names In Box Second Part'].iloc[0].split('; ')
                    defending_players_in_box_x_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position Second Part'].iloc[0]).split('; ')]
                    defending_players_in_box_y_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position Second Part'].iloc[0]).split('; ')]
                    defending_players_in_box_second_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Frames Second Part Downsampled'].iloc[0]).split(';')]


                defending_player_ids_in_box_third_part = [None]
                defending_player_names_in_box_third_part = [None]
                defending_players_in_box_x_third_part = [None]
                defending_players_in_box_y_third_part = [None]
                defending_players_in_box_third_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box Third Part'].iloc[0]) == str:
                    defending_player_ids_in_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs In Box Third Part'].iloc[0].split('; ')
                    defending_player_names_in_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names In Box Third Part'].iloc[0].split('; ')
                    defending_players_in_box_x_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position Third Part'].iloc[0]).split('; ')]
                    defending_players_in_box_y_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position Third Part'].iloc[0]).split('; ')]
                    defending_players_in_box_third_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Frames Third Part Downsampled'].iloc[0]).split(';')]


                attacking_player_ids_out_box = [None]
                attacking_player_names_out_box = [None]
                attacking_players_out_box_x = [None]
                attacking_players_out_box_y = [None]
                attacking_players_out_box_x_start = [None]
                attacking_players_out_box_y_start = [None]
                attacking_players_out_box_x_end = [None]
                attacking_players_out_box_y_end = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box'].iloc[0]) == str:
                    attacking_player_ids_out_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box'].iloc[0].split('; ')
                    attacking_player_names_out_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names Outside Box'].iloc[0].split('; ')
                    attacking_players_out_box_x = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position Outside Box'].iloc[0]).split('; ')]
                    attacking_players_out_box_y = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position Outside Box'].iloc[0]).split('; ')]
                    attacking_players_out_box_x_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Starting X Position Outside Box'].iloc[0]).split('; ')]
                    attacking_players_out_box_y_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Starting Y Position Outside Box'].iloc[0]).split('; ')]
                    attacking_players_out_box_x_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Ending X Position Outside Box'].iloc[0]).split('; ')]
                    attacking_players_out_box_y_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Ending Y Position Outside Box'].iloc[0]).split('; ')]

                attacking_player_ids_out_box_first_part = [None]
                attacking_player_names_out_box_first_part = [None]
                attacking_players_out_box_x_first_part = [None]
                attacking_players_out_box_y_first_part = [None]
                attacking_players_out_box_first_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box First Part'].iloc[0]) == str:
                    attacking_player_ids_out_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box First Part'].iloc[0].split('; ')
                    attacking_player_names_out_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names Outside Box First Part'].iloc[0].split('; ')
                    attacking_players_out_box_x_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position Outside Box First Part'].iloc[0]).split('; ')]
                    attacking_players_out_box_y_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position Outside Box First Part'].iloc[0]).split('; ')]
                    attacking_players_out_box_first_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Frames First Part Downsampled Outside Box'].iloc[0]).split(';')]

                attacking_player_ids_out_box_second_part = [None]
                attacking_player_names_out_box_second_part = [None]
                attacking_players_out_box_x_second_part = [None]
                attacking_players_out_box_y_second_part = [None]
                attacking_players_out_box_second_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box Second Part'].iloc[0]) == str:
                    attacking_player_ids_out_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box Second Part'].iloc[0].split('; ')
                    attacking_player_names_out_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names Outside Box Second Part'].iloc[0].split('; ')
                    attacking_players_out_box_x_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position Outside Box Second Part'].iloc[0]).split('; ')]
                    attacking_players_out_box_y_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position Outside Box Second Part'].iloc[0]).split('; ')]
                    attacking_players_out_box_second_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Frames Second Part Downsampled Outside Box'].iloc[0]).split(';')]


                attacking_player_ids_out_box_third_part = [None]
                attacking_player_names_out_box_third_part = [None]
                attacking_players_out_box_x_third_part = [None]
                attacking_players_out_box_y_third_part = [None]
                attacking_players_out_box_third_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box Third Part'].iloc[0]) == str:
                    attacking_player_ids_out_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player IDs Outside Box Third Part'].iloc[0].split('; ')
                    attacking_player_names_out_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Player Names Outside Box Third Part'].iloc[0].split('; ')
                    attacking_players_out_box_x_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average X Position Outside Box Third Part'].iloc[0]).split('; ')]
                    attacking_players_out_box_y_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Players Average Y Position Outside Box Third Part'].iloc[0]).split('; ')]
                    attacking_players_out_box_third_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Attacking Frames Third Part Downsampled Outside Box'].iloc[0]).split(';')]

                defending_player_ids_out_box = [None]
                defending_player_names_out_box = [None]
                defending_players_out_box_x = [None]
                defending_players_out_box_y = [None]
                defending_players_out_box_x_start = [None]
                defending_players_out_box_y_start = [None]
                defending_players_out_box_x_end = [None]
                defending_players_out_box_y_end = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box'].iloc[0]) == str:
                    defending_player_ids_out_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box'].iloc[0].split('; ')
                    defending_player_names_out_box = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names Outside Box'].iloc[0].split('; ')
                    defending_players_out_box_x = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position Outside Box'].iloc[0]).split('; ')]
                    defending_players_out_box_y = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position Outside Box'].iloc[0]).split('; ')]
                    defending_players_out_box_x_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Starting X Position Outside Box'].iloc[0]).split('; ')]
                    defending_players_out_box_y_start = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Starting Y Position Outside Box'].iloc[0]).split('; ')]
                    defending_players_out_box_x_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Ending X Position Outside Box'].iloc[0]).split('; ')]
                    defending_players_out_box_y_end = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Ending Y Position Outside Box'].iloc[0]).split('; ')]

                defending_player_ids_out_box_first_part = [None]
                defending_player_names_out_box_first_part = [None]
                defending_players_out_box_x_first_part = [None]
                defending_players_out_box_y_first_part = [None]
                defending_players_out_box_first_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box First Part'].iloc[0]) == str:
                    defending_player_ids_out_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box First Part'].iloc[0].split('; ')
                    defending_player_names_out_box_first_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names Outside Box First Part'].iloc[0].split('; ')
                    defending_players_out_box_x_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position Outside Box First Part'].iloc[0]).split('; ')]
                    defending_players_out_box_y_first_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position Outside Box First Part'].iloc[0]).split('; ')]
                    defending_players_out_box_first_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Frames First Part Downsampled Outside Box'].iloc[0]).split(';')]

                defending_player_ids_out_box_second_part = [None]
                defending_player_names_out_box_second_part = [None]
                defending_players_out_box_x_second_part = [None]
                defending_players_out_box_y_second_part = [None]
                defending_players_out_box_second_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box Second Part'].iloc[0]) == str:
                    defending_player_ids_out_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box Second Part'].iloc[0].split('; ')
                    defending_player_names_out_box_second_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names Outside Box Second Part'].iloc[0].split('; ')
                    defending_players_out_box_x_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position Outside Box Second Part'].iloc[0]).split('; ')]
                    defending_players_out_box_y_second_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position Outside Box Second Part'].iloc[0]).split('; ')]
                    defending_players_out_box_second_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Frames Second Part Downsampled Outside Box'].iloc[0]).split(';')]

                defending_player_ids_out_box_third_part = [None]
                defending_player_names_out_box_third_part = [None]
                defending_players_out_box_x_third_part = [None]
                defending_players_out_box_y_third_part = [None]
                defending_players_out_box_third_part_frame = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box Third Part'].iloc[0]) == str:
                    defending_player_ids_out_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player IDs Outside Box Third Part'].iloc[0].split('; ')
                    defending_player_names_out_box_third_part = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Player Names Outside Box Third Part'].iloc[0].split('; ')
                    defending_players_out_box_x_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average X Position Outside Box Third Part'].iloc[0]).split('; ')]
                    defending_players_out_box_y_third_part = [float(x) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Players Average Y Position Outside Box Third Part'].iloc[0]).split('; ')]
                    defending_players_out_box_third_part_frame = [int(float(x)) for x in str(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Defending Frames Third Part Downsampled Outside Box'].iloc[0]).split(';')]


                defending_player_ids_within_2m_crosser = [None]
                if type(data_tracking[data_tracking['OPTA Event ID']==cross_id]['Player IDS Of Opponents Within 2 Meters From The Crosser'].iloc[0]) == str:
                    defending_player_ids_within_2m_crosser = data_tracking[data_tracking['OPTA Event ID']==cross_id]['Player IDS Of Opponents Within 2 Meters From The Crosser'].iloc[0].split('; ')


                if crosser_id in list(core_stats_reduced['Player ID']):
                    player_ids = [x for y in [[crosser_id], attacking_player_ids_in_box, defending_player_ids_in_box, attacking_player_ids_out_box, defending_player_ids_out_box] for x in y if x is not None]
                    player_names = [x for y in [[crosser_name], attacking_player_names_in_box, defending_player_names_in_box, attacking_player_names_out_box, defending_player_names_out_box] for x in y if x is not None]
                    team_ids = [att_team_id if x in [y for z in [[crosser_id], attacking_player_ids_in_box, attacking_player_ids_out_box] for y in z] else def_team_id for x in player_ids]
                    team_names = [att_team_name if x in [y for z in [[crosser_id], attacking_player_ids_in_box, attacking_player_ids_out_box] for y in z] else def_team_name for x in player_ids]

                    #these coords are in the same order as the 'main' player ids order, hence they do not need reordering
                    x_coords = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['X Crosser Second Spectrum'].iloc[0]], attacking_players_in_box_x, defending_players_in_box_x, attacking_players_out_box_x, defending_players_out_box_x] for x in y if x is not None]
                    y_coords = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Y Crosser Second Spectrum'].iloc[0]], attacking_players_in_box_y, defending_players_in_box_y, attacking_players_out_box_y, defending_players_out_box_y] for x in y if x is not None]
                    x_coords_start = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['X Crosser Second Spectrum Start'].iloc[0]], attacking_players_in_box_x_start, defending_players_in_box_x_start, attacking_players_out_box_x_start, defending_players_out_box_x_start] for x in y if x is not None]
                    y_coords_start = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Y Crosser Second Spectrum Start'].iloc[0]], attacking_players_in_box_y_start, defending_players_in_box_y_start, attacking_players_out_box_y_start, defending_players_out_box_y_start] for x in y if x is not None]
                    x_coords_end = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['X Crosser Second Spectrum End'].iloc[0]], attacking_players_in_box_x_end, defending_players_in_box_x_end, attacking_players_out_box_x_end, defending_players_out_box_x_end] for x in y if x is not None]
                    y_coords_end = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Y Crosser Second Spectrum End'].iloc[0]], attacking_players_in_box_y_end, defending_players_in_box_y_end, attacking_players_out_box_y_end, defending_players_out_box_y_end] for x in y if x is not None]

                    #first part
                    player_ids_first_part = [x for y in [[crosser_id], attacking_player_ids_in_box_first_part, defending_player_ids_in_box_first_part, attacking_player_ids_out_box_first_part, defending_player_ids_out_box_first_part] for x in y if x is not None]
                    stored_indices = []
                    for p in player_ids_first_part:
                        stored_index = 0
                        for q in player_ids:
                            if p==q:
                                stored_indices.append(stored_index)
                            else:
                                stored_index += 1

                    x_coords_first_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['X Crosser Second Spectrum First Part Downsampled'].iloc[0]], attacking_players_in_box_x_first_part, defending_players_in_box_x_first_part, attacking_players_out_box_x_first_part, defending_players_out_box_x_first_part] for x in y if x is not None]
                    y_coords_first_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Y Crosser Second Spectrum First Part Downsampled'].iloc[0]], attacking_players_in_box_y_first_part, defending_players_in_box_y_first_part, attacking_players_out_box_y_first_part, defending_players_out_box_y_first_part] for x in y if x is not None]
                    frames_first_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Frame Crosser First Part Downsampled'].iloc[0]], attacking_players_in_box_first_part_frame, defending_players_in_box_first_part_frame, attacking_players_out_box_first_part_frame, defending_players_out_box_first_part_frame] for x in y if x is not None]


                    x_coords_first_part_reordered = [None]*len(player_ids)
                    y_coords_first_part_reordered = [None]*len(player_ids)
                    frames_first_part_reordered = [None]*len(player_ids)
                    if tot_frames >= 5:
                        for i in range(len(stored_indices)):
                            x_coords_first_part_reordered[stored_indices[i]] = x_coords_first_part[i]
                            y_coords_first_part_reordered[stored_indices[i]] = y_coords_first_part[i]
                            frames_first_part_reordered[stored_indices[i]] = frames_first_part[i]



                    player_ids_second_part = [x for y in [[crosser_id], attacking_player_ids_in_box_second_part, defending_player_ids_in_box_second_part, attacking_player_ids_out_box_second_part, defending_player_ids_out_box_second_part] for x in y if x is not None]
                    stored_indices = []
                    for p in player_ids_second_part:
                        stored_index = 0
                        for q in player_ids:
                            if p==q:
                                stored_indices.append(stored_index)
                            else:
                                stored_index += 1

                    x_coords_second_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['X Crosser Second Spectrum Second Part Downsampled'].iloc[0]], attacking_players_in_box_x_second_part, defending_players_in_box_x_second_part, attacking_players_out_box_x_second_part, defending_players_out_box_x_second_part] for x in y if x is not None]
                    y_coords_second_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Y Crosser Second Spectrum Second Part Downsampled'].iloc[0]], attacking_players_in_box_y_second_part, defending_players_in_box_y_second_part, attacking_players_out_box_y_second_part, defending_players_out_box_y_second_part] for x in y if x is not None]
                    frames_second_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Frame Crosser Second Part Downsampled'].iloc[0]], attacking_players_in_box_second_part_frame, defending_players_in_box_second_part_frame, attacking_players_out_box_second_part_frame, defending_players_out_box_second_part_frame] for x in y if x is not None]

                    x_coords_second_part_reordered = [None]*len(player_ids)
                    y_coords_second_part_reordered = [None]*len(player_ids)
                    frames_second_part_reordered = [None]*len(player_ids)
                    if tot_frames >= 5:
                        for i in range(len(stored_indices)):
                            x_coords_second_part_reordered[stored_indices[i]] = x_coords_second_part[i]
                            y_coords_second_part_reordered[stored_indices[i]] = y_coords_second_part[i]
                            frames_second_part_reordered[stored_indices[i]] = frames_second_part[i]


                    player_ids_third_part = [x for y in [[crosser_id], attacking_player_ids_in_box_third_part, defending_player_ids_in_box_third_part, attacking_player_ids_out_box_third_part, defending_player_ids_out_box_third_part] for x in y if x is not None]
                    stored_indices = []
                    for p in player_ids_third_part:
                        stored_index = 0
                        for q in player_ids:
                            if p==q:
                                stored_indices.append(stored_index)
                            else:
                                stored_index += 1

                    x_coords_third_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['X Crosser Second Spectrum Third Part Downsampled'].iloc[0]], attacking_players_in_box_x_third_part, defending_players_in_box_x_third_part, attacking_players_out_box_x_third_part, defending_players_out_box_x_third_part] for x in y if x is not None]
                    y_coords_third_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Y Crosser Second Spectrum Third Part Downsampled'].iloc[0]], attacking_players_in_box_y_third_part, defending_players_in_box_y_third_part, attacking_players_out_box_y_third_part, defending_players_out_box_y_third_part] for x in y if x is not None]
                    frames_third_part = [x for y in [[data_tracking[data_tracking['OPTA Event ID']==cross_id]['Frame Crosser Third Part Downsampled'].iloc[0]], attacking_players_in_box_third_part_frame, defending_players_in_box_third_part_frame, attacking_players_out_box_third_part_frame, defending_players_out_box_third_part_frame] for x in y if x is not None]

                    x_coords_third_part_reordered = [None]*len(player_ids)
                    y_coords_third_part_reordered = [None]*len(player_ids)
                    frames_third_part_reordered = [None]*len(player_ids)
                    if tot_frames >= 5:
                        for i in range(len(stored_indices)):
                            x_coords_third_part_reordered[stored_indices[i]] = x_coords_third_part[i]
                            y_coords_third_part_reordered[stored_indices[i]] = y_coords_third_part[i]
                            frames_third_part_reordered[stored_indices[i]] = frames_third_part[i]



                    attack_defend = ['Attacker' if x in [y for z in [[crosser_id], attacking_player_ids_in_box, attacking_player_ids_out_box] for y in z] else 'Defender' for x in player_ids]
                    attack_defend[0] = 'Crosser'
                    if 'Set Piece Type' in list(data.columns):
                        attack_defend[0] = 'Passer'
                    within_2m_crosser = ['Yes' if x in defending_player_ids_within_2m_crosser else 'No' for x in player_ids]
                    within_2m_crosser[0] = 'Not Applicable'

                    present_in_box_start = [int((x_coords_start[i] >= 83.0) & (y_coords_start[i] >= 21.1) & (y_coords_start[i] <= 78.9)) for i in range(len(x_coords_start))]
                    present_in_box_start = ['Yes' if present_in_box_start[i]==1 else 'No' for i in range(len(present_in_box_start))]
                    present_in_box_start = ['Not Applicable' if player_ids[i] in core_stats_reduced[(core_stats_reduced['Team ID'] == def_team_id) & (core_stats_reduced['Position ID']==1)]['Player ID'].tolist() else present_in_box_start[i] for i in range(len(player_ids))]
                    present_in_box_end = [int((x_coords_end[i] >= 83.0) & (y_coords_end[i] >= 21.1) & (y_coords_end[i] <= 78.9)) for i in range(len(x_coords_end))]
                    present_in_box_end = ['Yes' if present_in_box_end[i]==1 else 'No' for i in range(len(present_in_box_end))]
                    present_in_box_end = ['Not Applicable' if player_ids[i] in core_stats_reduced[(core_stats_reduced['Team ID'] == def_team_id) & (core_stats_reduced['Position ID']==1)]['Player ID'].tolist() else present_in_box_end[i] for i in range(len(player_ids))]
                    present_in_box = ['Yes' if x in [y for z in [attacking_player_ids_in_box, defending_player_ids_in_box] for y in z] else 'No' for x in player_ids]
                    present_in_box = ['Not Applicable' if player_ids[i] in core_stats_reduced[(core_stats_reduced['Team ID'] == def_team_id) & (core_stats_reduced['Position ID']==1)]['Player ID'].tolist() else present_in_box[i] for i in range(len(player_ids))]
                    present_in_box_first_part = [None]*len(player_ids)
                    present_in_box_second_part = [None]*len(player_ids)
                    present_in_box_third_part = [None]*len(player_ids)
                    if tot_frames >= 5:
                        present_in_box_first_part = ['Yes' if x in [y for z in [attacking_player_ids_in_box_first_part, defending_player_ids_in_box_first_part] for y in z] else 'No' for x in player_ids]
                        present_in_box_first_part = ['Not Applicable' if player_ids[i] in core_stats_reduced[(core_stats_reduced['Team ID'] == def_team_id) & (core_stats_reduced['Position ID']==1)]['Player ID'].tolist() else present_in_box_first_part[i] for i in range(len(player_ids))]
                        present_in_box_second_part = ['Yes' if x in [y for z in [attacking_player_ids_in_box_second_part, defending_player_ids_in_box_second_part] for y in z] else 'No' for x in player_ids]
                        present_in_box_second_part = ['Not Applicable' if player_ids[i] in core_stats_reduced[(core_stats_reduced['Team ID'] == def_team_id) & (core_stats_reduced['Position ID']==1)]['Player ID'].tolist() else present_in_box_second_part[i] for i in range(len(player_ids))]
                        present_in_box_third_part = ['Yes' if x in [y for z in [attacking_player_ids_in_box_third_part, defending_player_ids_in_box_third_part] for y in z] else 'No' for x in player_ids]
                        present_in_box_third_part = ['Not Applicable' if player_ids[i] in core_stats_reduced[(core_stats_reduced['Team ID'] == def_team_id) & (core_stats_reduced['Position ID']==1)]['Player ID'].tolist() else present_in_box_third_part[i] for i in range(len(player_ids))]
                    present_in_box[0] = np.where((data[data['OPTA Event ID']==cross_id]['X Coordinate'].iloc[0] >=83.0) &
                                                 (data[data['OPTA Event ID']==cross_id]['Y Coordinate'].iloc[0] >=21.1) &
                                                 (data[data['OPTA Event ID']==cross_id]['Y Coordinate'].iloc[0] <=78.9), 'Yes', 'No').tolist()
                    if 'Set Piece Type' in list(data.columns):
                        if cross_id == data[data['OPTA Event ID']==cross_id]['Relevant OPTA Event ID'].iloc[0]:
                            present_in_box[0] = np.where((data[data['OPTA Event ID']==cross_id]['Relevant X Coordinate'].iloc[0] >=83.0) &
                                                         (data[data['OPTA Event ID']==cross_id]['Relevant Y Coordinate'].iloc[0] >=21.1) &
                                                         (data[data['OPTA Event ID']==cross_id]['Relevant Y Coordinate'].iloc[0] <=78.9), 'Yes', 'No').tolist()

                # else:
                #     player_ids = [x for y in [attacking_player_ids_in_box, defending_player_ids_in_box, attacking_player_ids_out_box, defending_player_ids_out_box] for x in y if x is not None]
                #     player_names = [x for y in [attacking_player_names_in_box, defending_player_names_in_box, attacking_player_names_out_box, defending_player_names_out_box] for x in y if x is not None]
                #     team_ids = [att_team_id if x in [y for z in [attacking_player_ids_in_box, attacking_player_ids_out_box] for y in z] else def_team_id for x in player_ids]
                #     team_names = [att_team_name if x in [y for z in [attacking_player_ids_in_box, attacking_player_ids_out_box] for y in z] else def_team_name for x in player_ids]
                #     x_coords = [x for y in [attacking_players_in_box_x, defending_players_in_box_x, attacking_players_out_box_x, defending_players_out_box_x] for x in y if x is not None]
                #     y_coords = [x for y in [attacking_players_in_box_y, defending_players_in_box_y, attacking_players_out_box_y, defending_players_out_box_y] for x in y if x is not None]
                #     attack_defend = ['Attacker' if x in [y for z in [attacking_player_ids_in_box, attacking_player_ids_out_box] for y in z] else 'Defender' for x in player_ids]
                #     within_2m_crosser = ['Yes' if x in defending_player_ids_within_2m_crosser else 'No' for x in player_ids]
                #     present_in_box = ['Yes' if x in [y for z in [attacking_player_ids_in_box, defending_player_ids_in_box] for y in z] else 'No' for x in player_ids]


                if 'Cross Type' in list(data.columns):
                    df = pd.DataFrame([[cross_id]*len(player_ids), [set_piece_id]*len(player_ids),
                                       team_ids, team_names, player_ids, player_names,
                                       #[number_attack_in_box]*len(player_ids), [number_defend_in_box]*len(player_ids),
                                       present_in_box, present_in_box_start, present_in_box_first_part, present_in_box_second_part, present_in_box_third_part, present_in_box_end,
                                       x_coords, y_coords, x_coords_start, y_coords_start, x_coords_first_part_reordered, y_coords_first_part_reordered,
                                       x_coords_second_part_reordered, y_coords_second_part_reordered, x_coords_third_part_reordered, y_coords_third_part_reordered,
                                       x_coords_end, y_coords_end, attack_defend, within_2m_crosser,
                                       [max_height_ball]*len(player_ids), [end_height_ball]*len(player_ids), [avg_speed_ball_at_max_height]*len(player_ids),
                                       [avg_speed_ball]*len(player_ids),
                                       [max_speed_ball]*len(player_ids), [speed_ball_at_end]*len(player_ids),
                                       [start_frame]*len(player_ids), frames_first_part_reordered,
                                       frames_second_part_reordered, frames_third_part_reordered,
                                       [end_frame]*len(player_ids), [tot_frames]*len(player_ids)]).transpose()
                    df.columns = ['Cross OPTA Event ID', 'Set Piece OPTA Event ID',
                                  'Team ID', 'Team Name', 'Player ID', 'Player Name',
                                  #'Number of Att Players In Box', 'Number of Def Players In Box',
                                  'Present In The Box Average', 'Present In The Box Start', 'Present In The Box First Part', 'Present In The Box Second Part', 'Present In The Box Third Part', 'Present In The Box End',
                                  'Average X Position', 'Average Y Position', 'Start X Position', 'Start Y Position', 'First Part X Position', 'First Part Y Position',
                                  'Second Part X Position', 'Second Part Y Position', 'Third Part X Position', 'Third Part Y Position', 'End X Position', 'End Y Position',
                                  'Attacker/Defender',
                                  'Within 2 Metres Of Crosser',
                                  'Max Height Ball', 'End Height Ball', 'Average Speed Ball At Max Height',
                                  'Average Speed Ball', 'Max Speed Ball', 'Speed Ball At End',
                                  'Start Frame', 'First Part Frame', 'Second Part Frame', 'Third Part Frame',
                                  'End Frame', 'Total Frames']

                if 'Set Piece Type' in list(data.columns):
                    df = pd.DataFrame([[set_piece_id]*len(player_ids), [data[data['OPTA Event ID']==cross_id]['Relevant OPTA Event ID'].iloc[0]]*len(player_ids),
                                       [freekick_pass_is_relevant]*len(player_ids), team_ids, team_names, player_ids, player_names,
                                       #[number_attack_in_box]*len(player_ids), [number_defend_in_box]*len(player_ids),
                                       present_in_box, present_in_box_start, present_in_box_first_part, present_in_box_second_part, present_in_box_third_part, present_in_box_end,
                                       x_coords, y_coords, x_coords_start, y_coords_start, x_coords_first_part_reordered, y_coords_first_part_reordered,
                                       x_coords_second_part_reordered, y_coords_second_part_reordered, x_coords_third_part_reordered, y_coords_third_part_reordered,
                                       x_coords_end, y_coords_end, attack_defend, within_2m_crosser,
                                       [max_height_ball]*len(player_ids), [end_height_ball]*len(player_ids), [avg_speed_ball_at_max_height]*len(player_ids),
                                       [avg_speed_ball]*len(player_ids),
                                       [max_speed_ball]*len(player_ids), [speed_ball_at_end]*len(player_ids),
                                       [start_frame]*len(player_ids), frames_first_part_reordered,
                                       frames_second_part_reordered, frames_third_part_reordered,
                                       [end_frame]*len(player_ids), [tot_frames]*len(player_ids)]).transpose()
                    df.columns = ['Set Piece OPTA Event ID', 'Relevant Pass OPTA Event ID', 'Free Kick Pass Is Relevant',
                                  'Team ID', 'Team Name', 'Player ID', 'Player Name',
                                  #'Number of Att Players In Box', 'Number of Def Players In Box',
                                  'Present In The Box Average', 'Present In The Box Start', 'Present In The Box First Part', 'Present In The Box Second Part', 'Present In The Box Third Part', 'Present In The Box End',
                                  'Average X Position', 'Average Y Position', 'Start X Position', 'Start Y Position', 'First Part X Position', 'First Part Y Position',
                                  'Second Part X Position', 'Second Part Y Position', 'Third Part X Position', 'Third Part Y Position', 'End X Position', 'End Y Position',
                                  'Attacker/Defender',
                                  'Within 2 Metres Of Crosser',
                                  'Max Height Ball', 'End Height Ball', 'Average Speed Ball At Max Height',
                                  'Average Speed Ball', 'Max Speed Ball', 'Speed Ball At End',
                                  'Start Frame', 'First Part Frame', 'Second Part Frame', 'Third Part Frame',
                                  'End Frame', 'Total Frames']


            else:
                all_player_ids = core_stats_reduced['Player ID'].tolist()
                all_player_names = core_stats_reduced['Player Name'].tolist()
                all_player_team_id = core_stats_reduced['Team ID'].tolist()
                all_player_team_name = core_stats_reduced['Team Name'].tolist()
                all_player_start = core_stats_reduced['Start'].tolist()
                all_player_sub_on = core_stats_reduced['Substitute On'].tolist()
                all_player_sub_off = core_stats_reduced['Substitute Off'].tolist()
                all_player_sent_off = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[(opta_event_data_df.type_id==17) & (opta_event_data_df.qualifier_id.isin([32,33]))].unique().tolist()))) == 1, 1, 0).tolist() for x in all_player_ids]
                all_player_retired = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==20].unique().tolist()))) == 1, 1, 0).tolist() for x in all_player_ids]
                all_player_off_pitch = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==77].unique().tolist()))) == 1, 1, 0).tolist() for x in all_player_ids]
                all_player_back_on_pitch = [np.where(len(set([int(x.replace('p', ''))]).intersection(set(opta_event_data_df.player_id.loc[opta_event_data_df.type_id==21].unique().tolist()))) == 1, 1, 0).tolist() for x in all_player_ids]

                player_ids = []
                player_names = []
                team_ids = []
                team_names = []

                for i in range(len(all_player_ids)):

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
                        time_player_out = 10000
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


                    if period_player_in == period_player_out:
                        if (freekick_mins*60 + freekick_secs > time_player_in) & (freekick_mins*60 + freekick_secs < time_player_out) & (freekick_period_id == period_player_out):
                            if time_player_off_temp is None:
                                player_ids.append(all_player_ids[i])
                                player_names.append(all_player_names[i])
                                team_ids.append(all_player_team_id[i])
                                team_names.append(all_player_team_name[i])
                            else:
                                if (((freekick_mins*60 + freekick_secs <= time_player_off_temp) | (freekick_mins*60 + freekick_secs >= time_player_back)) & (freekick_period_id==period_player_off_temp)) | (freekick_period_id != period_player_off_temp):
                                    player_ids.append(all_player_ids[i])
                                    player_names.append(all_player_names[i])
                                    team_ids.append(all_player_team_id[i])
                                    team_names.append(all_player_team_name[i])
                    else:
                        if (freekick_period_id < period_player_out) & (freekick_period_id > period_player_in):
                            if time_player_off_temp is None:
                                player_ids.append(all_player_ids[i])
                                player_names.append(all_player_names[i])
                                team_ids.append(all_player_team_id[i])
                                team_names.append(all_player_team_name[i])
                            else:
                                if period_player_off_temp == period_player_back:
                                    if (((freekick_mins*60 + freekick_secs <= time_player_off_temp) | (freekick_mins*60 + freekick_secs >= time_player_back)) & (freekick_period_id==period_player_off_temp)) | (freekick_period_id != period_player_off_temp):
                                        player_ids.append(all_player_ids[i])
                                        player_names.append(all_player_names[i])
                                        team_ids.append(all_player_team_id[i])
                                        team_names.append(all_player_team_name[i])
                                else:
                                    if ((freekick_mins*60 + freekick_secs <= time_player_off_temp) & (freekick_period_id == period_player_off_temp)) | ((freekick_mins*60 + freekick_secs >= time_player_back) & (freekick_period_id == period_player_back)) | ((freekick_period_id != period_player_off_temp) & (freekick_period_id != period_player_back)):
                                        player_ids.append(all_player_ids[i])
                                        player_names.append(all_player_names[i])
                                        team_ids.append(all_player_team_id[i])
                                        team_names.append(all_player_team_name[i])
                        if freekick_period_id == period_player_out:
                            if freekick_mins*60 + freekick_secs < time_player_out:
                                if time_player_off_temp is None:
                                    player_ids.append(all_player_ids[i])
                                    player_names.append(all_player_names[i])
                                    team_ids.append(all_player_team_id[i])
                                    team_names.append(all_player_team_name[i])
                                else:
                                    if period_player_off_temp == period_player_back:
                                        if (((freekick_mins*60 + freekick_secs <= time_player_off_temp) | (freekick_mins*60 + freekick_secs >= time_player_back)) & (freekick_period_id==period_player_off_temp)) | (freekick_period_id != period_player_off_temp):
                                            player_ids.append(all_player_ids[i])
                                            player_names.append(all_player_names[i])
                                            team_ids.append(all_player_team_id[i])
                                            team_names.append(all_player_team_name[i])
                                    else:
                                        if ((freekick_mins*60 + freekick_secs <= time_player_off_temp) & (freekick_period_id == period_player_off_temp)) | ((freekick_mins*60 + freekick_secs >= time_player_back) & (freekick_period_id == period_player_back)) | ((freekick_period_id != period_player_off_temp) & (freekick_period_id != period_player_back)):
                                            player_ids.append(all_player_ids[i])
                                            player_names.append(all_player_names[i])
                                            team_ids.append(all_player_team_id[i])
                                            team_names.append(all_player_team_name[i])
                        if freekick_period_id == period_player_in:
                            if freekick_mins*60 + freekick_secs > time_player_in:
                                if time_player_off_temp is None:
                                    player_ids.append(all_player_ids[i])
                                    player_names.append(all_player_names[i])
                                    team_ids.append(all_player_team_id[i])
                                    team_names.append(all_player_team_name[i])
                                else:
                                    if period_player_off_temp == period_player_back:
                                        if (((freekick_mins*60 + freekick_secs <= time_player_off_temp) | (freekick_mins*60 + freekick_secs >= time_player_back)) & (freekick_period_id==period_player_off_temp)) | (freekick_period_id != period_player_off_temp):
                                            player_ids.append(all_player_ids[i])
                                            player_names.append(all_player_names[i])
                                            team_ids.append(all_player_team_id[i])
                                            team_names.append(all_player_team_name[i])
                                    else:
                                        if ((freekick_mins*60 + freekick_secs <= time_player_off_temp) & (freekick_period_id == period_player_off_temp)) | ((freekick_mins*60 + freekick_secs >= time_player_back) & (freekick_period_id == period_player_back)) | ((freekick_period_id != period_player_off_temp) & (freekick_period_id != period_player_back)):
                                            player_ids.append(all_player_ids[i])
                                            player_names.append(all_player_names[i])
                                            team_ids.append(all_player_team_id[i])
                                            team_names.append(all_player_team_name[i])



                attack_defend = ['Attacker' if x == att_team_id else 'Defender' for x in team_ids]
                present_in_box = [None]*len(player_ids)
                present_in_box_start = [None]*len(player_ids)
                present_in_box_first_part = [None]*len(player_ids)
                present_in_box_second_part = [None]*len(player_ids)
                present_in_box_third_part = [None]*len(player_ids)
                present_in_box_end = [None]*len(player_ids)
                within_2m_crosser = [None]*len(player_ids)

                if crosser_id in player_ids:
                    attack_defend[np.where(np.array(player_ids)==crosser_id)[0][0]] = 'Crosser'
                    within_2m_crosser[np.where(np.array(player_ids)==crosser_id)[0][0]] = 'Not Applicable'
                    present_in_box[np.where(np.array(player_ids)==crosser_id)[0][0]] = np.where((data[data['OPTA Event ID']==cross_id]['X Coordinate'].iloc[0] >=83.0) &
                                                                                                (data[data['OPTA Event ID']==cross_id]['Y Coordinate'].iloc[0] >=21.1) &
                                                                                                (data[data['OPTA Event ID']==cross_id]['Y Coordinate'].iloc[0] <=78.9), 'Yes', 'No').tolist()
                    if 'Set Piece Type' in list(data.columns):
                        attack_defend[np.where(np.array(player_ids)==crosser_id)[0][0]] = 'Passer'
                        if cross_id == data[data['OPTA Event ID']==cross_id]['Relevant OPTA Event ID'].iloc[0]:
                            present_in_box[np.where(np.array(player_ids)==crosser_id)[0][0]] = np.where((data[data['OPTA Event ID']==cross_id]['Relevant X Coordinate'].iloc[0] >=83.0) &
                                                                                                        (data[data['OPTA Event ID']==cross_id]['Relevant Y Coordinate'].iloc[0] >=21.1) &
                                                                                                        (data[data['OPTA Event ID']==cross_id]['Relevant Y Coordinate'].iloc[0] <=78.9), 'Yes', 'No').tolist()


                if 'Cross Type' in list(data.columns):
                    df = pd.DataFrame([[cross_id]*len(player_ids), [set_piece_id]*len(player_ids),
                                       team_ids, team_names, player_ids, player_names,
                                       #[None]*len(player_ids), [None]*len(player_ids),
                                       present_in_box, present_in_box_start, present_in_box_first_part, present_in_box_second_part, present_in_box_third_part, present_in_box_end,
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       attack_defend, within_2m_crosser,
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids)]).transpose()
                    df.columns = ['Cross OPTA Event ID', 'Set Piece OPTA Event ID',
                                  'Team ID', 'Team Name', 'Player ID', 'Player Name',
                                  #'Number of Att Players In Box', 'Number of Def Players In Box',
                                  'Present In The Box Average', 'Present In The Box Start', 'Present In The Box First Part', 'Present In The Box Second Part', 'Present In The Box Third Part', 'Present In The Box End',
                                  'Average X Position', 'Average Y Position', 'Start X Position', 'Start Y Position', 'First Part X Position', 'First Part Y Position',
                                  'Second Part X Position', 'Second Part Y Position', 'Third Part X Position', 'Third Part Y Position', 'End X Position', 'End Y Position',
                                  'Attacker/Defender',
                                  'Within 2 Metres Of Crosser',
                                  'Max Height Ball', 'End Height Ball', 'Average Speed Ball At Max Height',
                                  'Average Speed Ball', 'Max Speed Ball', 'Speed Ball At End',
                                  'Start Frame', 'First Part Frame', 'Second Part Frame', 'Third Part Frame',
                                  'End Frame', 'Total Frames']

                if 'Set Piece Type' in list(data.columns):
                    df = pd.DataFrame([[set_piece_id]*len(player_ids), [data[data['OPTA Event ID']==cross_id]['Relevant OPTA Event ID'].iloc[0]]*len(player_ids),
                                       [freekick_pass_is_relevant]*len(player_ids), team_ids, team_names, player_ids, player_names,
                                       #[None]*len(player_ids), [None]*len(player_ids),
                                       present_in_box, present_in_box_start, present_in_box_first_part, present_in_box_second_part, present_in_box_third_part, present_in_box_end,
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       attack_defend, within_2m_crosser,
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids), [None]*len(player_ids),
                                       [None]*len(player_ids), [None]*len(player_ids)]).transpose()
                    df.columns = ['Set Piece OPTA Event ID', 'Relevant Pass OPTA Event ID', 'Free Kick Pass Is Relevant',
                                  'Team ID', 'Team Name', 'Player ID', 'Player Name',
                                  #'Number of Att Players In Box', 'Number of Def Players In Box',
                                  'Present In The Box Average', 'Present In The Box Start', 'Present In The Box First Part', 'Present In The Box Second Part', 'Present In The Box Third Part', 'Present In The Box End',
                                  'Average X Position', 'Average Y Position', 'Start X Position', 'Start Y Position', 'First Part X Position', 'First Part Y Position',
                                  'Second Part X Position', 'Second Part Y Position', 'Third Part X Position', 'Third Part Y Position', 'End X Position', 'End Y Position',
                                  'Attacker/Defender',
                                  'Within 2 Metres Of Crosser',
                                  'Max Height Ball', 'End Height Ball', 'Average Speed Ball At Max Height',
                                  'Average Speed Ball', 'Max Speed Ball', 'Speed Ball At End',
                                  'Start Frame', 'First Part Frame', 'Second Part Frame', 'Third Part Frame',
                                  'End Frame', 'Total Frames']




            list_tracking_info.append(df)

            cnt += 1
            print ('{} crosses have been processed in {} seconds for {} {}.'.format(cnt, time.process_time() - start_time, competition,
                                                                                    season))


        summary_df = pd.concat(list_tracking_info, axis = 0).reset_index(drop=True)

        #now, we create the final columns to decide whether a player is in the box based on all the different in box splits, delayed arrival and subsequent number of attackers/defenders
        summary_df['Present In The Box'] = np.where(summary_df['Present In The Box Average'].isnull(), None,
                                                    np.where(summary_df['Present In The Box Average']=='Not Applicable', 'Not Applicable',
                                                             np.where(summary_df['Present In The Box Average']=='Yes', 'Yes',
                                                                      np.where((summary_df['Present In The Box Third Part']=='Yes') | (summary_df['Present In The Box End']=='Yes'), 'Yes', 'No'))))

        summary_df['Late Arrival'] = np.where(summary_df['Present In The Box'].isnull(), None,
                                              np.where(summary_df['Present In The Box']=='Not Applicable', 'Not Applicable',
                                                       np.where(summary_df['Present In The Box']=='No', 'No',
                                                                np.where(((summary_df['Present In The Box Third Part']=='Yes') | (summary_df['Present In The Box End']=='Yes')) & (summary_df['Present In The Box Start']==
                                                                                                                                                                                   'No') & (summary_df['Present In The Box First Part']=='No') & (summary_df['Present In The Box Second Part']=='No'), 'Yes', 'No'))))



        if 'Cross OPTA Event ID' in list(summary_df):
            summary_df = summary_df.groupby(['Cross OPTA Event ID']).apply(self.numbers_in_box).reset_index(drop=True)
        else:
            summary_df = summary_df.groupby(['Set Piece OPTA Event ID']).apply(self.numbers_in_box).reset_index(drop=True)


        summary_df = summary_df.drop_duplicates([list(summary_df)[0], list(summary_df)[1], 'Player ID']).reset_index(drop=True)
        '''
        writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Tracking Data Info from Crosses Output.xlsx'.format(season, competition), engine='xlsxwriter')
        if 'Set Piece Type' in list(data.columns):
            writer = pd.ExcelWriter('\\\ctgshares\\Drogba\\Advanced Data Metrics\\{}\\{}\\Set Pieces & Crosses\\Tracking Data Info from Set Pieces Output.xlsx'.format(season, competition), engine='xlsxwriter')
        summary_df.to_excel(writer, index = False, sheet_name = 'Sheet1')  # send df to writer
        worksheet = writer.sheets['Sheet1']  # pull worksheet object
        for idx, col in enumerate(summary_df):  # loop through all columns
            series = summary_df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()
        '''
        return summary_df

    @staticmethod
    def numbers_in_box (df):
        attackers = df[(df['Attacker/Defender']=='Attacker') & (df['Present In The Box']=='Yes')].shape[0]
        defenders = df[(df['Attacker/Defender']=='Defender') & (df['Present In The Box']=='Yes')].shape[0]
        if sum(df['Present In The Box'].isnull()) > 0:
            attackers = None
            defenders = None
        df['Number Of Att Players In Box'] = attackers
        df['Number Of Def Players In Box'] = defenders

        return df
