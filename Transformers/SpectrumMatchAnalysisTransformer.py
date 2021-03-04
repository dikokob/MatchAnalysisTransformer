import os
import pandas as pd
import numpy as np
import logging
import json
import xmltodict


class SpectrumMatchAnalysisTransformer:
    """

    """
    logger = None
    data_source = None

    def __init__(self, data_source: str):
        """[summary]

        Args:
            data_source (str): [description]
        """
        self.data_source = data_source
        self.logger = logging.getLogger(
            '{}.{}'.format(os.environ['FLASK_APP'], os.environ['session_folder']))

    @staticmethod
    def is_numeric(s) -> bool:
        """[summary]

        Args:
            s (): [description]

        Returns:
            bool: [description]

        """
        try:
            float(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_none(x):
        """[summary]

        Args:
            x (): [description]

        Returns:

        """
        return x is None

    def transform(self, session: str, files: list) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, dict, pd.DataFrame,
                                                       dict, pd.DataFrame):
        """[summary]

        Args:
            session (str): [description]
            files (list): [description]

        Returns:
            pd.DataFrame: [df_track_players]
            pd.DataFrame: [df_player_names_raw]
            pd.DataFrame: [players_df_lineup]
            dict: [match_info]
            pd.DataFrame: [df_opta_events]
            dict: [opta_match_info]
            pd.DataFrame: [df_time_possession]

        """
        if len(files) < 1:
            self.logger.error('There are %d files in the Spectrum session directory %s. There must be exactly one '
                              'or more files: .jsonl data file, a .json metadata file and xml. The files in the '
                              'directory are: %s' %
                              (len(files), session, str(files)))
            return None, None

        df_track_players, df_player_names_raw, players_df_lineup, match_info, \
        df_opta_events, opta_match_info, df_time_possession = [None for i in range(7)]

        # Let's make sure the file that have been uploaded match the format we expect.
        data_file_dat = [file for file in files if '.dat' in file.lower()]
        metadata_xml = [file for file in files if 'metadata.xml' in file.lower()]
        data_file = [file for file in files if 'data.jsonl' in file.lower()]
        meta_file_json = [file for file in files if 'metadata.json' in file.lower()]
        event_details = [file for file in files if ('eventdetails.xml' in file.lower()) or ('f24.xml' in file.lower())]
        match_results = [file for file in files if ('matchresults.xml' in file.lower()) or ('f7.xml' in file.lower())]

        if len(data_file_dat) == 0:
            self.logger.warning(
                'There are no .dat data files in the Spectrum session directory %s. There are %d file(s) '
                'in the session directory: %s..' % (session, len(files), str(files)))
        elif len(data_file) > 1:
            self.logger.warning('There is more than one .dat data file in the Spectrum session directory %s. %d .dat '
                                'files were found: %s.' % (session, len(data_file), str(data_file)))

        elif len(metadata_xml) == 0:
            self.logger.warning(
                'There are no MetaData files in the Spectrum session directory %s. There are %d file(s) '
                'in the session directory: %s.' % (session, len(files), str(files)))
        else:
            self.logger.info('Loading tracking data from {}'.format(data_file_dat[0]))
            df_track_players = self.get_track_player_data(data_file_dat[0], metadata_xml[0])

        if len(data_file) == 0:
            self.logger.warning(
                'There are no .jsonl data files in the Spectrum session directory %s. There are %d file(s) '
                'in the session directory: %s.' % (session, len(files), str(files)))
        elif len(data_file) > 1:
            self.logger.warning(
                'There is more than one .jsonl data file in the Spectrum session directory %s. %d .jsonl '
                'files were found: %s.' % (session, len(data_file), str(data_file)))
        else:
            self.logger.info('Loading tracking data from {}'.format(data_file[0]))
            df_track_players = self.get_track_player_data(data_file[0])

        if len(meta_file_json) == 0 and len(metadata_xml) == 0:
            self.logger.warning(
                'There are no MetaData files in the Spectrum session directory %s. There are %d file(s) '
                'in the session directory: %s.' % (session, len(files), str(files)))
        elif len(meta_file_json) > 1:
            self.logger.warning('There is more than one .json metadata file in the Spectrum session directory %s. %d '
                                'Metadata.json files were found: %s.' %
                                (session, len(meta_file_json), str(meta_file_json)))
        elif len(metadata_xml) > 1:
            self.logger.warning('There is more than one .xml metadata file in the Spectrum session directory %s. %d '
                                'Metadata.xml files were found: %s.' %
                                (session, len(metadata_xml), str(metadata_xml)))

        meta_file = None
        if len(meta_file_json) > 0:
            meta_file = meta_file_json[0]
        elif len(metadata_xml) > 0:
            meta_file = metadata_xml[0]

        if len(match_results) == 0:
            self.logger.warning(
                'There are no .xml matchresults data files in the Spectrum session directory %s. There are %d file(s) '
                'in the session directory: %s.' % (session, len(files), str(files)))
        elif len(match_results) > 1:
            self.logger.warning(
                'There is more than one .xml matchresults file in the Spectrum session directory %s. %d '
                'Metadata.json files were found: %s.' %
                (session, len(match_results), str(match_results)))
        elif meta_file is not None:
            self.logger.info('Loading player and match meta_data from from {}'.format(match_results[0]))
            df_player_names_raw, players_df_lineup, match_info = self.get_player_match_data(match_results[0], meta_file)
        else:
            self.logger.warning('There is more than one metadata file in the Spectrum session directory %s. %d '
                                'files were found: %s.' %
                                (session, len(metadata_xml), str(metadata_xml)))

        if len(event_details) == 0:
            self.logger.warning(
                'There are no .xml eventdetails files in the Spectrum session directory %s. There are %d file(s) '
                'in the session directory: %s.' % (session, len(files), str(files)))
        elif len(event_details) > 1:
            self.logger.warning(
                'There is more than one .xml eventdetails file in the Spectrum session directory %s. %d '
                'Metadata.json files were found: %s.' %
                (session, len(event_details), str(event_details)))
        else:
            self.logger.info('Loading opta event data from {}'.format(event_details[0]))
            df_opta_events, opta_match_info = self.get_opta_events(event_details[0])

        if len(files) > 2:
            valid_files = data_file + meta_file_json + event_details + match_results
            invalid_files = set(files) - set(valid_files)
            self.logger.warning('There were %d extra files in the Spectrum session directory: %s. These file are '
                                'neither .jsonl or .json files: %s. Ignoring extra files.' %
                                (len(invalid_files), session, str(invalid_files)))

        if (df_track_players is not None) and (len(match_results) > 0):
            self.logger.info('Calculation time possession')
            df_time_possession = self.time_possession(df_track_players, match_results[0])

        return df_track_players, df_player_names_raw, players_df_lineup, match_info, df_opta_events, opta_match_info, \
               df_time_possession

    def get_track_player_data(self, track_path: str, track_metadata_path=None) -> pd.DataFrame:
        """[summary]

        Args:
            track_path (str): [description]
            track_metadata_path (str): [description]

        Returns:
            pd.DataFrame: [df]

        """

        if track_path.endswith('.dat'):

            x = []
            with open(track_path) as fp:
                for cnt, line in enumerate(fp):
                    x.append(line)

            # x[0].split(':')[0] #gives frame id
            # x[0].split(':')[1].split(';')[:(len(x[0].split(':')[1].split(';'))-1)] #gives all players tracking info
            # x[0].split(':')[2].split(';')[0] #gives ball tracking info

            # let's try to extract the player data sensibly from the array and turn them into a long dataframe
            all_players_and_frames = []

            for k in range(len(x)):
                all_players = []
                for i in range(len(x[k].split(':')[1].split(';')[:(len(x[k].split(':')[1].split(';')) - 1)])):
                    player_values = [float(item) for item in
                                     x[k].split(':')[1].split(';')[:(len(x[k].split(':')[1].split(';')) - 1)][i].split(
                                         ',')]
                    player_values.append(int(x[k].split(':')[0]))
                    player_values = [player_values[j] for j in [6, 0, 1, 2, 3, 4, 5]]
                    all_players.append(player_values)
                    del (player_values)
                all_players_and_frames.append(all_players)
                del (all_players)

            # we first need to flatten the array
            all_players_and_frames_flattened = [item for sublist in all_players_and_frames for item in sublist]
            track_players_df = pd.DataFrame(all_players_and_frames_flattened,
                                            columns=['Frame_ID', 'Team_ID', 'System_Target_ID', 'Jersey_Number',
                                                     'x_Player', 'y_Player', 'Speed_Player'])

            # let's try to use the same approach to get ball tracking data in a nicer format
            all_balls_and_frames = []

            for k in range(len(x)):
                all_balls_and_frames.append(x[k].split(':')[2].split(';')[0].split(','))

            track_ball_df = pd.DataFrame(all_balls_and_frames)
            if track_ball_df.shape[1] > 7:
                track_ball_df = track_ball_df.iloc[:, :7].reset_index(drop=True)
            track_ball_df.columns = ['x_Ball', 'y_Ball', 'z_Ball', 'Speed_Ball', 'Last_Touched', 'Is_Ball_Live',
                                     'Ball_contact_dev_info']

            for col in ['x_Ball', 'y_Ball', 'z_Ball']:
                track_ball_df[col] = 0.01 * track_ball_df[col].astype(float)
            for col in ['x_Player', 'y_Player']:
                track_players_df[col] = 0.01 * track_players_df[col].astype(float)

            track_ball_df['Speed_Ball'] = 0.01 * track_ball_df['Speed_Ball'].astype(float)

            # we now add the frame_count column
            track_ball_df['Frame_ID'] = [int(x[k].split(':')[0]) for k in range(len(all_balls_and_frames))]
            # track_ball_df['Is_Ball_Live'] = np.where(track_ball_df['Ball_Status']=='Alive', 'true', 'false')
            # track_ball_df['Last_Touched'] = track_ball_df['Team_Possession']
            # track_ball_df['Is_Home_Away'] = np.where()

            # we now join the data
            all_tracking_data = track_players_df.merge(track_ball_df, how='inner', left_on=['Frame_ID'],
                                                       right_on=['Frame_ID'])

            # we need start and end frames for first and second half

            with open(track_metadata_path, encoding='utf-8') as fd:
                tracab_metadata = xmltodict.parse(fd.read())

            frame_starts = []
            frame_ends = []

            for i in range(len(tracab_metadata['TracabMetaData']['match']['period'])):
                frame_starts.append(int(tracab_metadata['TracabMetaData']['match']['period'][i]['@iStartFrame']))
                frame_ends.append(int(tracab_metadata['TracabMetaData']['match']['period'][i]['@iEndFrame']))

            all_tracking_data_splits = []
            max_frame_period = 0
            for i in range(len(frame_starts)):
                data_split = all_tracking_data[
                    (all_tracking_data.Frame_ID >= frame_starts[i]) & (all_tracking_data.Frame_ID <= frame_ends[i]) & (
                        ~all_tracking_data.Team_ID.isin([-1, 4]))].sort_values(['Frame_ID']).reset_index(drop=True)
                data_split['Period_ID'] = i + 1
                reps_per_frame = data_split[['Frame_ID', 'Team_ID']].groupby(
                    ['Frame_ID']).count().reset_index().sort_values(['Frame_ID']).reset_index(drop=True)
                data_split['Time'] = np.round(
                    np.repeat(np.array(range(len(data_split.Frame_ID.unique()))) / 25, reps_per_frame.Team_ID.tolist()),
                    2)
                data_split['Frame_ID'] = np.round(data_split['Time'] * 25) + max_frame_period
                all_tracking_data_splits.append(data_split)
                max_frame_period = data_split['Frame_ID'].max()

            df = pd.concat(all_tracking_data_splits)
            df['Is_Ball_Live'] = np.where(df['Is_Ball_Live'] == 'Alive', 'true', 'false')
            df['Last_Touched'] = np.where(df['Last_Touched'] == 'A', 'away', 'home')
            df['Is_Home_Away'] = np.where(df['Team_ID'] == 0, 'away', 'home')
            df['z_Player'] = 0
            # all_tracking_data_final.drop_duplicates(['Frame_Count']).Team_Possession.value_counts()

        else:  # json file

            track_file = open(track_path)

            raw_data = track_file.read()

            track_file.close()

            result = [json.loads(jline) for jline in str(raw_data).split('\n')]

            ball_df = pd.DataFrame(result)[
                ['ball', 'frameIdx', 'period', 'gameClock', 'wallClock', 'lastTouch', 'live']]
            ball_df['is_ball_none'] = list(map(self.is_none, ball_df['ball']))
            ball_df['ball'] = np.where(ball_df['is_ball_none'], {'xyz': [None, None, None], 'speed': None},
                                       ball_df['ball'])
            ball_df = pd.concat([ball_df, pd.json_normalize(ball_df['ball'])], axis=1, sort=False)
            ball_df = pd.concat([ball_df, pd.DataFrame(ball_df.xyz.values.tolist(), index=ball_df.index,
                                                       columns=['ball_x', 'ball_y', 'ball_z'])], axis=1, sort=False)
            ball_df['ball_speed'] = ball_df['speed']
            ball_df = ball_df.drop(columns=['speed', 'xyz', 'ball', 'is_ball_none'])

            homePlayers_df = pd.json_normalize(result, 'homePlayers',
                                               ['frameIdx', 'period', 'gameClock', 'wallClock', 'lastTouch', 'live'])
            homePlayers_df = pd.concat([homePlayers_df,
                                        pd.DataFrame(homePlayers_df.xyz.values.tolist(), index=homePlayers_df.index,
                                                     columns=['x', 'y', 'z'])], axis=1, sort=False)
            homePlayers_df['is_home'] = True

            awayPlayers_df = pd.json_normalize(result, 'awayPlayers',
                                               ['frameIdx', 'period', 'gameClock', 'wallClock', 'lastTouch', 'live'])
            awayPlayers_df = pd.concat([awayPlayers_df,
                                        pd.DataFrame(awayPlayers_df.xyz.values.tolist(), index=awayPlayers_df.index,
                                                     columns=['x', 'y', 'z'])], axis=1, sort=False)
            awayPlayers_df['is_home'] = False

            players_df = homePlayers_df.append(awayPlayers_df, sort=False)
            players_df = players_df.drop(columns=['xyz'])

            df = pd.merge(players_df, ball_df, on=['frameIdx', 'period', 'gameClock', 'wallClock', 'lastTouch', 'live'])

            # Dataframe Clean
            # df = df.rename(columns={"playerId": "source_player_id", "number": "jersey", "wallClock": "session_elapsed_time"})

            # df['session_elapsed_time'] = pd.to_datetime(df.session_elapsed_time, unit='ms')

            df['live'] = np.where(df['live'], 'true', 'false')
            df['is_home'] = np.where(df['is_home'], 'home', 'away')
            # df.loc[(df.live == 0), 'ball_status'] = 'Dead'
            # df.loc[(df.live == 1), 'ball_status'] = 'Alive'
            # df.loc[(df.lastTouch == 'home'), 'ball_possession'] = 'H'
            # df.loc[(df.lastTouch == 'away'), 'ball_possession'] = 'A'

            # Dataframe Clean
            df = df.drop(columns=['optaUuid', 'ssiID', 'name', 'wallClock', 'number'], errors='ignore')
            df.columns = ['Player_ID', 'Speed_Player', 'Frame_ID', 'Period_ID', 'Time', 'Last_Touched', 'Is_Ball_Live',
                          'x_Player', 'y_Player', 'z_Player', 'Is_Home_Away', 'x_Ball', 'y_Ball', 'z_Ball',
                          'Speed_Ball']
            df = df[['Period_ID', 'Frame_ID', 'Time', 'Player_ID', 'Is_Home_Away', 'x_Player', 'y_Player', 'z_Player',
                     'Speed_Player', 'x_Ball', 'y_Ball', 'z_Ball', 'Speed_Ball', 'Is_Ball_Live', 'Last_Touched']]
            df['Period_ID'] = df['Period_ID'].astype(int)
            df['Frame_ID'] = df['Frame_ID'].astype(int)
            df['Time'] = df['Time'].astype(float)
            df['Player_ID'] = df['Player_ID'].astype(int)

        return df

    @staticmethod
    def get_opta_events(event_path: str) -> (pd.DataFrame, dict):
        """[summary]

        Args:
            event_path (str): [description]

        Returns:
            pd.DataFrame: [opta_event_data_df]
            dict: [opta_match_info]

        """

        try:
            with open(event_path, encoding='utf-8') as fd:
                opta_event = xmltodict.parse(fd.read())
        except UnicodeDecodeError:
            with open(event_path, encoding='latin-1') as fd:
                opta_event = xmltodict.parse(fd.read())

        # start rearranging the file in a nicer format
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
        away_score = '0'

        if '@away_score' in list(opta_event['Games']['Game'].keys()):
            away_score = int(opta_event['Games']['Game']['@away_score'])

        away_team_id = int(opta_event['Games']['Game']['@away_team_id'])
        away_team_name = opta_event['Games']['Game']['@away_team_name']
        home_score = '0-0'

        if '@home_score' in list(opta_event['Games']['Game'].keys()):
            home_score = int(opta_event['Games']['Game']['@home_score'])

        home_team_id = int(opta_event['Games']['Game']['@home_team_id'])
        home_team_name = opta_event['Games']['Game']['@home_team_name']

        fixture = home_team_name + ' v ' + away_team_name
        result = '0'
        if (home_score is not None) & (away_score is not None):
            result = str(home_score) + ' - ' + str(away_score)

        # loop over events in the opta file
        for i in range(len(opta_event['Games']['Game']['Event'])):
            unique_event_id = int(opta_event['Games']['Game']['Event'][i]['@id'])
            event_id = int(opta_event['Games']['Game']['Event'][i]['@event_id'])
            type_id = int(opta_event['Games']['Game']['Event'][i]['@type_id'])
            period_id = int(opta_event['Games']['Game']['Event'][i]['@period_id'])
            mins = int(opta_event['Games']['Game']['Event'][i]['@min'])
            sec = int(opta_event['Games']['Game']['Event'][i]['@sec'])
            team_id = int(opta_event['Games']['Game']['Event'][i]['@team_id'])
            outcome = int(opta_event['Games']['Game']['Event'][i]['@outcome'])
            # we need to check conditions for existence of player_id, keypass and assist
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

            # inner loop over qualifiers - if there is only one qualifier then we go straight with the dict like approach
            # we need first to check whether there are any qualifiers
            if len(set(list(opta_event['Games']['Game']['Event'][i].keys())).intersection(set(['Q']))) == 0:
                qualifier_id = None
                value = None
                opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day,
                                        game_date, period_1_start, period_2_start, home_score, home_team_id,
                                        home_team_name,
                                        away_score, away_team_id, away_team_name, fixture, result,
                                        unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id,
                                        team_id, outcome, keypass, assist,
                                        x, y, qualifier_id, value])
            else:
                if type(opta_event['Games']['Game']['Event'][i]['Q']) != list:
                    qualifier_id = int(opta_event['Games']['Game']['Event'][i]['Q']['@qualifier_id'])
                    if len(set(list(opta_event['Games']['Game']['Event'][i]['Q'].keys())).intersection(
                            set(['@value']))) == 1:
                        value = opta_event['Games']['Game']['Event'][i]['Q']['@value']
                    else:
                        value = None

                    opta_event_data.append(
                        [competition_id, competition_name, season_id, season_name, game_id, match_day,
                         game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name,
                         away_score, away_team_id, away_team_name, fixture, result,
                         unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id,
                         outcome, keypass, assist,
                         x, y, qualifier_id, value])
                else:
                    for j in range(len(opta_event['Games']['Game']['Event'][i]['Q'])):
                        qualifier_id = int(opta_event['Games']['Game']['Event'][i]['Q'][j]['@qualifier_id'])
                        # check whether '@value' is present or not
                        if len(set(list(opta_event['Games']['Game']['Event'][i]['Q'][j].keys())).intersection(
                                set(['@value']))) == 1:  # value present
                            value = opta_event['Games']['Game']['Event'][i]['Q'][j]['@value']
                        else:
                            value = None

                        # append everything
                        opta_event_data.append(
                            [competition_id, competition_name, season_id, season_name, game_id, match_day,
                             game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name,
                             away_score, away_team_id, away_team_name, fixture, result,
                             unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id,
                             outcome, keypass, assist,
                             x, y, qualifier_id, value])

        # convert the list to a pandas dataframe
        opta_event_data_df = pd.DataFrame(opta_event_data, index=None,
                                          columns=['competition_id', 'competition_name', 'season_id', 'season_name',
                                                   'game_id', 'match_day',
                                                   'game_date', 'period_1_start', 'period_2_start', 'home_score',
                                                   'home_team_id', 'home_team_name',
                                                   'away_score', 'away_team_id', 'away_team_name', 'fixture', 'result',
                                                   'unique_event_id', 'event_id', 'period_id', 'min', 'sec',
                                                   'timestamp', 'type_id', 'player_id', 'team_id', 'outcome', 'keypass',
                                                   'assist',
                                                   'x', 'y', 'qualifier_id', 'value'])

        opta_match_info = {}
        opta_match_info['match_id'] = game_id
        opta_match_info['match_date'] = game_date
        opta_match_info['away_score'] = away_score
        opta_match_info['away_team_id'] = away_team_id
        opta_match_info['away_team_name'] = away_team_name
        opta_match_info['home_score'] = home_score
        opta_match_info['home_team_id'] = home_team_id
        opta_match_info['home_team_name'] = home_team_name
        opta_match_info['fixture'] = fixture

        return opta_event_data_df, opta_match_info

    @staticmethod
    def get_player_match_data(match_results_xml, path_track_meta) -> (pd.DataFrame, pd.DataFrame, dict):
        """[summary]

        Args:
            match_results_xml (str): [description]
            path_track_meta (str): [description]

        Returns:
            pd.DataFrame: [df_player_names_raw]
            pd.DataFrame: [players_df_lineup]
            dict: [match_info]

        """

        try:
            with open(match_results_xml, encoding='utf-8') as fd:  # encoding essential for special characters in names
                opta_results = xmltodict.parse(fd.read())
        except UnicodeDecodeError:
            with open(match_results_xml, encoding='latin-1') as fd:
                opta_results = xmltodict.parse(fd.read())

        if type(opta_results['SoccerFeed']['SoccerDocument']) != list:
            game_id = int(opta_results['SoccerFeed']['SoccerDocument']['@uID'].strip('f'))
            competition_id = int(opta_results['SoccerFeed']['SoccerDocument']['Competition']['@uID'].strip('c'))
            competition_name = opta_results['SoccerFeed']['SoccerDocument']['Competition']['Name']
            referee_id = int(
                opta_results['SoccerFeed']['SoccerDocument']['MatchData']['MatchOfficial']['@uID'].strip('o'))
            referee_name = opta_results['SoccerFeed']['SoccerDocument']['MatchData']['MatchOfficial']['OfficialName'][
                               'First'] + ' ' + \
                           opta_results['SoccerFeed']['SoccerDocument']['MatchData']['MatchOfficial']['OfficialName'][
                               'Last']
            venue = opta_results['SoccerFeed']['SoccerDocument']['Venue']['Name']
            team_data = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'])
            players_df_home = pd.DataFrame(team_data.PlayerLineUp.iloc[0]['MatchPlayer'])
            players_df_home['team_id'] = int(
                opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0]['@TeamRef'].strip('t'))
            players_df_away = pd.DataFrame(team_data.PlayerLineUp.iloc[1]['MatchPlayer'])
            players_df_away['team_id'] = int(
                opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1]['@TeamRef'].strip('t'))

            if '@Formation_Place' not in list(players_df_home):
                players_df_home = players_df_home[
                    ['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition',
                     'team_id']]
            else:
                players_df_home = players_df_home[
                    ['@Captain', '@PlayerRef', '@Formation_Place', '@Position', '@ShirtNumber', '@Status',
                     '@SubPosition',
                     'team_id']]

            if '@Formation_Place' not in list(players_df_away):
                players_df_away = players_df_away[
                    ['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition',
                     'team_id']]
            else:
                players_df_away = players_df_away[
                    ['@Captain', '@PlayerRef', '@Formation_Place', '@Position', '@ShirtNumber', '@Status',
                     '@SubPosition',
                     'team_id']]

            players_df_lineup = players_df_home.append(players_df_away, sort=False)

            if '@Formation' not in list(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0].keys()):
                home_formation = None
            else:
                home_formation = int(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0]['@Formation'])
            if '@Formation' not in list(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1].keys()):
                away_formation = None
            else:
                away_formation = int(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1]['@Formation'])

            player_names_home_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument']['Team'][0]['Player'])
            player_names_away_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument']['Team'][1]['Player'])
            player_names_home_raw = player_names_home_raw[['@Position', '@uID', 'PersonName']]
            player_names_home_raw['is_home'] = True
            player_names_away_raw = player_names_away_raw[['@Position', '@uID', 'PersonName']]
            player_names_away_raw['is_home'] = False
            player_names_raw = player_names_home_raw.append(player_names_away_raw, sort=False)

            player_names_raw['first_name'] = [player_names_raw['PersonName'].iloc[i]['First'] for i in
                                              range(player_names_raw.shape[0])]
            player_names_raw['last_name'] = [player_names_raw['PersonName'].iloc[i]['Last'] for i in
                                             range(player_names_raw.shape[0])]

            player_names_raw['full_name'] = [(player_names_raw['PersonName'].iloc[i]['First'] + ' ' +
                                              player_names_raw.PersonName.iloc[i]['Last']) if len(
                set(list(player_names_raw.PersonName.iloc[i].keys())).intersection(set(['Known']))) == 0 else
                                             player_names_raw.PersonName.iloc[i]['Known'] for i in
                                             range(player_names_raw.shape[0])]

        else:  # take only first coming up
            game_id = int(opta_results['SoccerFeed']['SoccerDocument'][0]['@uID'].strip('f'))
            competition_id = int(opta_results['SoccerFeed']['SoccerDocument'][0]['Competition']['@uID'].strip('c'))
            competition_name = opta_results['SoccerFeed']['SoccerDocument'][0]['Competition']['Name']
            referee_id = int(
                opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['MatchOfficial']['@uID'].strip('o'))
            referee_name = \
                opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['MatchOfficial']['OfficialName'][
                    'First'] + ' ' + \
                opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['MatchOfficial']['OfficialName']['Last']
            venue = opta_results['SoccerFeed']['SoccerDocument'][0]['Venue']['Name']
            team_data = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'])
            players_df_home = pd.DataFrame(team_data.PlayerLineUp.iloc[0]['MatchPlayer'])
            players_df_home['team_id'] = int(
                opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][0]['@TeamRef'].strip('t'))
            players_df_away = pd.DataFrame(team_data.PlayerLineUp.iloc[1]['MatchPlayer'])
            players_df_away['team_id'] = int(
                opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][1]['@TeamRef'].strip('t'))

            if '@Formation_Place' not in list(players_df_home):
                players_df_home = players_df_home[
                    ['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition',
                     'team_id']]
            else:
                players_df_home = players_df_home[
                    ['@Captain', '@PlayerRef', '@Formation_Place', '@Position', '@ShirtNumber', '@Status',
                     '@SubPosition',
                     'team_id']]

            if '@Formation_Place' not in list(players_df_away):
                players_df_away = players_df_away[
                    ['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition',
                     'team_id']]
            else:
                players_df_away = players_df_away[
                    ['@Captain', '@PlayerRef', '@Formation_Place', '@Position', '@ShirtNumber', '@Status',
                     '@SubPosition',
                     'team_id']]

            players_df_lineup = pd.concat([players_df_home, players_df_away])

            if '@Formation' not in list(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0].keys()):
                home_formation = None
            else:
                home_formation = int(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0]['@Formation'])
            if '@Formation' not in list(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1].keys()):
                away_formation = None
            else:
                away_formation = int(
                    opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1]['@Formation'])

            player_names_home_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument'][0]['Team'][0]['Player'])
            player_names_away_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument'][0]['Team'][1]['Player'])
            player_names_home_raw = player_names_home_raw[['@Position', '@uID', 'PersonName']]
            player_names_home_raw['is_home'] = True
            player_names_away_raw = player_names_away_raw[['@Position', '@uID', 'PersonName']]
            player_names_away_raw['is_home'] = False
            player_names_raw = player_names_home_raw.append(player_names_away_raw, sort=False)

            player_names_raw['first_name'] = [player_names_raw['PersonName'].iloc[i]['First'] for i in
                                              range(player_names_raw.shape[0])]
            player_names_raw['last_name'] = [player_names_raw['PersonName'].iloc[i]['Last'] for i in
                                             range(player_names_raw.shape[0])]

            player_names_raw['full_name'] = [(player_names_raw['PersonName'].iloc[i]['First'] + ' ' +
                                              player_names_raw.PersonName.iloc[i]['Last']) if len(
                set(list(player_names_raw.PersonName.iloc[i].keys())).intersection(set(['Known']))) == 0 else
                                             player_names_raw.PersonName.iloc[i]['Known'] for i in
                                             range(player_names_raw.shape[0])]

        players_df_lineup = players_df_lineup.rename(columns={"@PlayerRef": "player_id", "@ShirtNumber": "jersey",
                                                              "@Position": "start_position", "@Status": "status",
                                                              "@Formation_Place": "formation_place",
                                                              "@SubPosition": "sub_position",
                                                              "@Captain": "captain"})
        players_df_lineup['player_id'] = [int(x.replace('p', '')) for x in players_df_lineup['player_id']]
        players_df_lineup['position_in_pitch'] = np.where(players_df_lineup['sub_position'].notnull(),
                                                          players_df_lineup['sub_position'],
                                                          players_df_lineup['start_position'])

        df_player_names_raw = player_names_raw.drop(columns=['PersonName'])
        df_player_names_raw = df_player_names_raw.rename(columns={"@Position": "position", "@uID": "player_id"})
        df_player_names_raw['player_id'] = [int(x.replace('p', '')) for x in df_player_names_raw['player_id']]
        # df_player_info = pd.merge(players_df_lineup, player_names_raw, on=['player_id'])

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

        match_info = {}
        # match_info['description'] = meta_data['description']
        # match_info['home_team_name'] = meta_data['description'].split(':')[0].split(' - ')[0]
        # match_info['away_team_name'] = meta_data['description'].split(':')[0].split(' - ')[1]
        # match_info['home_opta_id'] = meta_data['homeOptaId']
        # match_info['away_opta_id'] = meta_data['awayOptaId']
        # match_info['match_start_datetime'] = meta_data['startTime']
        # match_info['match_fps'] = meta_data['fps']
        # match_info['match_opta_id'] = meta_data['optaId']
        match_info['pitchLength'] = length_pitch
        match_info['pitchWidth'] = width_pitch
        match_info['referee_id'] = referee_id
        match_info['referee_name'] = referee_name
        match_info['venue'] = venue
        match_info['home_formation'] = home_formation
        match_info['away_formation'] = away_formation

        return df_player_names_raw, players_df_lineup, match_info

    @staticmethod
    def time_possession(track_players_df: pd.DataFrame, file_results: str) -> pd.DataFrame:
        """[summary]

        Args:
            track_players_df (pd.DataFrame): [description]
            file_results (str): [description]

        Returns:
            pd.DataFrame: [final_data_pivot]

        """

        if 'System_Target_ID' not in list(track_players_df.columns):  # means we have output from second spectrum file
            # time played by players
            time_played_by_players = pd.DataFrame(
                0.04 * track_players_df.groupby(['Is_Home_Away', 'Player_ID']).Frame_ID.count()).reset_index()
            # time in-out possession for players
            time_in_out_possession = pd.DataFrame(
                0.04 * track_players_df[track_players_df.Is_Ball_Live == 'true'].groupby(
                    ['Is_Home_Away', 'Player_ID', 'Last_Touched']).Frame_ID.count()).reset_index()
            time_played_by_players.rename(columns={'Frame_ID': 'time played'}, inplace=True)
            time_in_out_possession['time in/out possession for the team'] = np.where(
                time_in_out_possession.Last_Touched == time_in_out_possession.Is_Home_Away,
                'time in possession for the team', 'time out possession for the team').tolist()
            time_in_out_possession.rename(columns={'Is_Home_Away': 'home/away',
                                                   'Player_ID': 'player id', 'Frame_ID': 'time possession'},
                                          inplace=True)

            final_data = time_played_by_players.merge(time_in_out_possession, how='inner',
                                                      left_on=['Is_Home_Away', 'Player_ID'],
                                                      right_on=['home/away', 'player id'])[
                ['Player_ID', 'time played', 'time possession', 'time in/out possession for the team']]
            columns_final_data = list(final_data)
            column_pivot = 'time in/out possession for the team'
            value_pivot = 'time possession'

            # now, pivot the data based on the 'in-out' column
            final_data_pivot = final_data.fillna(-999).pivot_table(
                index=[x for x in columns_final_data if x not in [column_pivot, value_pivot]],
                columns=column_pivot, values=value_pivot, dropna=True).reset_index().replace(-999, np.nan)


        else:  # we are with the output from tracab
            track_players_df['Team_ID'] = np.where(track_players_df['Team_ID'] == 0, 'A', 'H')
            # time played by players
            time_played_by_players = pd.DataFrame(
                0.04 * track_players_df.groupby(['Team_ID', 'Jersey_Number']).Frame_Count.count()).reset_index()
            # time in-out possession for players
            # if alive_only:
            time_in_out_possession = pd.DataFrame(
                0.04 * track_players_df[track_players_df.Ball_Status == 'Alive'].groupby(
                    ['Team_ID', 'Jersey_Number', 'Team_Possession']).Frame_Count.count()).reset_index()
            time_played_by_players.rename(columns={'Frame_Count': 'time played'}, inplace=True)

            time_in_out_possession['time in/out possession for the team'] = np.where(
                time_in_out_possession.Team_Possession == time_in_out_possession.Team_ID,
                'time in possession for the team', 'time out possession for the team').tolist()
            time_in_out_possession.rename(columns={'Team_ID': 'home/away',
                                                   'Frame_Count': 'time possession'}, inplace=True)

            final_data = time_played_by_players.merge(time_in_out_possession, how='inner',
                                                      left_on=['Team_ID', 'Jersey_Number'],
                                                      right_on=['home/away', 'Jersey_Number'])[
                ['Team_ID', 'Jersey_Number', 'time played', 'time possession', 'time in/out possession for the team']]
            columns_final_data = list(final_data)
            column_pivot = 'time in/out possession for the team'
            value_pivot = 'time possession'

            # now, pivot the data based on the 'in-out' column
            final_data_pivot = final_data.fillna(-999).pivot_table(
                index=[x for x in columns_final_data if x not in [column_pivot, value_pivot]],
                columns=column_pivot, values=value_pivot, dropna=True).reset_index().replace(-999, 0)

            # file_results = file_results
            with open(file_results, encoding='utf-8') as fd:
                match_results = xmltodict.parse(fd.read())

            player_shirts = []
            for i in range(2):
                # team_id = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['@TeamRef']
                # team_name = np.where(team_id == 't' + str(int(home_team_id)), home_team_name, away_team_name).tolist()
                if type(match_results['SoccerFeed']['SoccerDocument']) == list:
                    for k in range(len(match_results['SoccerFeed']['SoccerDocument'])):
                        if match_results['SoccerFeed']['SoccerDocument'][k]['@uID'] == 'f' + str(
                                track_players_df.game_id.iloc[0]):
                            break

                    home_away = match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['@Side'][
                        0].upper()
                    for j in range(len(match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i][
                                           'PlayerLineUp']['MatchPlayer'])):
                        player_id = \
                        match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp'][
                            'MatchPlayer'][j]['@PlayerRef']
                        shirt_number = int(match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i][
                                               'PlayerLineUp']['MatchPlayer'][j]['@ShirtNumber'])
                        player_shirts.append([player_id, shirt_number, home_away])

                else:
                    home_away = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['@Side'][
                        0].upper()
                    for j in range(len(
                            match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp'][
                                'MatchPlayer'])):
                        player_id = \
                        match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp'][
                            'MatchPlayer'][j]['@PlayerRef']
                        shirt_number = int(
                            match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp'][
                                'MatchPlayer'][j]['@ShirtNumber'])
                        player_shirts.append([player_id, shirt_number, home_away])

            player_shirts = pd.DataFrame(player_shirts, columns=['Player_ID', 'Jersey_Number', 'home_away'])

            final_data_pivot = final_data_pivot.merge(player_shirts, how='inner',
                                                      left_on=['Team_ID', 'Jersey_Number'],
                                                      right_on=['home_away', 'Jersey_Number']).drop(
                ['Team_ID', 'home_away',
                 'Jersey_Number'], axis=1)[['Player_ID', 'time played', 'time in possession for the team',
                                            'time out possession for the team']].reset_index(drop=True)
            final_data_pivot['Player_ID'] = list(map(lambda x: int(x.replace('p', '')), final_data_pivot['Player_ID']))

        final_data_pivot = final_data_pivot.fillna(0)  # to fill any na in time in possession or out possession

        return final_data_pivot
