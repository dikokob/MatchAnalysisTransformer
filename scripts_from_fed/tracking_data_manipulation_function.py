#script that contains the function to manipulate and rearrange json tracking data


#track_path = 'Y:\\API Data Files\\2019-20\\Premier League\\Round 2 All Data\\g1059712\\g1059712_SecondSpectrum_Data.jsonl'
#track_metadata_path = 'Y:\\API Data Files\\2019-20\\Premier League\\Round 2 All Data\\g1059712\\g1059712_SecondSpectrum_Metadata.json'

#track_path = 'Y:\\API Data Files\\2017-18\\Premier League\\Round 1 All Data\\g918894\\918894.dat'
#file_results = 'Y:\\API Data Files\\2017-18\\Premier League\\Round 1 All Data\\g918894\\srml-8-2017-f918894-matchresults.xml'
#track_metadata_path = 'Y:\\API Data Files\\2017-18\\Premier League\\Round 1 All Data\\g918894\\918894_metadata.xml'

def tracking_data_manipulation_new (track_path, track_metadata_path):
    import json
    import xmltodict
    import pandas as pd 

    def track_player_data(track_path):
        #global track_metadata_path
        import numpy as np
        import xmltodict

        if track_path.endswith('.dat'):

            x = []
            with open(track_path) as fp:  
                for cnt, line in enumerate(fp):
                    x.append(line)

            #x[0].split(':')[0] #gives frame id
            #x[0].split(':')[1].split(';')[:(len(x[0].split(':')[1].split(';'))-1)] #gives all players tracking info
            #x[0].split(':')[2].split(';')[0] #gives ball tracking info

            #let's try to extract the player data sensibly from the array and turn them into a long dataframe
            all_players_and_frames = []

            for k in range(len(x)): 
                all_players = []
                for i in range(len(x[k].split(':')[1].split(';')[:(len(x[k].split(':')[1].split(';'))-1)])):
                    player_values = [float(item) for item in x[k].split(':')[1].split(';')[:(len(x[k].split(':')[1].split(';'))-1)][i].split(',')]
                    player_values.append(int(x[k].split(':')[0])) 
                    player_values = [player_values[j] for j in [6,0,1,2,3,4,5]]
                    all_players.append(player_values)
                    del(player_values)
                all_players_and_frames.append(all_players)
                del(all_players)

            #we first need to flatten the array
            all_players_and_frames_flattened = [item for sublist in all_players_and_frames for item in sublist]
            track_players_df = pd.DataFrame(all_players_and_frames_flattened, columns = ['Frame_Count', 'Team_ID', 'System_Target_ID', 'Jersey_Number', 'x_Player', 'y_Player', 'Speed_Player'])
    

            #let's try to use the same approach to get ball tracking data in a nicer format
            all_balls_and_frames = []
 
            for k in range(len(x)):
                all_balls_and_frames.append(x[k].split(':')[2].split(';')[0].split(','))

            track_ball_df = pd.DataFrame(all_balls_and_frames)
            if track_ball_df.shape[1] > 7:
                track_ball_df = track_ball_df.iloc[:,:7].reset_index(drop=True)
            track_ball_df.columns = ['X_coord_ball', 'Y_coord_ball', 'Z_coord_ball', 'Speed_ball', 'Team_Possession', 'Ball_Status', 'Ball_contact_dev_info']

            for col in ['X_coord_ball', 'Y_coord_ball', 'Z_coord_ball', 'Speed_ball']: 
                track_ball_df[col] = track_ball_df[col].astype(float)

            #we now add the frame_count column
            track_ball_df['Frame_Count'] = [int(x[k].split(':')[0]) for k in range(len(all_balls_and_frames))]
            #track_ball_df['Is_Ball_Live'] = np.where(track_ball_df['Ball_Status']=='Alive', 'true', 'false')
            #track_ball_df['Last_Touched'] = track_ball_df['Team_Possession']
            #track_ball_df['Is_Home_Away'] = np.where()


            #we now join the data
            all_tracking_data = track_players_df.merge(track_ball_df, how = 'inner', left_on = ['Frame_Count'], right_on = ['Frame_Count'])

            #we need start and end frames for first and second half

            with open(track_metadata_path, encoding = 'utf-8') as fd:
                tracab_metadata = xmltodict.parse(fd.read())

            frame_starts = []
            frame_ends = []

            for i in range(len(tracab_metadata['TracabMetaData']['match']['period'])):
                frame_starts.append(int(tracab_metadata['TracabMetaData']['match']['period'][i]['@iStartFrame']))
                frame_ends.append(int(tracab_metadata['TracabMetaData']['match']['period'][i]['@iEndFrame']))


            all_tracking_data_splits = []
            for i in range(len(frame_starts)):
                all_tracking_data_splits.append(all_tracking_data[(all_tracking_data.Frame_Count >= frame_starts[i]) & (all_tracking_data.Frame_Count <= frame_ends[i]) & (~all_tracking_data.Team_ID.isin([-1,4]))].reset_index(drop = True))

            df = pd.concat(all_tracking_data_splits)
            #all_tracking_data_final.drop_duplicates(['Frame_Count']).Team_Possession.value_counts()

        else: #json file

            track_file = open(track_path)

            raw_data = track_file.read()

            track_file.close()

            result = [json.loads(jline) for jline in str(raw_data).split('\n')]

            def is_none (x):
                return (x is None)

            ball_df = pd.DataFrame(result)[['ball', 'frameIdx', 'period','gameClock','wallClock', 'lastTouch', 'live']]
            ball_df['is_ball_none'] = list(map(is_none, ball_df['ball']))
            ball_df['ball'] = np.where(ball_df['is_ball_none'], {'xyz': [None, None, None], 'speed': None}, ball_df['ball'])
            ball_df = pd.concat([ball_df, pd.io.json.json_normalize(ball_df['ball'])], axis=1, sort=False)
            ball_df = pd.concat([ball_df, pd.DataFrame(ball_df.xyz.values.tolist(), index= ball_df.index, columns=['ball_x', 'ball_y', 'ball_z'])], axis=1, sort=False)
            ball_df['ball_speed'] = ball_df['speed']
            ball_df = ball_df.drop(columns=['speed', 'xyz', 'ball', 'is_ball_none'])

            homePlayers_df = pd.io.json.json_normalize(result, 'homePlayers', ['frameIdx', 'period','gameClock','wallClock', 'lastTouch', 'live'])
            homePlayers_df = pd.concat([homePlayers_df, pd.DataFrame(homePlayers_df.xyz.values.tolist(), index= homePlayers_df.index, columns=['x', 'y', 'z'])], axis=1, sort=False)
            homePlayers_df['is_home'] = True

            awayPlayers_df = pd.io.json.json_normalize(result, 'awayPlayers', ['frameIdx', 'period','gameClock','wallClock', 'lastTouch', 'live'])
            awayPlayers_df = pd.concat([awayPlayers_df, pd.DataFrame(awayPlayers_df.xyz.values.tolist(), index= awayPlayers_df.index, columns=['x', 'y', 'z'])], axis=1, sort=False)
            awayPlayers_df['is_home'] = False

            players_df = homePlayers_df.append(awayPlayers_df, sort=False)
            players_df = players_df.drop(columns=['xyz'])

            df = pd.merge(players_df, ball_df, on=['frameIdx', 'period','gameClock','wallClock', 'lastTouch', 'live'])

            # Dataframe Clean
            #df = df.rename(columns={"playerId": "source_player_id", "number": "jersey", "wallClock": "session_elapsed_time"})

            #df['session_elapsed_time'] = pd.to_datetime(df.session_elapsed_time, unit='ms')

            df['live'] = np.where(df['live'], 'true', 'false')
            df['is_home'] = np.where(df['is_home'], 'home', 'away')
            #df.loc[(df.live == 0), 'ball_status'] = 'Dead'
            #df.loc[(df.live == 1), 'ball_status'] = 'Alive'
            #df.loc[(df.lastTouch == 'home'), 'ball_possession'] = 'H'
            #df.loc[(df.lastTouch == 'away'), 'ball_possession'] = 'A'

            # Dataframe Clean
            df = df.drop(columns=['optaUuid', 'ssiID', 'name', 'wallClock', 'number'], errors='ignore')
            df.columns = ['Player_ID', 'Speed_Player', 'Frame_ID', 'Period_ID', 'Time', 'Last_Touched', 'Is_Ball_Live', 'x_Player', 'y_Player', 'z_Player', 'Is_Home_Away', 'x_Ball', 'y_Ball', 'z_Ball', 'Speed_Ball']
            df = df[['Period_ID', 'Frame_ID', 'Time', 'Player_ID', 'Is_Home_Away', 'x_Player', 'y_Player', 'z_Player', 'Speed_Player', 'x_Ball', 'y_Ball', 'z_Ball', 'Speed_Ball', 'Is_Ball_Live', 'Last_Touched']]
            df['Period_ID'] = df['Period_ID'].astype(int)
            df['Frame_ID'] = df['Frame_ID'].astype(int)
            df['Time'] = df['Time'].astype(float)
            df['Player_ID'] = df['Player_ID'].astype(int)


        return df

    track_players_df = track_player_data(track_path)


    if track_metadata_path.endswith('.json'):
        with open(track_metadata_path, 'r') as f:
            datastore = json.load(f)

        track_players_df['game_id'] = datastore['optaId']

        #take out player ids and names from the json metadata
        player_names = []
        for squad in ['away', 'home']:
            if squad == 'away':
                for i in range(len(datastore['awayPlayers'])):
                    if datastore['awayPlayers'][i]['optaId'] is not None:
                        player_names.append([int(datastore['awayPlayers'][i]['optaId']), str(datastore['awayPlayers'][i]['name'])])
            else:
                for i in range(len(datastore['homePlayers'])):
                    if datastore['homePlayers'][i]['optaId'] is not None:
                        player_names.append([int(datastore['homePlayers'][i]['optaId']), str(datastore['homePlayers'][i]['name'])])

        player_names_df = pd.DataFrame(player_names, columns = ['player_id', 'player_name'])

    else:
        try:
            with open(track_metadata_path, encoding = 'utf-8') as fd:
                datastore = xmltodict.parse(fd.read())
        except UnicodeDecodeError:
            with open(track_metadata_path, encoding = 'latin-1') as fd:
                datastore = xmltodict.parse(fd.read())     
        
        track_players_df['game_id'] = datastore['TracabMetaData']['match']['@iId']
        player_names_df = None

    return (track_players_df, player_names_df)



#function to apply on already imported tracking data

def time_possession (track_players_df, file_results):
    import pandas as pd
    import numpy as np
    import xmltodict
    #global file_results

    if 'System_Target_ID' not in list(track_players_df.columns): #means we have output from second spectrum file
        #time played by players
        time_played_by_players = pd.DataFrame(0.04*track_players_df.groupby(['Is_Home_Away', 'Player_ID']).Frame_ID.count()).reset_index()
        #time in-out possession for players
        time_in_out_possession = pd.DataFrame(0.04*track_players_df[track_players_df.Is_Ball_Live=='true'].groupby(['Is_Home_Away', 'Player_ID', 'Last_Touched']).Frame_ID.count()).reset_index()
        time_played_by_players.rename(columns = {'Frame_ID': 'time played'}, inplace = True)
        time_in_out_possession['time in/out possession for the team'] = np.where(time_in_out_possession.Last_Touched==time_in_out_possession.Is_Home_Away, 'time in possession for the team', 'time out possession for the team').tolist()
        time_in_out_possession.rename(columns = {'Is_Home_Away': 'home/away',
            'Player_ID': 'player id', 'Frame_ID': 'time possession'}, inplace = True)

        final_data = time_played_by_players.merge(time_in_out_possession, how = 'inner', 
            left_on = ['Is_Home_Away', 'Player_ID'], right_on = ['home/away', 'player id'])[['Player_ID', 'time played', 'time possession', 'time in/out possession for the team']]
        columns_final_data = list(final_data)
        column_pivot = 'time in/out possession for the team'
        value_pivot = 'time possession'

        #now, pivot the data based on the 'in-out' column
        final_data_pivot = final_data.fillna(-999).pivot_table(index = [x for x in columns_final_data if x not in [column_pivot, value_pivot]],
            columns = column_pivot, values = value_pivot, dropna = True).reset_index().replace(-999, np.nan)


    else: #we are with the output from tracab
        track_players_df['Team_ID'] = np.where(track_players_df['Team_ID']==0, 'A', 'H')
        #time played by players
        time_played_by_players = pd.DataFrame(0.04*track_players_df.groupby(['Team_ID', 'Jersey_Number']).Frame_Count.count()).reset_index()
        #time in-out possession for players
        #if alive_only:
        time_in_out_possession = pd.DataFrame(0.04*track_players_df[track_players_df.Ball_Status=='Alive'].groupby(['Team_ID', 'Jersey_Number', 'Team_Possession']).Frame_Count.count()).reset_index()       
        time_played_by_players.rename(columns = {'Frame_Count': 'time played'}, inplace = True)
    
        time_in_out_possession['time in/out possession for the team'] = np.where(time_in_out_possession.Team_Possession==time_in_out_possession.Team_ID, 'time in possession for the team', 'time out possession for the team').tolist()
        time_in_out_possession.rename(columns = {'Team_ID': 'home/away',
            'Frame_Count': 'time possession'}, inplace = True)

        final_data = time_played_by_players.merge(time_in_out_possession, how = 'inner', 
            left_on = ['Team_ID', 'Jersey_Number'], right_on = ['home/away', 'Jersey_Number'])[['Team_ID', 'Jersey_Number', 'time played', 'time possession', 'time in/out possession for the team']]
        columns_final_data = list(final_data)
        column_pivot = 'time in/out possession for the team'
        value_pivot = 'time possession'

        #now, pivot the data based on the 'in-out' column
        final_data_pivot = final_data.fillna(-999).pivot_table(index = [x for x in columns_final_data if x not in [column_pivot, value_pivot]],
            columns = column_pivot, values = value_pivot, dropna = True).reset_index().replace(-999, 0)
        
        #file_results = file_results
        with open(file_results, encoding = 'utf-8') as fd:
            match_results = xmltodict.parse(fd.read())

        player_shirts = []
        for i in range(2):
            #team_id = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['@TeamRef']
            #team_name = np.where(team_id == 't' + str(int(home_team_id)), home_team_name, away_team_name).tolist()
            if type(match_results['SoccerFeed']['SoccerDocument']) == list:
                for k in range(len(match_results['SoccerFeed']['SoccerDocument'])):
                    if match_results['SoccerFeed']['SoccerDocument'][k]['@uID'] == 'f' + str(track_players_df.game_id.iloc[0]):
                        break

                home_away = match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['@Side'][0].upper()
                for j in range(len(match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'])):
                    player_id = match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@PlayerRef']
                    shirt_number = int(match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@ShirtNumber'])
                    player_shirts.append([player_id, shirt_number, home_away])

            else:
                home_away = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['@Side'][0].upper()
                for j in range(len(match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'])):
                    player_id = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@PlayerRef']
                    shirt_number = int(match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@ShirtNumber'])
                    player_shirts.append([player_id, shirt_number, home_away])

        player_shirts = pd.DataFrame(player_shirts, columns = ['Player_ID', 'Jersey_Number', 'home_away'])

        final_data_pivot = final_data_pivot.merge(player_shirts, how = 'inner',
            left_on = ['Team_ID', 'Jersey_Number'], 
            right_on = ['home_away', 'Jersey_Number']).drop(['Team_ID', 'home_away', 
            'Jersey_Number'], axis = 1)[['Player_ID', 'time played', 'time in possession for the team', 
            'time out possession for the team']].reset_index(drop=True)
        final_data_pivot['Player_ID'] = list(map(lambda x: int(x.replace('p', '')), final_data_pivot['Player_ID']))

    final_data_pivot = final_data_pivot.fillna(0) #to fill any na in time in possession or out possession

    return final_data_pivot




def convert_chyronego_to_spectrum (track_path, track_metadata_path, file_results):
    import json
    import xmltodict
    import pandas as pd 

    def track_player_data(track_path):
        #global track_metadata_path
        import numpy as np
        import xmltodict

        if track_path.endswith('.dat'):

            x = []
            with open(track_path) as fp:  
                for cnt, line in enumerate(fp):
                    x.append(line)

            #x[0].split(':')[0] #gives frame id
            #x[0].split(':')[1].split(';')[:(len(x[0].split(':')[1].split(';'))-1)] #gives all players tracking info
            #x[0].split(':')[2].split(';')[0] #gives ball tracking info

            #let's try to extract the player data sensibly from the array and turn them into a long dataframe
            all_players_and_frames = []

            for k in range(len(x)): 
                all_players = []
                for i in range(len(x[k].split(':')[1].split(';')[:(len(x[k].split(':')[1].split(';'))-1)])):
                    player_values = [float(item) for item in x[k].split(':')[1].split(';')[:(len(x[k].split(':')[1].split(';'))-1)][i].split(',')]
                    player_values.append(int(x[k].split(':')[0])) 
                    player_values = [player_values[j] for j in [6,0,1,2,3,4,5]]
                    all_players.append(player_values)
                    del(player_values)
                all_players_and_frames.append(all_players)
                del(all_players)

            #we first need to flatten the array
            all_players_and_frames_flattened = [item for sublist in all_players_and_frames for item in sublist]
            track_players_df = pd.DataFrame(all_players_and_frames_flattened, columns = ['Frame_ID', 'Team_ID', 'System_Target_ID', 'Jersey_Number', 'x_Player', 'y_Player', 'Speed_Player'])
    

            #let's try to use the same approach to get ball tracking data in a nicer format
            all_balls_and_frames = []
 
            for k in range(len(x)):
                all_balls_and_frames.append(x[k].split(':')[2].split(';')[0].split(','))

            track_ball_df = pd.DataFrame(all_balls_and_frames)
            if track_ball_df.shape[1] > 7:
                track_ball_df = track_ball_df.iloc[:,:7].reset_index(drop=True)
            track_ball_df.columns = ['x_Ball', 'y_Ball', 'z_Ball', 'Speed_Ball', 'Last_Touched', 'Is_Ball_Live', 'Ball_contact_dev_info']

            for col in ['x_Ball', 'y_Ball', 'z_Ball']: 
                track_ball_df[col] = 0.01*track_ball_df[col].astype(float)
            for col in ['x_Player', 'y_Player']: 
                track_players_df[col] = 0.01*track_players_df[col].astype(float)

            track_ball_df['Speed_Ball'] = 0.01*track_ball_df['Speed_Ball'].astype(float)

            #we now add the frame_count column
            track_ball_df['Frame_ID'] = [int(x[k].split(':')[0]) for k in range(len(all_balls_and_frames))]
            #track_ball_df['Is_Ball_Live'] = np.where(track_ball_df['Ball_Status']=='Alive', 'true', 'false')
            #track_ball_df['Last_Touched'] = track_ball_df['Team_Possession']
            #track_ball_df['Is_Home_Away'] = np.where()


            #we now join the data
            all_tracking_data = track_players_df.merge(track_ball_df, how = 'inner', left_on = ['Frame_ID'], right_on = ['Frame_ID'])

            #we need start and end frames for first and second half

            with open(track_metadata_path, encoding = 'utf-8') as fd:
                tracab_metadata = xmltodict.parse(fd.read())

            frame_starts = []
            frame_ends = []

            for i in range(len(tracab_metadata['TracabMetaData']['match']['period'])):
                frame_starts.append(int(tracab_metadata['TracabMetaData']['match']['period'][i]['@iStartFrame']))
                frame_ends.append(int(tracab_metadata['TracabMetaData']['match']['period'][i]['@iEndFrame']))


            all_tracking_data_splits = []
            max_frame_period = 0
            for i in range(len(frame_starts)):
                data_split = all_tracking_data[(all_tracking_data.Frame_ID >= frame_starts[i]) & (all_tracking_data.Frame_ID <= frame_ends[i]) & (~all_tracking_data.Team_ID.isin([-1,4]))].sort_values(['Frame_ID']).reset_index(drop = True)
                data_split['Period_ID'] = i + 1
                reps_per_frame = data_split[['Frame_ID', 'Team_ID']].groupby(['Frame_ID']).count().reset_index().sort_values(['Frame_ID']).reset_index(drop=True)
                data_split['Time'] = np.round(np.repeat(np.array(range(len(data_split.Frame_ID.unique())))/25, reps_per_frame.Team_ID.tolist()), 2)
                data_split['Frame_ID'] = np.round(data_split['Time']*25) + max_frame_period
                all_tracking_data_splits.append(data_split)
                max_frame_period = data_split['Frame_ID'].max()

            df = pd.concat(all_tracking_data_splits)
            df['Is_Ball_Live'] = np.where(df['Is_Ball_Live']=='Alive', 'true', 'false')
            df['Last_Touched'] = np.where(df['Last_Touched']=='A', 'away', 'home')
            df['Is_Home_Away'] = np.where(df['Team_ID']==0, 'away', 'home')
            df['z_Player'] = 0
            #all_tracking_data_final.drop_duplicates(['Frame_Count']).Team_Possession.value_counts()

        return df

    track_players_df = track_player_data(track_path)    

    try:
        with open(track_metadata_path, encoding = 'utf-8') as fd:
            datastore = xmltodict.parse(fd.read())
    except UnicodeDecodeError:
        with open(track_metadata_path, encoding = 'latin-1') as fd:
            datastore = xmltodict.parse(fd.read())     
        
    track_players_df['game_id'] = datastore['TracabMetaData']['match']['@iId']

    if 1==1:
        with open(file_results, encoding = 'utf-8') as fd:
            match_results = xmltodict.parse(fd.read())

        player_shirts = []
        for i in range(2):
            #team_id = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['@TeamRef']
            #team_name = np.where(team_id == 't' + str(int(home_team_id)), home_team_name, away_team_name).tolist()
            if type(match_results['SoccerFeed']['SoccerDocument']) == list:
                for k in range(len(match_results['SoccerFeed']['SoccerDocument'])):
                    if match_results['SoccerFeed']['SoccerDocument'][k]['@uID'] == 'f' + str(track_players_df.game_id.iloc[0]):
                        break

                home_away = match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['@Side'].lower()
                for j in range(len(match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'])):
                    player_id = match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@PlayerRef']
                    shirt_number = int(match_results['SoccerFeed']['SoccerDocument'][k]['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@ShirtNumber'])
                    player_shirts.append([player_id, shirt_number, home_away])

            else:
                home_away = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['@Side'].lower()
                for j in range(len(match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'])):
                    player_id = match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@PlayerRef']
                    shirt_number = int(match_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][i]['PlayerLineUp']['MatchPlayer'][j]['@ShirtNumber'])
                    player_shirts.append([player_id, shirt_number, home_away])

        player_shirts = pd.DataFrame(player_shirts, columns = ['Player_ID', 'Jersey_Number', 'Is_Home_Away'])

    track_players_df = track_players_df.merge(player_shirts, how ='inner').sort_values(['Period_ID', 'Time']).reset_index(drop=True)
    track_players_df = track_players_df[['Period_ID', 'Frame_ID', 'Time', 'Player_ID', 'Is_Home_Away', 'x_Player', 'y_Player', 'z_Player', 'Speed_Player', 'x_Ball', 'y_Ball', 'z_Ball', 'Speed_Ball', 'Is_Ball_Live', 'Last_Touched', 'game_id']].reset_index(drop=True)
    
    track_players_df['Player_ID'] = list(map(lambda x: int(x.replace('p','')), track_players_df['Player_ID']))
    return track_players_df