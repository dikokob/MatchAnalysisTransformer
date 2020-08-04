#script for the OPTA files manipulation

def opta_event_file_manipulation(path_events):
    import xmltodict
    import numpy as np
    import pandas as pd

    try:
        with open(path_events, encoding = 'utf-8') as fd:
            opta_event = xmltodict.parse(fd.read())
    except UnicodeDecodeError:
        with open(path_events, encoding = 'latin-1') as fd:
            opta_event = xmltodict.parse(fd.read())    

    if 'f73' in path_events:
        opta_event['Games'] = opta_event.pop('SoccerFeed')    
    
    #start rearranging the file in a nicer format
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
    away_score = None
    if '@away_score' in list(opta_event['Games']['Game'].keys()):
        away_score = int(opta_event['Games']['Game']['@away_score'])
    away_team_id = int(opta_event['Games']['Game']['@away_team_id'])
    away_team_name = opta_event['Games']['Game']['@away_team_name']
    home_score = None
    if '@home_score' in list(opta_event['Games']['Game'].keys()):
        home_score = int(opta_event['Games']['Game']['@home_score'])
    home_team_id = int(opta_event['Games']['Game']['@home_team_id'])
    home_team_name = opta_event['Games']['Game']['@home_team_name']
    fixture = home_team_name + ' v ' + away_team_name
    result = None
    if (home_score is not None) & (away_score is not None):
        result = str(home_score) + ' - ' + str(away_score)

    #loop over events in the opta file
    for i in range(len(opta_event['Games']['Game']['Event'])):
        unique_event_id = int(opta_event['Games']['Game']['Event'][i]['@id'])
        event_id = int(opta_event['Games']['Game']['Event'][i]['@event_id'])
        type_id = int(opta_event['Games']['Game']['Event'][i]['@type_id'])
        period_id = int(opta_event['Games']['Game']['Event'][i]['@period_id'])
        mins = int(opta_event['Games']['Game']['Event'][i]['@min'])
        sec = int(opta_event['Games']['Game']['Event'][i]['@sec'])
        team_id = int(opta_event['Games']['Game']['Event'][i]['@team_id'])
        outcome = int(opta_event['Games']['Game']['Event'][i]['@outcome'])
        #we need to check conditions for existence of player_id, keypass and assist
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

        #inner loop over qualifiers - if there is only one qualifier then we go straight with the dict like approach
        #we need first to check whether there are any qualifiers
        if len(set(list(opta_event['Games']['Game']['Event'][i].keys())).intersection(set(['Q']))) == 0:
            qualifier_id = None
            value = None
            opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day, 
                game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name, 
                away_score, away_team_id, away_team_name, fixture, result, 
                unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id, outcome, keypass, assist,
                x, y, qualifier_id, value])
        else:
            if type(opta_event['Games']['Game']['Event'][i]['Q']) != list:
                qualifier_id = int(opta_event['Games']['Game']['Event'][i]['Q']['@qualifier_id'])
                if len(set(list(opta_event['Games']['Game']['Event'][i]['Q'].keys())).intersection(set(['@value']))) == 1:
                    value = opta_event['Games']['Game']['Event'][i]['Q']['@value']
                else:
                    value = None
            
                opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day, 
                    game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name, 
                    away_score, away_team_id, away_team_name, fixture, result, 
                    unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id, outcome, keypass, assist,
                    x, y, qualifier_id, value])
            else:
                for j in range(len(opta_event['Games']['Game']['Event'][i]['Q'])):
                    qualifier_id = int(opta_event['Games']['Game']['Event'][i]['Q'][j]['@qualifier_id'])
                    #check whether '@value' is present or not
                    if len(set(list(opta_event['Games']['Game']['Event'][i]['Q'][j].keys())).intersection(set(['@value']))) == 1: #value present
                        value = opta_event['Games']['Game']['Event'][i]['Q'][j]['@value']
                    else:
                        value = None

                    #append everything
                    opta_event_data.append([competition_id, competition_name, season_id, season_name, game_id, match_day, 
                        game_date, period_1_start, period_2_start, home_score, home_team_id, home_team_name, 
                        away_score, away_team_id, away_team_name, fixture, result, 
                        unique_event_id, event_id, period_id, mins, sec, timestamp, type_id, player_id, team_id, outcome, keypass, assist,
                        x, y, qualifier_id, value])

    #convert the list to a pandas dataframe
    opta_event_data_df = pd.DataFrame(opta_event_data, index = None, columns = ['competition_id', 'competition_name', 'season_id', 'season_name', 'game_id', 'match_day', 
                        'game_date', 'period_1_start', 'period_2_start', 'home_score', 'home_team_id', 'home_team_name', 
                        'away_score', 'away_team_id', 'away_team_name', 'fixture', 'result', 
                        'unique_event_id', 'event_id', 'period_id', 'min', 'sec', 'timestamp', 'type_id', 'player_id', 'team_id', 'outcome', 'keypass', 'assist',
                        'x', 'y', 'qualifier_id', 'value'])

    return (opta_event_data_df, game_id, game_date, away_score, away_team_id, away_team_name, home_score, home_team_id, home_team_name)



def match_results_file_manipulation(path_match_results):
    import xmltodict
    import pandas as pd 
    import numpy as np

    #import xml file and convert to dictionary
    try:
        with open(path_match_results, encoding = 'utf-8') as fd:
            opta_results = xmltodict.parse(fd.read())
    except UnicodeDecodeError:
        with open(path_match_results, encoding = 'latin-1') as fd:
            opta_results = xmltodict.parse(fd.read())  

    if type(opta_results['SoccerFeed']['SoccerDocument']) != list:
        game_id = int(opta_results['SoccerFeed']['SoccerDocument']['@uID'].strip('f'))
        competition_id = int(opta_results['SoccerFeed']['SoccerDocument']['Competition']['@uID'].strip('c'))
        competition_name = opta_results['SoccerFeed']['SoccerDocument']['Competition']['Name']
        referee_id = int(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['MatchOfficial']['@uID'].strip('o'))
        referee_name = opta_results['SoccerFeed']['SoccerDocument']['MatchData']['MatchOfficial']['OfficialName']['First'] + ' ' + opta_results['SoccerFeed']['SoccerDocument']['MatchData']['MatchOfficial']['OfficialName']['Last']
        venue =  opta_results['SoccerFeed']['SoccerDocument']['Venue']['Name']
        team_data = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'])
        players_df_home = pd.DataFrame(team_data.PlayerLineUp.iloc[0]['MatchPlayer'])
        players_df_home['team_id'] = int(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0]['@TeamRef'].strip('t'))
        players_df_away = pd.DataFrame(team_data.PlayerLineUp.iloc[1]['MatchPlayer'])
        players_df_away['team_id'] = int(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1]['@TeamRef'].strip('t'))
        if '@Formation_Place' not in list(players_df_home):
            players_df_home = players_df_home[['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        elif '@Captain' not in list(players_df_home):
            players_df_home = players_df_home[['@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        else:
            players_df_home = players_df_home[['@Captain', '@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        if '@Formation_Place' not in list(players_df_away):
            players_df_away = players_df_away[['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        elif '@Captain' not in list(players_df_away):
            players_df_home = players_df_away[['@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        else:
            players_df_away = players_df_away[['@Captain', '@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        players_df_lineup = pd.concat([players_df_home, players_df_away])
        if '@Formation' not in list(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0].keys()):
            home_formation = None
        else:
            try:
                home_formation = int(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0]['@Formation'])
            except ValueError:
                home_formation = int(''.join(filter(str.isdigit, opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][0]['@Formation'])))
        if '@Formation' not in list(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1].keys()):
            away_formation = None
        else:
            try:
                away_formation = int(opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1]['@Formation'])
            except ValueError:
                away_formation = int(''.join(filter(str.isdigit, opta_results['SoccerFeed']['SoccerDocument']['MatchData']['TeamData'][1]['@Formation'])))

        player_names_home_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument']['Team'][0]['Player'])
        player_names_away_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument']['Team'][1]['Player'])
        player_names_home_raw = player_names_home_raw[['@Position', '@uID', 'PersonName']]
        player_names_away_raw = player_names_away_raw[['@Position', '@uID', 'PersonName']]
        player_names_raw = pd.concat([player_names_home_raw, player_names_away_raw])
        player_names_raw['player_name'] = [(player_names_raw['PersonName'].iloc[i]['First'] + ' ' + 
            player_names_raw.PersonName.iloc[i]['Last']) if len(set(list(player_names_raw.PersonName.iloc[i].keys())).intersection(set(['Known']))) == 0 else player_names_raw.PersonName.iloc[i]['Known'] for i in range(player_names_raw.shape[0])]

    else: #take only first coming up
        game_id = int(opta_results['SoccerFeed']['SoccerDocument'][0]['@uID'].strip('f'))
        competition_id = int(opta_results['SoccerFeed']['SoccerDocument'][0]['Competition']['@uID'].strip('c'))
        competition_name = opta_results['SoccerFeed']['SoccerDocument'][0]['Competition']['Name']
        referee_id = int(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['MatchOfficial']['@uID'].strip('o'))
        referee_name = opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['MatchOfficial']['OfficialName']['First'] + ' ' + opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['MatchOfficial']['OfficialName']['Last']
        venue =  opta_results['SoccerFeed']['SoccerDocument'][0]['Venue']['Name']        
        team_data = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'])
        players_df_home = pd.DataFrame(team_data.PlayerLineUp.iloc[0]['MatchPlayer'])
        players_df_home['team_id'] = int(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][0]['@TeamRef'].strip('t'))
        players_df_away = pd.DataFrame(team_data.PlayerLineUp.iloc[1]['MatchPlayer'])
        players_df_away['team_id'] = int(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][1]['@TeamRef'].strip('t'))
        if '@Formation_Place' not in list(players_df_home):
            players_df_home = players_df_home[['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        elif '@Captain' not in list(players_df_home):
            players_df_home = players_df_home[['@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        else:
            players_df_home = players_df_home[['@Captain', '@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        if '@Formation_Place' not in list(players_df_away):
            players_df_away = players_df_away[['@Captain', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        elif '@Captain' not in list(players_df_away):
            players_df_home = players_df_away[['@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        else:
            players_df_away = players_df_away[['@Captain', '@Formation_Place', '@PlayerRef', '@Position', '@ShirtNumber', '@Status', '@SubPosition', 'team_id']]
        players_df_lineup = pd.concat([players_df_home, players_df_away])
        if '@Formation' not in list(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][0].keys()):
            home_formation = None
        else:
            try:
                home_formation = int(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][0]['@Formation'])
            except ValueError:
                home_formation = int(''.join(filter(str.isdigit, opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][0]['@Formation'])))
        if '@Formation' not in list(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][1].keys()):
            away_formation = None
        else:
            try:
                away_formation = int(opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][1]['@Formation'])
            except ValueError:
                away_formation = int(''.join(filter(str.isdigit, opta_results['SoccerFeed']['SoccerDocument'][0]['MatchData']['TeamData'][1]['@Formation'])))

        player_names_home_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument'][0]['Team'][0]['Player'])
        player_names_away_raw = pd.DataFrame(opta_results['SoccerFeed']['SoccerDocument'][0]['Team'][1]['Player'])
        player_names_home_raw = player_names_home_raw[['@Position', '@uID', 'PersonName']]
        player_names_away_raw = player_names_away_raw[['@Position', '@uID', 'PersonName']]
        player_names_raw = pd.concat([player_names_home_raw, player_names_away_raw])
        player_names_raw['player_name'] = [(player_names_raw['PersonName'].iloc[i]['First'] + ' ' + 
            player_names_raw.PersonName.iloc[i]['Last']) if len(set(list(player_names_raw.PersonName.iloc[i].keys())).intersection(set(['Known']))) == 0 else player_names_raw.PersonName.iloc[i]['Known'] for i in range(player_names_raw.shape[0])]



    return (referee_id, referee_name, venue, players_df_lineup, home_formation, away_formation, player_names_raw)
