import requests
from datetime import date
import numpy as np 

today = date.today()

headers_auth = {
  'Content-Type': 'application/json'
}

r_token = requests.post('https://dvms.premierleague.com/api/v2/authenticate', params={"username": "tom.king@chelseafc.com", "remember_me": True, "password": "Omeruo1!"}, headers = headers_auth)


headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'Hudl-AuthToken': r_token.json()['token']
}

headers_token = {'Hudl-AuthToken': r_token.json()['token']}

season = '2020'
season_complete = '2020-21'
premier_league_link = 'https://dvms.premierleague.com/dvms/58769ca489b30086dbf1ca7d/fixtures/{}'.format(season)

page_number = 0
#i = 0
r_assets = requests.post(premier_league_link, params={"pageNumber": page_number}, headers = headers)
#for h in range(1):
while len(r_assets.json()['fixtures']) > 0:
    #loop over games
    #for l in range(1):
    for i in range(len(r_assets.json()['fixtures'])):
        year, month, day = r_assets.json()['fixtures'][i]['date'].split('-')
        day = int(day.split('T')[0])
        year = int(year)
        month = int(month)
        #if yesterday there was an update to the files
        if (today - date(year, month, day)).days == 1:
            match_id = r_assets.json()['fixtures'][i]['optaMatchId']
            #loop over assets
            for j in range(len(r_assets.json()['fixtures'][i]['assets'])):

                #start getting the specific assets we need

                #lighter files
                if ((r_assets.json()['fixtures'][i]['assets'][j]['type']==2) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==20)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==2) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==21)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==5) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==38)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==5) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==40)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==5) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==42)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==5) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==43)): #| ((r_assets.json()['fixtures'][i]['assets'][j]['type']==5) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==44)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==0) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==0)) | ((r_assets.json()['fixtures'][i]['assets'][j]['type']==0) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==1)):
                    html_link = premier_league_link.replace('/{}'.format(season), '') + '/' + '/'.join(r_assets.json()['fixtures'][i]['assets'][j]['localDownloadUrl'].split('/')[:-1]) 
                    all_assets = requests.get(html_link, params={"username": "tom.king@chelseafc.com", "remember_me": True, "password": "Omeruo1!"}, headers = headers_token).text
                    all_assets = all_assets.replace('[', '').replace(']', '').replace('"', '')
                    myfile = requests.get(all_assets.split(',')[np.where([r_assets.json()['fixtures'][i]['assets'][j]['key'].split('/')[-1] in link for link in all_assets.split(',')])[0][0]], allow_redirects=True)
                    file_name = r_assets.json()['fixtures'][i]['assets'][j]['key'].split('/')[-1]
                    if ((r_assets.json()['fixtures'][i]['assets'][j]['type']==2) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==20)):
                        file_name = 'f24-8-{}-{}-eventdetails.xml'.format(season, match_id.replace('g', ''))
                    if ((r_assets.json()['fixtures'][i]['assets'][j]['type']==2) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==21)):
                        file_name = 'srml-8-{}-{}-matchresults.xml'.format(season, match_id.replace('g', 'f'))
                    if ((r_assets.json()['fixtures'][i]['assets'][j]['type']==5) & (r_assets.json()['fixtures'][i]['assets'][j]['subType']==44)):
                        open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League\\second spectrum insight\\{}'.format(season_complete, file_name), 'wb').write(myfile.content)
                    else:
                        open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League\\{}'.format(season_complete, file_name), 'wb').write(myfile.content)

    page_number += 1
    r_assets = requests.post(premier_league_link, params={"pageNumber": page_number}, headers = headers)
