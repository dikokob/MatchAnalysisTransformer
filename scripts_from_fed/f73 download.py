#f73 files extraction from ftp server
from ftplib import FTP
import wget
import os
import shutil
import time
from datetime import date


season_complete = '2020-21'
season = '2020'
if 'Opta f73 Files' not in os.listdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format(season_complete)):
    os.mkdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League\\Opta f73 Files'.format(season_complete))
os.chdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League\\Opta f73 Files'.format(season_complete))


while sum(['dummy' in x for y in os.listdir('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format(season_complete)) for x in y.split(' ')]) > 0:
    time.sleep(5)
    pass

open('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format(season_complete) + '\\' + 'f73 download dummy.txt', 'w').close()


link = 'ftp.performgroup.com'
folder = 'PL f73 feeds'
user = 'opta_pro_chelsea_client'
password = '8t3RE8@yx^z4fFn'

ftp = FTP(link)
ftp.login(user, password)
ftp.cwd(folder)

gen_files = ftp.mlsd()

date_today = date.today()

list_files = []
list_days_ago = []
for x in gen_files:
    if 'f73' in x[0]:
        list_files.append(x[0])
        #we need to extract dates of modifications as well
        timestamp = ftp.voidcmd('MDTM ' + x[0]).split(' ')[-1]
        date_modification = date.fromtimestamp(time.mktime(time.strptime(timestamp, '%Y%m%d%H%M%S')))
        days_ago = (date_today - date_modification).days
        list_days_ago.append(days_ago)



ftp.quit()

#after exiting ftp server, use the retrieved file names to download files with wget

# for x in list_files:
#     output = os.path.join(os.getcwd(), x)
#     file = wget.download('ftp://' + user + ':' + password + '@' + link + '/' + folder + '/' + x, output)
#     if os.path.exists(output):
#         shutil.move(file,output)

for x,y in zip(list_files, list_days_ago):
    output = os.path.join(os.getcwd(), x)
    if ((x not in os.listdir()) | (y <= 7)) & (season in x):
        file = wget.download('ftp://' + user + ':' + password + '@' + link + '/' + folder + '/' + x, output)
        if os.path.exists(output):
            shutil.move(file, output)

if 'README' in os.listdir():
    os.remove('README')

if len([x for x in os.listdir() if (x.endswith('.xml') is False)]) > 0:
    for y in [x for x in os.listdir() if (x.endswith('.xml') is False)]:
        os.remove(y)

if len([x for x in os.listdir() if (season not in x)]) > 0:
    for y in [x for x in os.listdir() if (season not in x)]:
        os.remove(y)

os.remove('\\\ctgshares\\Drogba\\API Data Files\\{}\\Premier League'.format(season_complete) + '\\' + 'f73 download dummy.txt')  