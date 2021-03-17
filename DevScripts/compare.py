import pandas as pd

# read file
azureCrosses = pd.read_csv('AzureCrosses.csv')
# replace with empty value, all rows in game_id that start by 'g'
azureCrosses['game_id'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
# replace with empty value, all rows in game_id that start by 'f'
azureCrosses['game_id'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
# list of unique game ids
azureCrosses_game_id = list(azureCrosses['game_id'].unique())

del azureCrosses
# read file
azureOptaCoreStats = pd.read_csv('AzureOptaCoreStats.csv')
# replace with empty value, all rows in game_id that start by 'g'
azureOptaCoreStats['Game ID'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
# replace with empty value, all rows in game_id that start by 'f'
azureOptaCoreStats['Game ID'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
# list of unique game ids
azureOptaCoreStats_game_id = list(azureOptaCoreStats['Game ID'].unique())
# Group Opta Core Stats
azureOptaCoreStatsGroupByCount = azureOptaCoreStats[['Competition Name', 'Game ID', 'Season Name']].drop_duplicates() \
    .groupby(['Competition Name', 'Season Name']).agg({"Game ID": "count"})

# del azureOptaCoreStats


allOptaCoreStats = pd.read_csv('all core stats.csv')
allOptaCoreStats['Game ID'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
allOptaCoreStats['Game ID'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
allOptaCoreStats_game_id = list(allOptaCoreStats['Game ID'].unique())
allOptaCoreStatsGroupByCount = allOptaCoreStats[['Competition Name', 'Game ID', 'Season Name']].drop_duplicates() \
    .groupby(['Competition Name', 'Season Name']).agg({"Game ID": "count"})

# del allOptaCoreStats

#read file
azureSecondPhaseSetPieces = pd.read_csv('AzureSecondPhaseSetPieces.csv')
# replace with empty value, all rows in game_id that start by 'g'
azureSecondPhaseSetPieces['game_id'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
# replace with empty value, all rows in game_id that start by 'f'
azureSecondPhaseSetPieces['game_id'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
# list of unique game ids
azureSecondPhaseSetPieces_game_id = list(azureSecondPhaseSetPieces['game_id'].unique())

del azureSecondPhaseSetPieces

'''
missing_values = []
for item in allOptaCoreStats_game_id:
    if item not in azureOptaCoreStats_game_id:
        missing_values.append(item)
        
[item for item in allOptaCoreStats_game_id if item not in azureOptaCoreStats_game_id]
'''

#creating a file with the game ids missing in Opta Core Stats
text_file = open("missingInAzure.txt", "w")
n = text_file.write(str([item for item in allOptaCoreStats_game_id if item not in azureOptaCoreStats_game_id]))
text_file.close()

# getting the game ids missing in azureOptaCoreStats compared to allOptaCoreStats
CoreStatsMissing = allOptaCoreStats[allOptaCoreStats['Game ID'].isin(
    [item for item in allOptaCoreStats_game_id if item not in azureOptaCoreStats_game_id])]
CoreStatsMissingGroupByCount = CoreStatsMissing[['Competition Name', 'Game ID', 'Season Name']].drop_duplicates() \
    .groupby(['Competition Name', 'Season Name']).agg({"Game ID": "sum"})

#creating a file with the game ids missing in Set Pieces
text_file = open("missingInSetPieces.txt", "w")
n = text_file.write(str([item for item in azureOptaCoreStats_game_id if item not in azureSecondPhaseSetPieces_game_id]))
text_file.close()

# getting the game ids missing in azureSecondPhaseSetPieces compared to azureOptaCoreStats
missingInSecondPhaseSetPieces = azureOptaCoreStats[azureOptaCoreStats['Game ID'].isin(
    [item for item in azureOptaCoreStats_game_id if item not in azureSecondPhaseSetPieces_game_id])]
missingInSecondPhaseSetPiecesGroupByCount = missingInSecondPhaseSetPieces[
    ['Competition Name', 'Game ID', 'Season Name']].drop_duplicates() \
    .groupby(['Competition Name', 'Season Name']).agg({"Game ID": "sum"})

print('main')
