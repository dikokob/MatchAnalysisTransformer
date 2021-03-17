import pandas as pd

azureCrosses = pd.read_csv('AzureCrosses.csv')
azureCrosses['game_id'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
azureCrosses['game_id'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
azureCrosses_game_id = list(azureCrosses['game_id'].unique())

del azureCrosses

azureOptaCoreStats = pd.read_csv('AzureOptaCoreStats.csv')
azureOptaCoreStats['Game ID'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
azureOptaCoreStats['Game ID'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
azureOptaCoreStats_game_id = list(azureOptaCoreStats['Game ID'].unique())
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

azureSecondPhaseSetPieces = pd.read_csv('AzureSecondPhaseSetPieces.csv')
azureSecondPhaseSetPieces['game_id'].replace(regex=True, inplace=True, to_replace=r'g', value=r'')
azureSecondPhaseSetPieces['game_id'].replace(regex=True, inplace=True, to_replace=r'f', value=r'')
azureSecondPhaseSetPieces_game_id = list(azureSecondPhaseSetPieces['game_id'].unique())

del azureSecondPhaseSetPieces

'''
missing_values = []
for item in allOptaCoreStats_game_id:
    if item not in azureOptaCoreStats_game_id:
        missing_values.append(item)
        
[item for item in allOptaCoreStats_game_id if item not in azureOptaCoreStats_game_id]
'''

text_file = open("missingInAzure.txt", "w")
n = text_file.write(str([item for item in allOptaCoreStats_game_id if item not in azureOptaCoreStats_game_id]))
text_file.close()

CoreStatsMissing = allOptaCoreStats[allOptaCoreStats['Game ID'].isin(
    [item for item in allOptaCoreStats_game_id if item not in azureOptaCoreStats_game_id])]
CoreStatsMissingGroupByCount = CoreStatsMissing[['Competition Name', 'Game ID', 'Season Name']].drop_duplicates() \
    .groupby(['Competition Name', 'Season Name']).agg({"Game ID": "sum"})

text_file = open("missingInSetPieces.txt", "w")
n = text_file.write(str([item for item in azureOptaCoreStats_game_id if item not in azureSecondPhaseSetPieces_game_id]))
text_file.close()

missingInSecondPhaseSetPieces = azureOptaCoreStats[azureOptaCoreStats['Game ID'].isin(
    [item for item in azureOptaCoreStats_game_id if item not in azureSecondPhaseSetPieces_game_id])]
missingInSecondPhaseSetPiecesGroupByCount = missingInSecondPhaseSetPieces[
    ['Competition Name', 'Game ID', 'Season Name']].drop_duplicates() \
    .groupby(['Competition Name', 'Season Name']).agg({"Game ID": "sum"})

print('main')
