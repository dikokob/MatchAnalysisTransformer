import pandas as pd

import numpy as np


list_of_files = ['df_aerial_duels_from_crosses.csv', 'df_aerial_duels_from_set_pieces.csv',
                 'df_crosses_output.csv', 'df_second_phase_set_pieces.csv',
                 'df_second_phase_set_pieces_shots.csv', 'df_shots_from_crosses.csv', 'df_opta_core_stats.csv']

for file in list_of_files:
    df_old = pd.read_csv(f'data/processed/g1059702 - old/{file}', sep='\t')

    df_new = pd.read_csv(f'data/processed - new/g1059702 - new/{file}')

    new_list = list(df_new.columns)
    old_list = list(df_old.columns)

    if sorted(new_list) != sorted(old_list):
        main_list = np.setdiff1d(old_list, new_list)
        print(f"Columns do not match on {file}. Missing columns {main_list}")



print('main')
