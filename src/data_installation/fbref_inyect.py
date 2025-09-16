import pandas as pd
import os

def obtain_data(url ,save_dir='../data/raw'):
    os.makedirs(save_dir, exist_ok=True)
    tables = pd.read_html(url)
    return tables

tables_stats = obtain_data(url='https://fbref.com/en/comps/12/stats/La-Liga-Stats')
print(len(tables_stats))
players_stats = tables_stats[1]
squads_stats = tables_stats[0]

print(players_stats.head())
print(squads_stats.head())







