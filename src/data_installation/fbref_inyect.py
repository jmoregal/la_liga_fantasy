import pandas as pd
from bs4 import BeautifulSoup, Comment
import os
import requests

all_logs = []

def obtain_data(url ,save_dir='../data/raw'):
    # Descargar el HTML (metodo que he encontrado para poder descargar tablas comentadas)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        html = response.text
        with open("html.txt", "w", encoding="utf-8") as f:
            f.write(html)
    else:
        raise Exception(f"Error al descargar la página: {response.status_code}")
    
    # Leer HTML guardado
    with open("html.txt", "r", encoding="utf-8") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    # Guardamos todos los DataFrames aquí
    dataframes = []
    # 1. Tablas normales (no comentadas)
    dfs_normal = pd.read_html(html)
    dataframes.extend(dfs_normal)
    # 2. Tablas dentro de comentarios
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for c in comments:
        try:
            print(c)
            dfs_comment = pd.read_html(c)
            dataframes.extend(dfs_comment)
        except ValueError:
            # si no hay tablas en ese comentario, pasa
            pass
    print(f"Se han encontrado {len(dataframes)} tablas en total")

    # Creamos un diccionario con las tablas y sus nombres
    os.makedirs(save_dir, exist_ok=True)
   
    return dataframes

def player_matches_scrap(df, save_dir='../data/raw'):
    for _, rows in df.iterrows():
        player_id = rows['(\'Unnamed: 1_level_0\', \'Player\')']
        url = rows['(\'Unnamed: 36_level_0\', \'Matches\')']
        print(url)
        
        html = requests.get(url).text
        tables = pd.read_html(html)
        match_log = tables[0]
        match_log['Player'] = player_id
        match_log['Squad'] = rows['(\'Unnamed: 4_level_0\', \'Squad\')']

        print(f"Procesando jugador: {player_id}, partidos: {len(match_log)}")

        # all_logs.append(match_log)
        time.sleep(0.5)  # Pausa para no saturar 


_ = obtain_data(url='https://fbref.com/en/comps/12/stats/La-Liga-Stats', )
prueba = pd.read_parquet('data/raw/fbref_players_stats.parquet')

# print(prueba.columns)
print(prueba.head(3))
player_matches_scrap(prueba)

# if __name__ == '__main__':

#     # tabla con estadisticas de jugadores y plantillas
#     tables_stats = obtain_data(url='https://fbref.com/en/comps/12/stats/La-Liga-Stats', )
#     players_stats = tables_stats[2]
#     squads_stats = tables_stats[0]

#     players_stats.to_parquet('data/raw/fbref_players_stats.parquet', index=False)
#     squads_stats.to_parquet('data/raw/fbref_squads_stats.parquet', index=False)

#     # tabla con clasificación general, local|visitante 
#     tables_rankings = obtain_data(url='https://fbref.com/en/comps/12/La-Liga-Stats', )
#     ranking_general = tables_stats[0]
#     ranking_home_away = tables_stats[1]

#     ranking_general.to_parquet('data/raw/fbref_ranking_general.parquet', index=False)
#     ranking_home_away.to_parquet('data/raw/fbref_ranking_home_away.parquet', index=False)

#     # tabla con resultados de partidos
#     tables_results = obtain_data(url='https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures', )
#     results = tables_results[0]

#     results.to_parquet('data/raw/fbref_results.parquet', index=False)
    
#     # tabla jugador-partidos
#     player_match_stats = player_matches_scrap(players_stats)


# print(players_stats.head())
# print(squads_stats.head())







