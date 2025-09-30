import pandas as pd
from bs4 import BeautifulSoup, Comment
import os
import requests
import time

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
            dfs_comment = pd.read_html(c)
            dataframes.extend(dfs_comment)
        except ValueError:
            # si no hay tablas en ese comentario, pasa
            pass
    links = []
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("/en/players/") and len(a["href"])>20:
            full_url = a["href"]
            links.append(full_url)
            print(full_url)
    print(f"Se han encontrado {len(dataframes)} tablas en total")

    # Creamos un diccionario con las tablas y sus nombres
    os.makedirs(save_dir, exist_ok=True)
   
    return dataframes

def player_matches_scrap(df, save_dir='../data/raw'):
    rows = []
    for tr in df.find_all("tr"):
        row = []
        for td in tr.find_all(["td", "th"]):
            link = td.find("a")
            if link:
                # Guardar tanto el texto como el link
                row.append((link.text.strip(), link["href"]))
            else:
                row.append(td.get_text(strip=True))
        rows.append(row)
    player_matches_df = pd.DataFrame(rows)
    player_matches_df = player_matches_df['']



def read_html_tables(file_path):
    # Reads an HTML file already saved locally, and returns all tables as a list of DataFrames.
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
    tables = pd.read_html(html_content)
    return tables

# print(prueba.columns)
# print(prueba.head(3))
# player_matches_scrap(prueba)

if __name__ == '__main__':

    # tabla con estadisticas de jugadores y plantillas
    tables_stats = read_html_tables('html.txt')
    players_stats = tables_stats[2]
    squads_stats = tables_stats[0]

    players_stats.to_parquet('data/raw/fbref_players_stats.parquet', index=False)
    squads_stats.to_parquet('data/raw/fbref_squads_stats.parquet', index=False)

    # tabla con clasificación general, local|visitante 
    tables_rankings = obtain_data(url='https://fbref.com/en/comps/12/La-Liga-Stats', )
    ranking_general = tables_stats[0]
    ranking_home_away = tables_stats[1]

    ranking_general.to_parquet('data/raw/fbref_ranking_general.parquet', index=False)
    ranking_home_away.to_parquet('data/raw/fbref_ranking_home_away.parquet', index=False)

    # tabla con resultados de partidos
    tables_results = obtain_data(url='https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures', )
    results = tables_results[0]

    results.to_parquet('data/raw/fbref_results.parquet', index=False)
    
    # tabla jugador-partidos
    player_match_stats = player_matches_scrap(players_stats)


# print(players_stats.head())
# print(squads_stats.head())







