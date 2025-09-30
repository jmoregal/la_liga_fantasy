import pandas as pd
from bs4 import BeautifulSoup, Comment
import os
import requests
import time
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
]

def obtain_data(url, save_dir='../data/raw'):
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
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
            pass
    
    return dataframes


def read_html_tables(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, "lxml")
    tables = soup.find_all("table")
    return tables


def clean_players_stats(table):
    rows = []
    for tr in table.find_all("tr"):
        row = []
        for td in tr.find_all(["td", "th"]):
            link = td.find("a")
            if link:
                # Guardar tanto el texto como el link
                row.append((link.text.strip(), link["href"]))
            else:
                row.append(td.get_text(strip=True))
        rows.append(row)
    df = pd.DataFrame(rows)
    """ Limpieza y extracción de links en la tabla de stats de jugadores """
    # Renombrar columnas
    df = df.rename(columns={0: "Rk", 1: "Player", 2: "Nation", 3: "Pos", 4: "Squad", 5: "Age", 6: "Born",
                            7: "MP", 8: "Starts", 9: "Min", 10: "90s", 11: "Gls", 12: "Ast", 13: "G+A",
                            14: "G-PK", 15: "PK", 16: "PKatt", 17: "CrdY", 18: "CrdR", 19: "xG", 20: "npxG",
                            21: "xAG", 22: "npxG+xAG", 23: "PrgC", 24: "PrgP", 25: "PrgR",
                            26: "Gls-90min", 27: "Ast-90min", 28: "G+A-90min", 29: "G-PK-90min",
                            30: "G+A-PK-90min", 31: "xG-90min", 32: "xAG-90min", 33: "xG+xA-90min",
                            34: "npxG-90min", 35: "npxG+xAG-90min", 36: "Matches"})
    
    # Limpiar celdas con tuplas (texto, link)
    df.Player = df.Player.apply(lambda x: x[0] if isinstance(x, tuple) else x)
    df.Nation = df.Nation.apply(lambda x: x[0] if isinstance(x, tuple) else x)
    df.Squad = df.Squad.apply(lambda x: x[0] if isinstance(x, tuple) else x)
    df.Matches = df.Matches.apply(lambda x: x[1] if isinstance(x, tuple) else None)
    
    # Quitar filas iniciales de cabecera repetida
    df = df.drop([0, 1]).reset_index(drop=True)
    return df

def clean_squads_stats(table):
    df = pd.read_html(str(table))[0]
    return df


def player_matches_scrap(players_df, save_dir='../data/raw/players_matches'):
    os.makedirs(save_dir, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for _, row in players_df.iterrows():
        player_name = row["Player"].replace(" ", "_")
        url = row["Matches"]
        
        if url is None:
            continue
        
        full_url = f"https://fbref.com{url}"
        response = requests.get(full_url, headers=headers)
        if response.status_code != 200:
            print(f"⚠️ No se pudo obtener {player_name}")
            continue
        
        soup = BeautifulSoup(response.text, "html.parser")
        tables = pd.read_html(response.text)
        
        # Si hay varias tablas, cogemos la primera con partidos
        if len(tables) == 0:
            continue
        
        df_matches = tables[0]
        print(df_matches.head())
        df_matches.to_parquet(os.path.join(save_dir, f"data/raw/{player_name}_matches.parquet"), index=False)
        
        print(f"✔ Guardados partidos de {player_name}")
        time.sleep(1)  # para no saturar fbref


if __name__ == '__main__':
    # 1. Stats de jugadores y plantillas
    tables_stats = read_html_tables('html_players.txt')
    players_stats_raw = tables_stats[2]
    squads_stats = tables_stats[0]
    
    players_stats = clean_players_stats(players_stats_raw)
    squads_stats = clean_squads_stats(squads_stats) 
    
    players_stats.to_parquet('data/raw/fbref_players_stats.parquet', index=False)
    squads_stats.to_parquet('data/raw/fbref_squads_stats.parquet', index=False)

    # 2. Ranking
    tables_rankings = obtain_data(url='https://fbref.com/en/comps/12/La-Liga-Stats')
    ranking_general = tables_rankings[0]
    ranking_home_away = tables_rankings[1]
    
    ranking_general.to_parquet('data/raw/fbref_ranking_general.parquet', index=False)
    ranking_home_away.to_parquet('data/raw/fbref_ranking_home_away.parquet', index=False)

    # 3. Resultados
    tables_results = obtain_data(url='https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures')
    results = tables_results[0]
    
    results.to_parquet('data/raw/fbref_results.parquet', index=False)
    
    # 4. Partidos por jugador
    player_matches_scrap(players_stats)
