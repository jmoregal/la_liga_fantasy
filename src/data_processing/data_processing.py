import pandas as pd

# Abrir todos los parquets en DataFrames
df_ranking = pd.read_parquet("../../data/raw/fbref_ranking_general.parquet")
df_ranking_ha = pd.read_parquet("../../data/raw/fbref_ranking_home_away.parquet")
df_players = pd.read_parquet("../../data/raw/fbref_players_stats.parquet")
df_matches = pd.read_parquet("../../data/raw/fbref_results.parquet")
df_squad = pd.read_parquet("../../data/raw/fbref_squads_stats.parquet")
df_matches_players = pd.read_parquet("../../data/raw/fbref_player_matches.parquet")


# ranking preprocess:
#  -> Eliminar columnas innecesarias
df_ranking = df_ranking.drop(columns=["Attendance", "Notes"])

# ranking home/away preprocess:
#  -> Renombrar columnas
for col in df_ranking_ha.columns:
    col_clean = col.strip().replace("(", "").replace(")", "").replace("'", "")
    parts = [p.strip() for p in col_clean.split(",")]
    
    if len(parts) == 2:
        if parts[0] == "Home":
            new_col = parts[1] + "_home"
        elif parts[0] == "Away":
            new_col = parts[1] + "_away"
        else:
            new_col = parts[1]
    else:
        new_col = parts[0]
    
    df_ranking_ha = df_ranking_ha.rename(columns={col: new_col})

# players preprocess:
#  -> Eliminar columnas innecesarias
df_players = df_players.drop(columns=["Rk", "Nation", "Matches"])

# matches preprocess:
#  -> Eliminar columnas innecesarias
df_matches = df_matches.drop(columns=["Date", "Match Report", "Notes"])

# squad preprocess:
#  -> Renombrar columnas
for col in df_squad.columns:
    col_clean = col.strip().replace("(", "").replace(")", "").replace("'", "")
    parts = [p.strip() for p in col_clean.split(",")]
    
    if len(parts) == 2:
        new_col = parts[1]
    else:
        new_col = parts[0]
    
    df_squad = df_squad.rename(columns={col: new_col})

# matches players preprocess:
#  -> Eliminar columnas innecesarias
df_matches_players = df_matches_players.drop(columns=["PlayerLink", "Match Report"])
