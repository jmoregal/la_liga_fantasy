import pandas as pd
from bs4 import BeautifulSoup, Comment

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

print(f"✅ Se han encontrado {len(dataframes)} tablas en total")

# Ejemplo: mostrar los nombres de las columnas de cada tabla
# for i, df in enumerate(dataframes, 1):
#     print(f"\n--- Tabla {i} ---")
#     print(df.head())


