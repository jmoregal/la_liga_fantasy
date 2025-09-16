import pandas as pd
import requests
from bs4 import BeautifulSoup

url = "https://fbref.com/en/comps/12/stats/La-Liga-Stats"
html = requests.get(url).text
print(html[:5000])  # ver los primeros 500 caracteres del HTML
with open("LaLiga_FBref.txt", "w", encoding="utf-8") as f:
    f.write(html)
soup = BeautifulSoup(html, "lxml")

for table in soup.find_all("table"):
    print(table.get("id"))  # ver los ids
    df = pd.read_html(str(table))[0]
    print(df.shape)
