import ast
import pandas as pd

#Transofrm csv into a pandas dataframe
df = pd.read_csv("../data/raw/understat_xg_data.csv")

# Convert stringified dicts into real dicts
for col in ["h", "a", "goals", "xG", "forecast"]:
    df[col] = df[col].apply(ast.literal_eval)

df["home_team"] = df["h"].apply(lambda x: x["title"])
df["away_team"] = df["a"].apply(lambda x: x["title"])

df["home_goals"] = df["goals"].apply(lambda x: int(x["h"]))
df["away_goals"] = df["goals"].apply(lambda x: int(x["a"]))

df["home_xg"] = df["xG"].apply(lambda x: float(x["h"]))
df["away_xg"] = df["xG"].apply(lambda x: float(x["a"]))

df["date"] = pd.to_datetime(df["datetime"]).dt.date
df["season"] = df["season"].astype(str)

df_clean = df[[
    "date", "home_team", "away_team",
    "home_goals", "away_goals",
    "home_xg", "away_xg",
    "league", "season"
]]

#df_clean.to_csv("../data/raw/understat_xg_data_clean.csv", index=False)