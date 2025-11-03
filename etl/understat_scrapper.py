from understat import Understat
import aiohttp
import asyncio
import nest_asyncio
import pandas as pd

nest_asyncio.apply()

async def get_understat_data():
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        leagues = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]
        seasons = list(range(2017, 2025))
        all_data = []

        for league in leagues:
            for season in seasons:
                try:
                    matches = await understat.get_league_results(league, season)
                    df = pd.DataFrame(matches)
                    df["league"] = league
                    df["season"] = season
                    all_data.append(df)
                    print(f"✅ {league} {season}: {len(df)} matches")
                except Exception as e:
                    print(f"⚠️ {league} {season}: {e}")

        return pd.concat(all_data, ignore_index=True)

data = asyncio.run(get_understat_data())
data.to_csv("../data/raw/understat_xg_data.csv", index=False)
print("💾 Saved Understat data!")