import pandas as pd

file = pd.read_csv("./data/air_quality_no2.csv",index_col=0,parse_dates=True)

print(file.head())

# file["london_mg_per_cubic"] = file["station_london"]*1.882

# print(file.head())

file_renamed = file.rename(
    columns={
        "station_antwerp": "BETR801",
        "station_paris": "FR04014",
        "station_london": "London Westminster",
    }
)

print(file_renamed.head())

file_lower_renamed = file_renamed.rename(columns= str.lower)

print(file_lower_renamed.head())