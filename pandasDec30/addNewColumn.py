import pandas as pd

air_quality = pd.read_csv("./data/air_quality_no2.csv",index_col=0,parse_dates=True)

# print(air_quality.head()) 

##.How to create new columns derived from existing columns

# air_quality['london_mg_per_cubic'] = air_quality['station_london'] * 1.882

# print(air_quality.head())

###>I want to rename the data columns to the corresponding station identifiers used by

# air_quality_remnamed = air_quality.rename(columns={
#     "station_antwerp": "BETR801",
#     "station_paris": "FR04014",
#     "station_london": "London Westminster",
# })

# print(air_quality_remnamed)

air_quality_reanmed_all = air_quality.rename(columns=str.upper)
print(air_quality_reanmed_all)