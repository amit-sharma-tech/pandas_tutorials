import pandas as pd

air_quality2 = pd.read_csv("./data/air_quality_long.csv",parse_dates=True)

air_quality2 = air_quality2[["date.utc","location","parameter","value"]]

# print(air_quality2.head())

##another table

air_quality25 = pd.read_csv("./data/air_quality_long.csv",parse_dates=True)

air_quality25 = air_quality25[["date.utc","location","parameter","value"]]

# print(air_quality25.head())

###--Concatenating objects

# air_quality = pd.concat([air_quality25,air_quality2],axis=0)
# print(air_quality.head(10))

# print('Shape of the ``air_quality_25``table:',air_quality25.shape)


air_quality = air_quality25.sort_values("date.utc")
print(air_quality.head())