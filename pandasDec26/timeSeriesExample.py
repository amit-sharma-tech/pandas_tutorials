import pandas as pd
import matplotlib.pyplot as plt

# air_quality = pd.read_csv("./data/air_quality_long.csv")

# # print(air_quality)

# air_quality = air_quality.rename(columns={"date.utc":"datetime"})

# # print(air_quality)

# # print(air_quality.city.unique())
# # print(air_quality['city'].unique())


# ##---How to handle time series data with ease

# air_quality["datetime"] = pd.to_datetime(air_quality["datetime"])
# print(air_quality["datetime"])


air_quality = pd.read_csv("./data/air_quality_no2.csv",parse_dates=["datetime"])
# print(air_quality)

# print(air_quality["datetime"].min(), air_quality["datetime"].max())

# print(air_quality["datetime"].max() - air_quality["datetime"].min())

###--I want to add a new column to the DataFrame containing only the month of the measurement

air_quality['month'] = air_quality["datetime"].dt.month
print(air_quality)

print(air_quality.groupby(
    [air_quality["datetime"].dt.weekday, "location"])["value"].mean())