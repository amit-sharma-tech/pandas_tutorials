import pandas as pd

titanic = pd.read_csv("./data/titanic.csv")

# print(titanic.head())

air_quality = pd.read_csv("./data/air_quality_long.csv",index_col="date.utc",parse_dates=True)

# print(air_quality.head())

###--I want to sort the Titanic data according to the age of the passengers.

# print(titanic.sort_values(by="Age").head())

##-I want to sort the Titanic data according to the cabin class and age in descending order.

# print(titanic.sort_values(by=["Pclass","Age"],ascending=False).head())


##---Long to wide table format

no2 = air_quality[air_quality["parameter"]=="no2"]
# print(no2)
# print(no2.shape)

no2_subset = no2.sort_index().groupby(["location"]).head(2)
# print(no2_subset)

###--I want the values for the three stations as separate columns next to each other.

no2_subset.pivot(columns="location",values="value").plot()
print(no2_subset)

print(no2.pivot(columns="location", values="value").plot())