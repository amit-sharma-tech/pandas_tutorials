import pandas as pd

titanic = pd.read_csv("./data/titanic.csv")

# print(titanic.head())
air_quality = pd.read_csv("./data/air_quality_long.csv",index_col="date.utc",parse_dates=True)

# print(air_quality)

##.I want to sort the Titanic data according to the age of the passengers.

# print(titanic.sort_values(by=["Age","Name"]).head())

###>I want to sort the Titanic data according to the cabin class and age in descending order.

print(titanic.sort_values(by=["Pclass","Age"],ascending=False).head())