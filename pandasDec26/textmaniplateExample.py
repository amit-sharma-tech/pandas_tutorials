import pandas as pd

titanic = pd.read_csv("./data/titanic.csv")


print(titanic.head())

###-manuplate text

print(titanic["Name"].str.lower())

print(titanic["Name"].str.split(","))