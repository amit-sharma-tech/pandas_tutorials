import pandas as pd

titanic = pd.read_csv("./data/titanic.csv")


# print(titanic.head())


##..How to calculate summary statistics

# print(titanic.describe())

# print(titanic["Age"].mean())

# print(titanic[["Age","Fare"]].median())

# print(titanic.agg({
#     "Age" : ["min","max","mean","skew"],
#     "Fare":["min","max","median","mean"]
# }))

##..What is the average age for male versus female Titanic passengers?


print(titanic[["Age","Sex"]].groupby("Sex").mean())

