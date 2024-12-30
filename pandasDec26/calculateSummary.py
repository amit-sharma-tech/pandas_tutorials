import pandas as pd

fileName = pd.read_csv("./data/titanic.csv")

print(fileName.head())

###--What is the average age of the Titanic passengers?

# print(fileName["Age"].mean())

###--What is the median age and ticket fare price of the Titanic passengers?

# print(fileName[["Age","Fare"]].median())

# print(fileName[["Age","Fare"]].describe())

##apply dataframe aggregate function

# print(fileName.agg(
#     {
#         "Age":["min","max","median","skew"],
#         "Fare":["min","max","median","mean"]
#     }
# ))

##---What is the average age for male versus female Titanic passengers?

# print(fileName[["Age","Sex"]].groupby("Sex").mean())


# print(fileName.groupby("Sex").mean(numeric_only=True))

# print(fileName.groupby("Sex")["Age"].mean())

##---What is the mean ticket fare price for each of the sex and cabin class combinations?

# print(fileName.groupby(["Sex","Pclass"])["Fare"].mean())

##--What is the number of passengers in each of the cabin classes?

print(fileName["Pclass"].value_counts())