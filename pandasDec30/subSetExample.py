import pandas as pd

titanic = pd.read_csv("./data/titanic.csv")

# print(titanic.head())

age = titanic["Age"]

# print(age)

# print(type(age))

# print(type(titanic["Age"]))

# print(titanic['Age'].shape)

# age_sex = titanic[["Age","Sex"]]

# print(age_sex)
# print(type(age_sex))
# print(age_sex.shape)

###.How do I filter specific rows from a DataFrame?

# above_age_35 = titanic[titanic["Age"] > 35]
# above_age_35 = titanic["Age"] > 35
# above_age_35 = titanic[age > 35]
# print(above_age_35,type(above_age_35),above_age_35.shape)


##..I’m interested in the Titanic passengers from cabin class 2 and 3.

# cabin_class = titanic[titanic["Pclass"].isin([2,3])]
# cabin_class = titanic[(titanic["Pclass"] == 2) | (titanic["Pclass"] ==3)]
# print(cabin_class)

###>.I want to work with passenger data for which the age is known.
# age_no_na = titanic[titanic["Age"].isna()]
# print(age_no_na)
# print(age_no_na.shape)

##..I’m interested in the names of the passengers older than 35 years.

# adult_name = titanic.loc[titanic["Age"] > 35,["Name","Pclass","Sex"]]

# print(adult_name)

##..I’m interested in rows 10 till 25 and columns 3 to 5.

print(titanic.iloc[9:25,2:5])