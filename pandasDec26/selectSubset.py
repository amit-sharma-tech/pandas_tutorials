import pandas as pd

file = pd.read_csv('./data/titanic.csv')

# print(file)

age = file['Age']

# print(age.head(20))
# print(type(age))

# print(age.shape)

# age_sex = file[["Age","Sex"]]

# print(age_sex)

# print(type(age_sex))

# print(age_sex.shape)

# above_35 = file[file["Age"] >35]

# print(above_35)

# print(above_35.head())
# only_age = file["Age"] > 35
# print(only_age) 

# print(above_35.shape)

##filtering row

# class23 = file[(file["Pclass"] ==2) | (file["Pclass"] ==3)]
# print(class23.head())
# print(class23.shape)

# class_23 = file[file["Pclass"].isin([2,3])]

# print(class_23.head())
# print(class_23.shape)

# age_no_na = file[file["Age"].notna()]

# print(age_no_na.head())
# print(age_no_na.shape)

#----------I’m interested in the names of the passengers older than 35 years.
# adult_name = file.loc[file["Age"] > 35,"Name"]

# print(adult_name.head())
# print(adult_name.shape)

#------I’m interested in rows 10 till 25 and columns 3 to 5.

# file_row = file.iloc[9:25,2:5]

# print(file_row.head)
# print(file_row.shape)

file.iloc[0:3, 3] = "anonymous"

print(file.head())