import pandas as pd
df = pd.DataFrame({
    "Name":[
        "Braund, Mr. Owen Harris",
        "Allen, Mr. William Henry",
        "Bonnell, Miss. Elizabeth",
    ],
    "Age":[22,35,58],
    "Sex":["Male","Male","Female"]
})

# print(df)

# print(df['Age'])

ages = pd.Series([22,56,89],name="Age")

# print(ages.max())
# print(ages)

##--I want to know the maximum Age of the passengers

print(df["Age"].max())

###...I’m interested in some basic statistics of the numerical data of my data table
print(ages.describe())
print(ages.mean())