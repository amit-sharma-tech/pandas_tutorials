import pandas as pd

df = pd.DataFrame({
    "name" :[
        "Braund, Mr. Owen Harris",
        "Allen, Mr. William Henry",
        "Bonnell, Miss. Elizabeth",
    ],
    "Age":[22,35,58],
    "Sex":["male",'male',"female"]
})

print(df)
print(df['Age'])

age = pd.Series([22,31,58],name="Age")
print(age)

print(df['Age'].max())

print(df.describe())