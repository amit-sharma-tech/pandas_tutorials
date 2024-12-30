import pandas as pd

titanic = pd.read_excel("./data/titanic.xlsx",sheet_name="complete",index_col=False)
# print(titanic)

##..I’m interested in a technical summary of a DataFrame

print(titanic.info())