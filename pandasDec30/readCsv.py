import pandas as pd
import os

def create_folder(name,directory):
    os.chdir(directory)
    filename = os.listdir()
    if name in filename:
        print(f'Folder "{name}" exists')
        return False
    else:
        os.mkdir(name)
        return True
titanic = pd.read_csv("./data/titanic.csv")

# print(titanic)


##..I want to see the first 8 rows of a pandas DataFrame.

# print(titanic.head(8))

##. tail method to show last data

print(titanic.tail(10))

##.To show datatype of pandas

print(titanic.dtypes)
folderExist = create_folder("result","./")
# print(folderExist)
# breakpoint 
if folderExist == True:
    titanic.to_excel("./result/titanic.xlsx",sheet_name="passengers",index=False)
else:
    print("not file write call function")