# importing pandas package 
import pandas

# making data frame from csv file
df = pandas.read_csv('All_By_Precinct_2018_General.csv', encoding= 'unicode_escape', low_memory=False, index_col=[0])

# replacing blank spaces with 
df.columns =[column.replace(" ", "_") for column in df.columns]

# filtering with query method
df.query(' Office_Name == "Representative in Congress" and Cong == 2 ', inplace = True)
df.columns =[column.replace("_", " ") for column in df.columns]
df.to_csv('All_By_Precinct_2018_General_ROC2.csv')

print(df)

