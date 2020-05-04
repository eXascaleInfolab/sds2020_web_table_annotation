#Pandas indexing cheatsheet
#check also:
#https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html

import pandas as pd

T = pd.DataFrame({'Country': ['Dominica','Tajikistan','Djibouti','Gabon'],
	           'Population': [1000,2000,3000,4000],
	           'Capital': ['Roseau','Dushanbe','Djibouti','Libreville']})


for i in range(0, len(T)):
    for col in T:
        print("Col:",col) # name of the column headers!
        
        #nemishe in asan!
        #print("T.iloc[i][col]:",T.iloc[i][col])

        print('T.loc[i]',T.loc[i]) #access to column value by index
        print('T.iloc[i]',T.iloc[i]) 
        #these two have the same value!

        # x.iloc[1] = {'x': 9, 'y': 99} # u can define column names

        # loc gets rows (or columns) with particular labels from the index.
        # iloc gets rows (or columns) at particular positions in the index (so it only takes integers).
        # ix usually tries to behave like loc but falls back to behaving like iloc if a label is not present in the index.


        print("i:",i) #index for sure!

        print('T.iloc[i][colname]',T.iloc[i][col])         
