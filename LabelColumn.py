import numpy as np
import pandas as pd
from numbers import Number

#to check if all the prefixes and suffixes are the same for label column
# puting 4, for typos
def checkEqual(iterator):
   return len(set(iterator)) <= 1

def most_common(lst):
    return max(set(lst), key=lst.count)

def shortLength(iterator):
	try:
		return all(len(x) <= 3 for x in iterator)
	except TypeError:
		return False

def getLabelColumn(T):
	# T = pd.DataFrame({'Country': ['China', 'India', 'United States', 'Indonesia', 'Brazil'],
	# 	'Population': [1377516162, 1291999508, 323990000, 258705000, 206162929],
	# 	'Capital': ['Beijing','New Delhi','Washington, D.C.','Jakarta','Brasilia']})
	

	#number of rows and columns in T
	_tSize = {"_tRowNum": len(T.index), "_tColNum": len(T.columns)}


	#key is column label, value number of duplicate rows for this key
	duplicateCountDict = {}
	
	#for each column, how many rows have duplicate values
	for col in range(0,_tSize['_tColNum']):
		columnLabelName =(T.keys())[col] #e.g.: country, population,...
		duplicateCountDict[columnLabelName] = 0
		for row in range(0,_tSize['_tRowNum']):
			try:
				if(T.duplicated(columnLabelName)[row]):
					duplicateCountDict[columnLabelName] +=1
			except KeyError:
				print("row key does not exist in Table ")
				continue

	LabelColumn = "LabelColumn"
	duplicateCountDict[LabelColumn] = _tSize['_tRowNum']

	Priority_LabelColumn_Candidates = T.keys()

	for col in range(0,_tSize['_tColNum']):
		print("the col now is" , T.keys()[col],"with the number" , col)

		labelcolumn_prefixes = []
		labelcolumn_suffixes = []

		for r in T[T.keys()[col]]:
			try:
				suffix = r.rsplit(' ', 1)[1]
				prefix = r.split(' ', 1)[0]

				labelcolumn_suffixes.append(suffix)
				labelcolumn_prefixes.append(prefix)
			except Exception:
				labelcolumn_suffixes.append(r)
				labelcolumn_prefixes.append(r)
				pass
		if not(shortLength(T[T.keys()[col]])):
			if(checkEqual(labelcolumn_suffixes) != True and checkEqual(labelcolumn_prefixes) != True):
				print("duplicateCountDict:",duplicateCountDict)
				print("duplicateCountDict[(T.keys())[col]]",duplicateCountDict[(T.keys())[col]])
				print("duplicateCountDict[LabelColumn]",duplicateCountDict[LabelColumn])
				if(duplicateCountDict[(T.keys())[col]] < duplicateCountDict[LabelColumn]):
					#col is more on the left in each loop
					LabelColumn = T.keys()[col]

		else:
			LabelColumn = LabelColumn

	#try to choose LC with less constrains !
	if(LabelColumn == 'LabelColumn'):
		for col in range(0,_tSize['_tColNum']):
			for r in T[T.keys()[col]]:
				if(duplicateCountDict[(T.keys())[col]] < duplicateCountDict[LabelColumn]):
					LabelColumn = T.keys()[col]
				else:
					LabelColumn = T.keys()[0]
	print("Label Column:", LabelColumn)

	return LabelColumn
