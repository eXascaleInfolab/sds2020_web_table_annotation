import numpy as np
import pandas as pd
def getReferenceColumns(T,LabelColumn):

	_tSize = {"_tRowNum": len(T.index), "_tColNum": len(T.columns)}
	_refColumn = []
	#if the column is not labelColumn, it is reference
	for col in range(0,_tSize['_tColNum']):
		columnLabelName =(T.keys())[col] #e.g.: country, population,...
		if not(columnLabelName == LabelColumn):
			_refColumn.append(columnLabelName)
	return _refColumn
