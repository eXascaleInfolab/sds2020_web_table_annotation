# -*- coding: utf-8 -*-
import re
import numpy as np
import pandas as pd
import string
import html
import LabelColumn as LC
import ReferenceColumn as RC

import RefinedLookup as RLU
# import RefinedLookup_proposed as RLU

import AnnotationPhase as AP
import MetricsCalcul as MCal
def getFullPhase(T,Table_CSV_Name):

	#replace html characters reference in tables
	for col in T:
		try:
			T[col] = [html.unescape(elem) for elem in T[col]]
			#remove first spaces
			T[col] = [re.sub(r"^\s+", "", elem) for elem in T[col]]
			#remove last spaces
			T[col] = [re.sub(r"\s+$", "", elem) for elem in T[col]]
		except TypeError:
			print("Type not suitable for iteration")
			continue

	#remove punctuations and html characters reference from tables
	for col in T:
		#print("This is T[col] before punc removing", T[col])
		Keep_Column = []
		for row in T[col]:
			try:
				row = "".join([ch for ch in row if ch not in string.punctuation])
				Keep_Column.append(row)
			except TypeError:
				print("cannot remove punctuations")
				Keep_Column.append(row)
				continue
		T[col] = Keep_Column
		#print("This is T[col] after punc removing", T[col] )
		


	#removing numeric datatypes
	T = T.select_dtypes(exclude=['int_','float_','complex_'])

	for col in T:
		try:
			#remove if it is only digits (no letter or other inputs)
			T[col] = [re.sub('^[0-9 ]+$','',elem) for elem in T[col]]
			#T[col] = T[col].str.replace('\d+', '')
		except TypeError:
			print("cannot replace numeric values")
			continue


	#1.T' <- T;
	TPrime = T
	#----------------------------------Sample phase----------------------------------------

	#4.labelColumn  <- getLabelColumn(T);
	labelColumn = LC.getLabelColumn(T)
	print("-" * 50)
	print(" " * 50)
	print("-" * 50)
	
	#5.referenceColumns <- getReferenceColumns(T);
	referenceColumns = RC.getReferenceColumns(T, labelColumn)

	Tprime,TPrimeIsAnnotated,TPrimeAnnotation,acceptableTypes,descriptionToken,relations,SFDistanceDict = RLU.RefinedLookup(T, TPrime, labelColumn, referenceColumns)

	TprimeIsAnnotated, TPrimeAnnotation = AP.Annotation(T,Tprime,labelColumn,TPrimeIsAnnotated,TPrimeAnnotation,acceptableTypes,descriptionToken,relations,SFDistanceDict)

	print("Full phase",TprimeIsAnnotated,TPrimeAnnotation)
	return(TprimeIsAnnotated, TPrimeAnnotation)