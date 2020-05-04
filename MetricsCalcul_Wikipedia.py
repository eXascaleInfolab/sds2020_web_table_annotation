#Calculate precision and recall and F1
import sklearn.metrics as metrics
import numpy as np
import csv
import json
import os
import glob
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer

def MetricsCalcul(T,Table_CSV_Name,Table_Annotation,Table_IsAnnotated):

	mapped_Prediction_Dict = dict()
	table_csv = Table_CSV_Name
	#change the path to not to have the path in the file name
	os.chdir('data/Wikipedia/entities_instance/csv/')
	#exact csv file name
	allCsvEntityFiles = glob.glob('*.csv')


	for entity_csv in allCsvEntityFiles:
		if(entity_csv == table_csv):
			with open(entity_csv, 'r',encoding='utf-8') as csvEntityFile:
				print("This is the Entity file name :\n\n",entity_csv)
				try:
					E = pd.read_csv(entity_csv, index_col=False, header=None)
				except pd.errors.ParserError:
					#variant column numbers in different rows
					print("variant numexit in Table handled!")
					with open(entity_csv, 'r') as f:
						for line in f:
							E = pd.concat( [E, pd.DataFrame([tuple(line.strip().split(','))])], ignore_index=True )        
					
				#sort entity in the same order of table.
				E = E.sort_values(E.columns[-1])
				E[0] = [link.replace('://dbpedia.org/page/','://dbpedia.org/resource/',1) for link in E[0]]
				#print(E)
				#print(T)
				break
	#calculate metrics - precision, recall, F1
	
	#Find wikidata link from the annotation results. 
	for k in Table_Annotation:
		Table_Annotation[k] = "http://www.wikidata.org/entity/Q"+str(Table_Annotation[k])
		#print(Table_Annotation[k])

		#look in mapping file, for the corresponding dbpedia link
		with open('/home/yasamin/Codes/WebTableAnnotation/data/Wikipedia/DBpedia2Wikidata_Wikipedia.csv', 'r',encoding='utf-8') as csvFile:
			csv_reader = csv.reader(csvFile, delimiter=',')
			for row in csv_reader:
				try:
					if(Table_Annotation[k] == row[1]):
						#print("wikidata:", Table_Annotation[k], "DBpedia: ",row[1])
						#this is the same as result, but with dbpedia links instead of wikipedia codes
						mapped_Prediction_Dict[k] = row[0]
				except IndexError:
					print("Oops! Index Error in Mapping file.")

	#to decide later if they are wrong annotation or not annotitons (FN or FP)
	#need to keep the same index in prediction and GT

	for index, row in T.iterrows():
		if not(index in mapped_Prediction_Dict):
			print(row)
			print("the index i:",index, "(start from 0) is not in the mapping file.")
			mapped_Prediction_Dict[index] = "to be decided"
			print("the index not exist",index)			

	#calculating the metrics:
	
	#-------------------------Test--------------------------
	#make both the same type: numpy array

	#TODO : make this Test part efficient. forced to write like this
	#because of different indexes types in entity files and tables.
	#and also because some types were not itterable!

	y_true = E[0].values
	y_pred = mapped_Prediction_Dict.values()

	final_true = np.array([])
	final_pred = np.array([])


	for vt in y_true:
		final_true = np.append(final_true, vt)
	
	for vp in y_pred:
		final_pred = np.append(final_pred, vp)
	
	dict_final_true = {}
	dict_final_pred = {}

	my_counter = 0

	for index, row in T.iterrows():
		dict_final_true[index] = final_true[my_counter]
		dict_final_pred[index] = final_pred[my_counter]
		my_counter = my_counter + 1
		

	#print("final true:",dict_final_true)
	#print("final pred:",dict_final_pred)

	#-------------------------Test--------------------------
	

	#------------------------------------------
	#in case of wrong annotation/no annotation:
        #possible that we have a link(exist in mapping) but it is wrong and was for another row.so we put elif
        #number of rows in T
	count_row = T.shape[0]
	#count_row = final_true.size
	FN = 0
	FP = 0
	TP = 0

	for index, row in T.iterrows():
		try:
			print("dict_final_true of ", [index], "is: ",dict_final_true[index])
			print("dict_final_pred of ", [index], "is: ",dict_final_pred[index])
			#no annotation
			if(Table_IsAnnotated[index] == False):
				FN = FN + 1
			#link not exist in mapping, so the link is false
			elif not(index in mapped_Prediction_Dict):
				#mapped_Prediction_Dict[i] = "Wrong annotation"
				FP = FP + 1
				print("the index not exist",index)
			#link exist in mapping, but not the same as GT
			elif not(dict_final_true[index]==dict_final_pred[index]):
				FP = FP + 1
			elif(dict_final_true[index]==dict_final_pred[index]):
				print("they are equal : ", index)
				TP = TP + 1
			else:
				print("Dict Final true of",index,"is",dict_final_true[index])
				print("Table annotation of",index,"is",Table_Annotation[index])
		except IndexError:
			print("Index error in Metrics calculation")
			continue
		except KeyError:
			print("Key in table not exist in Metrics calculation:",i)
			continue
	# if(TP == 0):
	# 	print("Table had no header, Solved in the second row!")
	# 	#Todo : find a clean solution for tables without header!
	# 	#quick fix for table without header- we lose one row!
	# 	FN = 0
	# 	FP = 0
	# 	TP = 0
		
	# 	for index, row in T.iterrows():
	# 		try:
	# 			print("dict_final_true of ", [index+1], "is: ",dict_final_true[index+1])
	# 			print("dict_final_pred of ", [index], "is: ",dict_final_pred[index])
	# 			#no annotation
	# 			if(Table_IsAnnotated[index] == False):
	# 				FN = FN + 1
	# 			#link not exist in mapping, so the link is false
	# 			elif not(index in mapped_Prediction_Dict):
	# 				#mapped_Prediction_Dict[i] = "Wrong annotation"
	# 				FP = FP + 1
	# 				print("the index not exist",index)
	# 			#link exist in mapping, but not the same as GT
	# 			elif not(dict_final_true[index+1]==dict_final_pred[index]):
	# 				FP = FP + 1
	# 			elif(dict_final_true[index+1]==dict_final_pred[index]):
	# 				print("they are equal : ", index)
	# 				TP = TP + 1
	# 			else:
	# 				print("Dict Final true of",index+1,"is",dict_final_true[index+1])
	# 				print("Table annotation of",index,"is",Table_Annotation[index])
	# 		except IndexError:
	# 			print("Index error in Metrics calculation")
	# 			continue
	# 		except KeyError:
	# 			print("Key in table not exist in Metrics calculation:",index)
	# 			continue
	print("Information for Table File Name:",Table_CSV_Name)
	print("FN ",FN)
	print("FP ",FP)
	print("TP ",TP)
	#------------------------------------------
	#binarizer = MultiLabelBinarizer()
	#binarizer.fit(final_true)

	
	#p, r, f, s = metrics.precision_recall_fscore_support(binarizer.transform(final_true),binarizer.transform(final_pred), average=None)

	#break #to read 1 table
	
	#csvFile.close()

	#back to normal path
	os.chdir('/home/yasamin/Codes/WebTableAnnotation/')

	return(TP, FN, FP)
