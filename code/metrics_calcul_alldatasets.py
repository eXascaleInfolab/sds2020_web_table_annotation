import sklearn.metrics as metrics
import numpy as np
import csv
import json
import os
import glob
import pandas as pd
import operator
from collections import OrderedDict
from  decouple import config, Csv

T2D_DIR = config('T2D_DIR', cast=str)
BASE_DIR = config('BASE_DIR', cast=str)
DATASET_NAME = config('DATASET_NAME', cast=str)
LIMAYE_DIR = config('LIMAYE_DIR', cast=str)

def metrics_calcul(T, table_csv, Table_Annotation, Table_IsAnnotated):
    
    mapped_Prediction_Dict = dict()


    #set working directory to ds
    if DATASET_NAME == 'Limaye':
        os.chdir(LIMAYE_DIR + 'entities_instance/')
    elif DATASET_NAME == 'T2D':
        os.chdir(T2D_DIR +'Final_InstanceLevel_GoldStandard/entities_instance(Links of table in DBPedia)/')
    
    #exact csv file name
    allCsvEntityFiles = glob.glob('*.csv')
    
    for entity_csv in allCsvEntityFiles:
        if(entity_csv == table_csv):
            with open(entity_csv, 'r',encoding='utf-8'):
                print("This is the Entity file name :\n\n",entity_csv)
                E = pd.read_csv(entity_csv, header=None)
                #sort entity in the same order of table.
                E = E.sort_values(E.columns[-1])
                E[0] = [link.replace('://dbpedia.org/page/','://dbpedia.org/resource/',1) for link in E[0]]
                break
    #calculate metrics - precision, recall, F1

    #Find wikidata link from the annotation results. 
    for k in Table_Annotation:
        Table_Annotation[k] = "http://www.wikidata.org/entity/Q"+str(Table_Annotation[k])

        #look in mapping file, for the corresponding dbpedia link
        if DATASET_NAME == 'Limaye':
            dir = LIMAYE_DIR + 'DBpedia2Wikidata_Limaye.csv' 
        elif DATASET_NAME == 'T2D':
            dir = T2D_DIR + 'DBpedia2Wikidata_T2D.csv'

        with open(dir, 'r',encoding='utf-8') as csvFile:
            csv_reader = csv.reader(csvFile, delimiter=',')
            for row in csv_reader:
                try:
                    if(Table_Annotation[k] == row[1]):
                        #this is the same  result, but with dbpedia links instead of wikipedia
                        mapped_Prediction_Dict[k] = row[0]
                except IndexError:
                    print("Oops! Index Error in Mapping file.")
	
    # keep the same index in prediction and GT
    for index, row in T.iterrows():
        if not(index in mapped_Prediction_Dict):
            print("the index i:",index, "(start from 0) is not in the mapping file.")
            mapped_Prediction_Dict[index] = "to be decided"
            print("the index not exist",index)	


    #because of different format of indexes types in entity files and tables.

    y_true = E[0].values
    #sort annotation results by key
    mapped_Prediction_Dict = OrderedDict(sorted(mapped_Prediction_Dict.items()))
    y_pred = mapped_Prediction_Dict.values()
    print("y_pred:",y_pred)

    final_true = np.array([])
    final_pred = np.array([])

    for vt in y_true:
        final_true = np.append(final_true, vt)
	
    for vp in y_pred:
        final_pred = np.append(final_pred, vp)
    dict_final_true = {}
    dict_final_pred = {}

    my_counter = 0
    count_row = T.shape[0]
    FN = 0
    FP = 0
    TP = 0
    for index, row in T.iterrows():
        dict_final_true[index] = final_true[my_counter]
        dict_final_pred[index] = final_pred[my_counter]
        my_counter = my_counter + 1

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
            print("Key in table not exist in Metrics calculation:",index)
            continue

    print("Information for Table File Name:",table_csv)
    print("FN ",FN)
    print("FP ",FP)
    print("TP ",TP)
    
    #back to normal path
    os.chdir(BASE_DIR)

    return(TP, FN, FP)