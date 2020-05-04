import pandas as pd
import pickle
#import Levenshtein
import numpy as np
import html
import string
from collections import defaultdict
#what i want? 
#make a key, lower case
#remove punctuations from the surface forms

#problems: 
#repititive values in SF, some values are not there!
def SearchWithDistanceSF():
	SFDict = defaultdict(list)
	#T = pd.read_csv("./data/T2D/t2d_tables_instance/1146722_1_7558140036342906956.csv") 
	#LabelColumn ="peak"
	search = pickle.load(open("data/surface/surfaceForms-20180820.pickle", "rb"))
	for k in search:
		#replace html characters reference and remove punctuations in tables
		prev_key = k
		#replace html
		k = [html.unescape(elem) for elem in k]
		#remove punctuations
		k = "".join([ch for ch in k if ch not in string.punctuation])
		if not(k == "" and search[prev_key] == []):
			k = k.lower()
			if(k in SFDict):
				for v in search[prev_key]:
					SFDict[k].append(v)	
			else:
				SFDict[k] = search[prev_key]
		

	with open('data/surface/Surface_Lower_NoPunc.pickle', 'wb') as handle:
		pickle.dump(SFDict, handle)			
	


	#np.save("data/surface/searchNumpy.npy",search)
	#searchWithNumpy = np.load("data/surface/searchNumpy.npy").tolist()
	# for i in search:
	# 	for t in T['peak']:
	# 		#distance ratio for same strings = 1.0
	# 		similarityRatio = Levenshtein.ratio(i,t)
	# 		if(similarityRatio>0.8):
	# 			SFDict[t] = search[i] 
	# 			continue
	# print(SFDict)
	return("hey")

SearchWithDistanceSF()
