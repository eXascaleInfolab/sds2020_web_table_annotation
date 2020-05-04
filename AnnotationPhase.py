import pickle
import collections
from collections import defaultdict
#from wikidata.client import Client
#client = Client()
import requests
import pandas as pd
import wordcount as wc
import RefinedLookup as RLU
#from nltk.metrics import edit_distance
import Levenshtein
import re
import numpy as np

search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))
#searchWithNumpy = np.load("data/surface/searchNumpy.npy").tolist()

def Annotation(T,Tprime,labelColumn,TPrimeIsAnnotated,TPrimeAnnotation,acceptableTypes,descriptionToken,relations,SFDistanceDict):

	#24. for each row i of T do
	for index, row in T.iterrows():
		results = []
		#print("TPrimeIsAnnotated in AP first before strict",TPrimeIsAnnotated)
		# 25. if isAnnotated(T0:i) then continue;
		if(TPrimeIsAnnotated[index]==True):
			continue
		#26.label <- T:i:labelColumn;
		label = row[labelColumn]

		#27. results <- search_strict(label, acceptableTypes, descriptionTokens);
		results = search_strict(label,acceptableTypes, descriptionToken,SFDistanceDict)
		
		#TEST : Should i sort here? 
		results.sort()
		#TEST Sort end

		#28. if results.size > 0 then
		try:
			print("results from search_strict",results)
			if (len(results) > 0 and results != []):
				#print("Result in Annotation after strict search",results)
				#29. topResult <- results.get(0);
				topResult=results[0]
				#print("TPrimeIsAnnotated in AP res > 0",TPrimeIsAnnotated)
				#30. annotate(T0:i, topResult);
				TPrimeIsAnnotated[index]= True 
				TPrimeAnnotation[index]= topResult
				#31. continue ; /* go to the next row */
				#print("TPrimeIsAnnotated in AP after strict",TPrimeIsAnnotated)
				continue
		except TypeError:
			print("result has non type")
			continue
		#32. for each column j in relations do
		for j in relations:
			#33. r <- relations[j];
			r = relations[j] # r is a attr id e.g. : 'P31'
			#34. results <-  search_loose(label,r,T:i:j);
			results = search_loose(label,r,row[j],SFDistanceDict)
			#35. if results.size > 0 then
			if(len(results) > 0):
				#36. topResult <- results.get(0);
				topResult=results[0]
				#37. annotate(T0:i, topResult);
				print("TPrimeIsAnnotated in AP before loose",TPrimeIsAnnotated)
				TPrimeIsAnnotated[index]= True 
				TPrimeAnnotation[index]= topResult
				print("TPrimeIsAnnotated in AP after loose",TPrimeIsAnnotated)
				#38. break ; /* go to the next row */
				break
	print("From annotation phase: TprimeIsAnnotated: ", TPrimeIsAnnotated)
	print("From annotation phase: TprimeAnnotation : ", TPrimeAnnotation)
	#return(results, TprimeIsAnnotated, TPrimeAnnotation)
	return(TPrimeIsAnnotated, TPrimeAnnotation)

#----------------------------- Functions -------------------------------
#regarding line 34
def search_loose(label,r,Tij,SFDistanceDict):#e.g. r = 'P36', label=Germany, Tij=Berlin
	results = []
	finalresult = []
	print("we are in search_loose, label:",label,"r: ",r, "Tij: ",Tij)
	#TODO : put them all in the cleaning function
	label = label.title()

	#the must be capital as the first letter and small in between
	#remove second names in the paranthesis  
	label = label.lower()
	label = re.sub("\s\s+" , " ", label)
	label = label.split(" (")[0]
	
	try:
		results = search[label]
	except KeyError:
		print("keyword ",label, "not in Surface form")
		try:
			result = SFDistanceDict[label]
		except Exception:
			print("label not in SF distance dict")
			pass
		pass

	for res in results:
		_cf = ContainsFact(res,r,Tij)
		if not(_cf == [] ):
			finalresult.append(_cf) # mikhaim resultS tushun p36 dashte bashe ke mosavi berlin bashe
	print("Final result in search_loose",finalresult,"for the label:",label)
	return(finalresult)
#ye contain facte jadid doros kon age tu attribute 'P36', yechi tu mayehaye(lev. distance) Tij(berlin) peida kardi, un resulte

#regarding line 27
def search_strict(label,acceptableTypes,descriptionTokens,SFDistanceDict):
	print("we are in search_strict")
	results = []
	keyAllTypes = []
	iResultDescriptionToken = ""
	finalresult = []
	print("input param description tokens:",descriptionTokens)
	#TODO : put them all in the cleaning function
        #remove second names in the paranthesis  
	try:
		label = re.sub("\s\s+" , " ", label)
		label = label.split(" (")[0]
		label = label.lower()
	except TypeError:
		print("cant remove spaces- annotation phase")
	
	try:
		results = search[label]
	except KeyError:
		print("keyword ",label, "not in Surface form")
		try:
			results = SFDistanceDict[label]
		except Exception:
			print("label not in SF distance dict")
			pass
		pass

	#Surfaceformdict[label] = surface_forms[label]
	print("results in search strict:(remove this, test)",results)
	for i in results:
		iResultDescriptionToken = RLU.getDescriptionTokens(i)#this label should be ID instead of name
		keyAllTypes = RLU.getTypes(i) #e.g. get all types of UK
		#if the type is wikimedia disambiguation page
		if(all(x in keyAllTypes for x in ['Wikimedia disambiguation page','Wikimedia category'])):
			continue
		else:
			for ac_ty in acceptableTypes:
				if(ac_ty in keyAllTypes): #if all acceptable types exists in label
					for d in descriptionTokens:
						if(d.lower() in iResultDescriptionToken.lower()):
							if i not in finalresult:
								finalresult.append(i)

	print("Final result in search_strict",finalresult, "for the label:",label)
	return(finalresult)
	
#----------------------------- FUNCTIONS ----------------------------

def ContainsFact(rowtopresult, a, v):
	#print("we are in contains fact in annotation phase")
	#print("rowtopresult",rowtopresult,"a: ",a,"v: ",v)

	InnerDict = defaultdict(list)
	finalresult = []
	#url = 'https://query.wikidata.org/sparql'
	url = 'http://localhost:9999/bigdata/namespace/wdq/sparql'
	query = """SELECT ?e ?p ?label
		WHERE {
   		wd:Q"""+str(rowtopresult)+""" ?p ?e.
   		?e rdfs:label ?label.
   		FILTER(LANG(?label) = 'en')
   		SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
 		}
		"""
	r = requests.get(url, params = {'format': 'json', 'query': query})
	data = r.json()

	for item in data['results']['bindings']:
		#Top result description string
		value = item['label']['value'] 
		attribute = item['p']['value'].replace('http://www.wikidata.org/prop/direct/','')
		#missing values of table
		if not(pd.isnull(v)):
			distanceRatio = Levenshtein.ratio(value,v)
		else:
			v=""
			distanceRatio = Levenshtein.ratio(value,v)
		if(attribute==a):
			if(distanceRatio>0.4):#change it to lev. distance
				finalresult = rowtopresult
	return(finalresult)


