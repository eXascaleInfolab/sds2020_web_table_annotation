import pickle
import collections
from collections import defaultdict
#from wikidata.client import Client
#client = Client()
import requests
import pandas as pd
import wordcount as wc
import re
import Levenshtein
import numpy as np
import os
#import SearchWithDistanceSF as search

#ToDo : put searching for the label in SF in a funciton called search!
def RefinedLookup(T, Tprime, labelColumn, referenceColumns):
	search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))
	#searchWithNumpy = np.load("data/surface/searchNumpy.npy").tolist()

	Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein.pickle", "rb"))    
	#Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein_allcols.pickle", "rb")) 
	   
	#keeps surface form with levenshtein distance
	SFDistanceDict = {}

	TPrimeIsAnnotated = {} 
	TPrimeAnnotation = {}
	
	results = []

	allTypes = []

	descriptionTokens = ""

	attributes = []

	candidateRelations = defaultdict(list)

	acceptableTypes = set()

	relations = {}
	
	#6.for each row i of T do
	for index, row in T.iterrows():
		#print("TPrimeIsAnnotated in RLU first",TPrimeIsAnnotated)
		TPrimeIsAnnotated[index]=False

		#print("TPrimeIsAnnotated in RLU after putting false",TPrimeIsAnnotated)
		#7.label <- T:i:labelColumn;
		try:
			label = row[labelColumn]
		except IndexError:
			print("row label column not exists- RLU")
		
		#TODO : put them all in the cleaning function
		#TODO : Check in the begining if there is nan value and convert to string ""
		#8.results <-  search(label);
		#remove second names in the paranthesis  
		if(pd.isnull(label)):
			label = ""
		try:	
			label = label.lower()
			label = re.sub("\s\s+" , " ", label)
			label = label.split(" (")[0]
		except Exception:
			continue

		try:
			results = search[label]

			#if we come here, it means there is something in result
			#if not, we had the exception.
			for r in results:
				r_type = getTypes(r)
				if(all(x in r_type for x in ['Wikimedia disambiguation page'])):
					results.remove(r)
				if(all(x in r_type for x in ['Wikimedia category'])):
					results.remove(r)
				#if the only result was disambiguation page	and removed:
			if(len(results)==0):
				raise KeyError('we had just Wikimedia disambiguation pages. now result is zero.')

			#results= search.SearchWithDistanceSF(label)
		except KeyError:
			print("keyword ",label, "not in Surface form")
			#put the key in the dict to check it again when levenshtein 
			#when u know the acceptable types.
			
			try:
				#uncomment to create new levenshtein
				#--------------------------------------
				# levenshtein_candidate = []
				# for i in search:
				# 	similarityRatio = Levenshtein.ratio(i,label)
				# 	if(similarityRatio>0.80):
				# 		if(i in levenshtein_candidate):
				# 			for j in search[i]:
				# 				levenshtein_candidate.append(j)
				# 		else:
				# 			levenshtein_candidate = search[i]
				# levenshtein_candidate.sort()
				# if not(levenshtein_candidate == []):
				# 	results = levenshtein_candidate[0]
				# 	SFDistanceDict[label] = levenshtein_candidate[0]
				# 	print("solved with levenshtein")
				# 	print("label:",label)
				# 	print("solved result",results)
				#--------------------------------------
				#use saved levenshtein:
				results = Levenshtein_T2D[label]
				SFDistanceDict[label] = Levenshtein_T2D[label]
				print("Solved with levenshtein")
				print("label:",label)
				print("Solved result:",results)
			except (IndexError,KeyError) as err:
				print("no result from levenshtein on SF")
				continue
			continue

		#remove wikimedia disambiguation after the levenshtein
		for r in results:
			r_type = getTypes(r)
			if(all(x in r_type for x in ['Wikimedia disambiguation page'])):
				results.remove(r)
			if(all(x in r_type for x in ['Wikimedia category'])):
				results.remove(r)
		# 9. if results.size > 0 then
		print("This is the results array for label:",label,":\n",results)

		if (len(results) > 0):
			# 10.topResult <- results.get(0);
			#TEST : Should i sort here? 
			try:
				results.sort()
			except Exception:
				print("Cant sort results - RLU")
			#TEST Sort end

			topResult = results[0]
			
			# 11. allTypes.addAll(topResult.getTypes());
			for t in getTypes(topResult):
				if not(t == 'Wikimedia disambiguation page' or t == 'Wikimedia category'):
					allTypes.append(t) #e.g. for UK: 7887906

			#print("all types from topResult", allTypes)
			# 12. tokens <- topResult.getDescriptionTokens();
			tokens = getDescriptionTokens(topResult)
			
			# 13. descriptionTokens.addAll(tokens);
			descriptionTokens += " "+tokens 

			# 14. if results.size = 1 then
			if(len(results) == 1 and results != []):
				# 15. annotate(TPrime.i, topResult);
				TPrimeIsAnnotated[index]= True 
				TPrimeAnnotation[index]= topResult
				#16. for each column j of referenceColumns do
				for j in referenceColumns:
					#17. v <- T.i.j;
					v = row[j]

					# 18. if topResult.containsFact(a,v) then /* v is the value of a relation a */
					attributes = ContainsFact(topResult,v)
					# 19. candidateRelations.add(j,a); j=refcolumn(e.g.=capital)
					for a in attributes:
						candidateRelations[j].append(a)

	# 20. acceptableTypes <- allTypes.get5MostFrequent();
	acceptableTypes = getNMostFrequent(allTypes,5)
	

	# 21. descriptionTokens  <-  descriptionTokens.getMostFrequent();
	#TODO: Question? is it top token of each desc or just one top from all desc?
	descriptionTokenArr = []
	descriptionTokenArr = getMostFreqToken(descriptionTokens)

	# 22. for each column j of referenceColumns do
	for j in referenceColumns:
		print(candidateRelations[j],"\n\n")
		# 23. relations[j] <- candidateRelations.get(j).getFirst();
		if(candidateRelations[j] == []):
			print("continue")#TODO
		else:
			relations[j] = candidateRelations[j][0]
		#TODO: add 5 agreements 
	print("relations:",relations)
	print("acceptable types:",acceptableTypes)

	return(Tprime,TPrimeIsAnnotated,TPrimeAnnotation,acceptableTypes,descriptionTokenArr,relations,SFDistanceDict)


#------------------------Functions regarding Sample Phase------------------------
#Regarding line 21
def getMostFreqToken(DescriptionTokens):
	topDescToken = []
	topDescToken = wc.WordCount(DescriptionTokens)
	return(topDescToken)
#Regarding line 20
def getNMostFrequent(allTypes,n):
	itemCount = {}
	finalWordList = []
	
	for item in allTypes:
		if item not in itemCount:
			itemCount[item] = 1
		else:
			itemCount[item] +=1
	itemCounter = collections.Counter(itemCount)
	for item, count in itemCounter.most_common(n):
		finalWordList.append(item)	

	return(finalWordList)

#e.g. : search for london in Q7787906
def ContainsFact(rowtopresult, v):
	InnerDict = defaultdict(list)
	attributes = []
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
		if(v == value):
			attributes.append(attribute)
		#InnerDict[attribute].append(value)
	return(attributes)

def getDescriptionTokens(topResult):
	import requests
	import pandas as pd
	from collections import defaultdict
	descriptionTokens = ""
	#url = 'https://query.wikidata.org/sparql'
	url = 'http://localhost:9999/bigdata/namespace/wdq/sparql'

	query = """SELECT ?d
	WHERE {
  			wd:Q"""+str(topResult)+""" schema:description ?d.
   			FILTER(LANG(?d) = 'en')
   			SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
 			}"""

	r = requests.get(url, params = {'format': 'json', 'query': query})
	data = r.json()

	for item in data['results']['bindings']:
		#Top result description string
		descriptionTokens  = item['d']['value'] 

	return(descriptionTokens)


def getTypes(topResult):
	# e.g. : topResult is 145 , should be used as wd:Q145
	#considering all types in case of wikidata : Instance of wdt:P31
	import requests
	import pandas as pd
	from collections import defaultdict

	#url = 'https://query.wikidata.org/sparql'
	url = 'http://localhost:9999/bigdata/namespace/wdq/sparql'
	query = """SELECT 
		?item ?label  
	WHERE { 
		wd:Q"""+str(topResult)+""" wdt:P31 ?item. 
  		?item rdfs:label ?label.

  		FILTER(LANG(?label) = 'en')
	}
	"""
	r = requests.get(url, params = {'format': 'json', 'query': query})
	data = r.json()


	Types = []
	for item in data['results']['bindings']:
		#print(item)
		Types.append(item['label']['value'])

	
	return(Types)

#---------------------End Functions regarding Sample Phase------------------------



