import loadmodel as LM
import os
import pandas as pd
from collections import defaultdict
import pickle

import networkx as nx
import itertools
import re
import RefinedLookup as RL

#to plot from remote server
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#----------------------Loading pickle surface form ------------------------
search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))

#Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein_allcols.pickle", "rb"))    
Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein_allcols.pickle", "rb"))    

model = LM.Model.load(models_directory="data/models/Model_Creation",
                       filename="2-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=5")


#table file name 
table_csv = '36039980_4_4283009829602711082.csv'

#T2D path
os.chdir('/home/yasamin/Codes/WebTableAnnotation/data/T2D/Final_InstanceLevel_GoldStandard/tables_instance(Instance_level_GS)/')

#read the table
with open(table_csv, 'r',encoding='utf-8') as csvTableFile:
	print("This is the Table file name :\n\n",table_csv)
				
	T = pd.read_csv(table_csv, header=None)

    #switch path to normal form
	os.chdir('/home/yasamin/Codes/WebTableAnnotation/')


	Entities = []
	for index, row in T.iterrows():
		for e in row:
			Entities.append(e)

	print("Entities:",Entities)

    #Vertices = Union of (Candidates from SF of e --> m(e))
	CandidateVertices = defaultdict(list)
    #look the text fields in the SF
	for e in Entities:
		if(pd.isnull(e)):
			e = ""
		e = e.lower()
		e = re.sub("\s\s+" , " ", e)
		try:
			print("the entity we are going to search:",e,".")
                
			results = search[e]
			results.sort()
                
			print("its result:",results)
			for r in results:
				r_type = RL.getTypes(r)
				if(all(x in r_type for x in ['Wikimedia disambiguation page'])):
					results.remove(r)
				if(all(x in r_type for x in ['Wikimedia category'])):
					results.remove(r)
		        #if the only result was disambiguation page and removed:
				if(len(results)==0):
					raise KeyError('we had just Wikimedia disambiguation pages. now result is zero.')

				for pv in results:
					CandidateVertices[e].append(pv)
		except KeyError:
			print("keyword ",e, "not in Surface form")
			try:
				results = Levenshtein_T2D[e]
				print("Solved with levenshtein")
				print("e:",e)
				print("Solved result:",results)
			except (IndexError,KeyError) as err:
				print("no result from levenshtein on SF")
				continue
			continue

	CandidateVertices = { 'Switzerland': [39], 'Bern': [70], 'Iran': [794], 'Tehran':[3616], 'United Kingdom':[7887906,145],'London':[84] }
	print("Candidate vertices:",CandidateVertices)
    #Check if we have the embedding for all vertices:
	Vertices = defaultdict(list) 
	for ckey in CandidateVertices:
		for cval in CandidateVertices[ckey]:
			try:
				model.wv.word_vec("Q" + str(cval))
				Vertices[ckey].append(cval)
				print("The value:",cval,"with key",ckey,"added to Vertices" )
			except Exception:
				print(cval, " Was not in the embedding \n")
				continue

    #create the graph
	G = nx.DiGraph()
	Keys = []
	print("Vertices:",Vertices)
	for k in Vertices:
		Keys.append(k)
		for v in Vertices[k]:
			G.add_node(v)
	print("G.Nodes: \n",G.nodes)


    #weighted directed edge
    #weight(v1,v2) = cos( emb(v1),emb(v2))/Sigma-kInV (cos(emb(v1),emb(k)) )

	Sigma_kInV_dict = {}
    #calculating the denominator and save in dict:
	for k1, k2 in itertools.permutations(G.nodes,2):
		if(k1 in Sigma_kInV_dict):
			Sigma_kInV_dict[k1] += model.similarity("Q" + str(k1), "Q" + str(k2))
		else:
			Sigma_kInV_dict[k1] = model.similarity("Q" + str(k1), "Q" + str(k2))

    #TODO: use itertools.permutations and merge two directions of edges
    #first direction of edges
	for k1, k2 in itertools.combinations(Keys, 2):
		try:
			G.add_edges_from(((u, v, {'weight':round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3) })
				for u, v in itertools.product(Vertices[k1], Vertices[k2])))
		except KeyError:
			continue
    #second direction of edges
	for k1, k2 in itertools.combinations(Keys, 2):
		try:
			G.add_edges_from(((u, v, {'weight': round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3) })
				for u, v in itertools.product(Vertices[k2], Vertices[k1])))
		except KeyError:
			continue

	print(G.edges(data=True))

#with PageRank
rankingDict = nx.pagerank(G, alpha=0.85, max_iter = 50)
#with eigenvector
# rankingDict = nx.eigenvector_centrality(Looping_Graph, max_iter=100, tol=1e-06, nstart=None, weight=None)
#with Katz 
# rankingDict = nx.katz_centrality(Looping_Graph, alpha=0.1, beta=1.0, max_iter=1000, tol=1e-06, nstart=None, normalized=True, weight=None)
print("rankingDict PageRank:",rankingDict)

pos = nx.spring_layout(G)  # compute graph layout
nx.draw(G, pos)  # draw nodes and edges
edge_labels=dict([((u,v,),d['weight'])
             for u,v,d in G.edges(data=True)])
# nx.draw_networkx_labels(G, pos)
# labels = nx.get_edge_attributes(G, 'weight')
nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, label_pos=0.3, font_size=7)

#plt.figure()
plt.savefig('LoopingGraph.pdf')



#Backup
# pos = nx.spring_layout(G)  # compute graph layout
# nx.draw(G, pos)  # draw nodes and edges
# edge_labels=dict([((u,v,),d['weight'])
#              for u,v,d in G.edges(data=True)])
# # nx.draw_networkx_labels(G, pos)
# # labels = nx.get_edge_attributes(G, 'weight')
# nx.draw_networkx_edge_labels(G, pos, edge_labels=labels)

# #plt.figure()
# plt.savefig('LoopingGraph.pdf')