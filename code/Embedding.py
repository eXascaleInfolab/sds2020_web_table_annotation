import pickle
from gensim.models import Word2Vec
import loadmodel as LM
import pandas as pd
from collections import defaultdict
import networkx
import itertools
import Preprocessing_Table as PT
import Levenshtein
import re
import RefinedLookup as RL
import copy
from collections import Counter
from  decouple import config, Csv


DATASET_NAME = config('DATASET_NAME', cast=str)

def getEmbedding(T):
    #----------------------Loading embedding model ------------------------
    #model min_count = 1
    print('i am in embedding')
    model = LM.Model.load(models_directory="data/models/Model_Creation",
                       filename="3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1")
                       
    print(model.metadata)
    # print(model.similarity("Q19643", "Q12097")) #queen , king

    #----------------------Loading pickle surface form ------------------------
    search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))

    if DATASET_NAME == 'Limaye':
        levenshtein_pckl = pickle.load(open("data/surface/Levenshtein/LimayeLevenshtein_allcols.pickle", "rb")) 
    elif DATASET_NAME == 'T2D':
        levenshtein_pckl = pickle.load(open("data/surface/Levenshtein/T2DLevenshtein_allcols.pickle", "rb"))    
    #--------------------------- Disambiguation ----------------------------------

    T, labelColumn, referenceColumns = PT.Preprocessing_Table(T)
    print("T:",T)
    #Keep one Reference column and annotate with 2 columns at a time
    #first outer key = Ref Column index that we are using
    #inner key = Table row index
    RefBased_TPrimeAnnotation = defaultdict(lambda: defaultdict(list))

    TPrimeIsAnnotated = defaultdict(list) 
    TPrimeAnnotation = defaultdict(list)
    
    #no non-numeric(text) Reference column
    print("hell referenceColumns:",referenceColumns)
    if(referenceColumns == []):
        print("hell referenceColumns is == []")
        
    if(referenceColumns == []):
        #-------Start no ref Col
        #all the text fields in the table --> e
        Entities = []

        disambiguated_rows = {}

        for index, row in T.iterrows():
                for e in row:
                    #just put label column entity    
                    if(e == row[labelColumn]):
                        try:
                            if(len(e)>3):
                                Entities.append(e)
                        except Exception:
                            print("cant get the length")
                            continue

        print("Entities:",Entities)

        #Vertices = Union of (Candidates from SF of e --> m(e))
        CandidateVertices = defaultdict(list)

        #to understand wether we solved by levenshtein, keep the key
        IsWithLevenshtein = []

        #keep the number of nodes in initial looping graph.
        #key = number of nodes, val = number of tables with that much nodes.
        Frequency_NodesInFirstGraph = {}

        #look the text fields in the SF
        for e in Entities:
            if(e.isdigit()):
                    continue

            if(pd.isnull(e)):
                    e = ""
            e = e.lower()
            e = re.sub("\s\s+" , " ", e)
            try:
                    print("the entity we are going to search:",e,".")
                    
                    results = search[e]
                    results.sort()
                    #keep top 5 candidates - u can choose any number
                    results = results[0:5]
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
                            #uncomment to calculate new levenshtein
                            #--------------------------------------
                            # levenshtein_candidate = []
                            # for i in search:
                            #         similarityRatio = Levenshtein.ratio(i,e)
                            #         if(similarityRatio>0.80):
                            #                 if(i in levenshtein_candidate):
                            #                         for j in search[i]:
                            #                                 levenshtein_candidate.append(j)
                            #                 else:
                            #                         levenshtein_candidate = search[i]
                            # levenshtein_candidate.sort()
                            # #keep top 5
                            # levenshtein_candidate = levenshtein_candidate[0:5]
                            # if not(levenshtein_candidate == []):
                            #         results = levenshtein_candidate
                            #         #SFDistanceDict[e] = levenshtein_candidate[0]
                            #         print("solved with levenshtein")
                            #         print("e:",e)
                            #         print("solved result",results)
                            #         for pv in results:
                            #                 CandidateVertices[e].append(pv)
                            #--------------------------------------
                            results = levenshtein_pckl[e]
                            print("Solved with levenshtein")
                            print("e:",e)
                            print("Solved result:",results)
                            IsWithLevenshtein.append(e)
                            for pv in results:
                                CandidateVertices[e].append(pv)
                    except (IndexError,KeyError) as err:
                            print("no result from levenshtein on SF")
                            continue
                    continue

        print("Candidate vertices:",CandidateVertices)
        #Check if we have the embedding for all vertices:
        Vertices = defaultdict(list) 
        NoUnique_Candidate = defaultdict(list)

        for ckey in CandidateVertices:
                for cval in CandidateVertices[ckey]:
                        try:
                                model.wv.word_vec("Q" + str(cval))
                                if not(ckey == ''):
                                    Vertices[ckey].append(cval)
                                    print("The value:",cval,"with key",ckey,"added to Vertices" )
                        except Exception:
                                print(cval, " Was not in the embedding \n")
                                continue

        #create the graph
        G = networkx.DiGraph()
        Keys = []
        print("Is With Levenshtein?",IsWithLevenshtein)
        """ each time we add one not annotated (row/ or entity ?)"""
        Looping_Graph = networkx.DiGraph()
        Keys_Looping = []
        
        print("Vertices:",Vertices)
        
        for v_k in Vertices:
                #remove duplicates from candidateVertices:
                Vertices[v_k] = list(dict.fromkeys(Vertices[v_k]))
                #sort their candidates to have lower first:
                Vertices[v_k].sort()

                if(len(Vertices[v_k])==1 and (v_k not in IsWithLevenshtein)):
                    Keys.append(v_k)
                    #adding candidates of the entity
                    for v in Vertices[v_k]:
                        G.add_node(v) 

                elif(len(Vertices[v_k])>1 or (v_k in IsWithLevenshtein)):
                    Keys_Looping.append(v_k)
                    #later, eah of the values in nounique_candidate will be added to looping graph, one by one.
                    NoUnique_Candidate[v_k] = Vertices[v_k] 
                    print("v_k:",v_k,"added to NoUnique_Candidate",NoUnique_Candidate)

        #keep the number of nodes in initial looping graphs
        Nodes_Cnt_initialLoopingGraph = len(list(G.nodes))
        if (Nodes_Cnt_initialLoopingGraph in Frequency_NodesInFirstGraph):
            Frequency_NodesInFirstGraph[Nodes_Cnt_initialLoopingGraph] += 1
        else:
            Frequency_NodesInFirstGraph[Nodes_Cnt_initialLoopingGraph] = 1




        #if there is no node in the initial graph, get the most frequent type of all the candidates
        #just used to take types of table, for looping method, we make the graph empty.
        #---------------------------
        print("HELL G.nodes:",G.nodes)
        if(list(G.nodes) == []):
            for v_k in Vertices:
                #remove duplicates from candidateVertices:
                Vertices[v_k] = list(dict.fromkeys(Vertices[v_k]))
                #sort their candidates to have lower first:
                Vertices[v_k].sort()
                for v in Vertices[v_k]:
                        G.add_node(v) 
                        print("Hell v:",v,"Added to graph G(nounique nodes added as well). G:",G)
        #---------------------------

        LoopingGraph_types = []
        #Get the frequent type from the looping graph  
        print("HELL THIS IS G.nodes:",G.nodes)
        for node in G:
            print("HELL THIS IS NODE:",node)
            node_types = getTypes(node)
            print("HELL THIS IS NODE TYPES:",node_types)
            for node_type in node_types:
                LoopingGraph_types.append(node_type)
                print("HELL1 LoopingGraph_types:",LoopingGraph_types)
        print("HELL2 LoopingGraph_types:",LoopingGraph_types)
        #get top 3 types out of all types of nodes.
        try:
            #when we have no Reference column, keep only one(or 2) type! cause one label column must have 1 type!
            #when we have some Reference Columns, keep top 3 types
            top3_LoopingGraph_types = getNMostFrequent(LoopingGraph_types,3)
            print("HELL top3_LoopingGraph_types:",top3_LoopingGraph_types)
        except Exception:
            print("We dont have the Looping types")
 

        """we have all the entities with more than one candidate in NoUnique_Candidate 
                each time, one will be choosed to annotate with the nodes already exsit in graph G""" 
        print("NoUnique_Candidate:",NoUnique_Candidate)
        

        """if there is no node with more than 1 candidate, add one from the first graph 
        to enter this loop"""
        if(NoUnique_Candidate == {} and Vertices != {}):
            try:
                first_k = list(Vertices.keys())[0]
                NoUnique_Candidate[first_k] = Vertices[first_k]
            except Exception:
                print("problem in initialization of empty NoUnique_Candidate")


        for k in NoUnique_Candidate:
            Keys = []
            Keys_Looping = []
            G = networkx.DiGraph()
            print("Hell G:",G,"Just to be sure it is empty again in here")
            #to add G + already annotated ones + one key of current loop
            Looping_Graph = networkx.DiGraph()

            for v_k in Vertices:
                print("this is v_k",v_k,"this is Vertices[v_k]",Vertices[v_k])
                if(len(Vertices[v_k])==1 and (v_k not in IsWithLevenshtein)):
                        Keys.append(v_k)
                        print("k appended")
                        #adding candidates of the entity
                        for v in Vertices[v_k]:
                            print("NODE",v,"ADDED TO GRAPH G for the entity:",v_k)
                            G.add_node(v)

            Looping_Graph = copy.deepcopy(G)

            #adding just one node with more than one candidate.
            #add all the candidates of this loop's text entity
            for k_can in Vertices[k]:
                #k_can is the wikidata ID
                Looping_Graph.add_node(k_can)
            print("G.Nodes: \n",G.nodes)
            print("Node",k,"is added in this round")
            print("Looping_Graph:\n",Looping_Graph.nodes) 
            
            #add the k of this loop to the keys
            Keys_Looping = copy.deepcopy(Keys)
            Keys_Looping.append(k)
            print("keys:",Keys)
            print("Keys_Looping",Keys_Looping)

            #weighted directed edge
            #weight(v1,v2) = cos( emb(v1),emb(v2))/Sigma-kInV (cos(emb(v1),emb(k)) )

            Sigma_kInV_dict = {}
            #calculating the denominator and save in dict:
            for k1, k2 in itertools.permutations(Looping_Graph.nodes,2):
                if(k1 in Sigma_kInV_dict):
                    Sigma_kInV_dict[k1] += model.similarity("Q" + str(k1), "Q" + str(k2))
                else:
                    Sigma_kInV_dict[k1] = model.similarity("Q" + str(k1), "Q" + str(k2))

            #TODO: use itertools.permutations and merge two directions of edges
            #first direction of edges
            for k1, k2 in itertools.combinations(Keys, 2):
                try:
                    if(len(Vertices[k1])==1 and len(Vertices[k2])==1):
                        Looping_Graph.add_edges_from(((u, v, {'weight':round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3)+0.7 })
                                            for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                    else:
                        Looping_Graph.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                                    for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                except KeyError:
                        continue
        
            #second direction of edges
            for k1, k2 in itertools.combinations(Keys, 2):
                try:
                    if(len(Vertices[k1])==1 and len(Vertices[k2])==1):
                        Looping_Graph.add_edges_from(((u, v, {'weight': round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3)+0.7 })
                                for u, v in itertools.product(Vertices[k2], Vertices[k1])))
                    else:
                        Looping_Graph.add_edges_from(((u, v, {'weight': (round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                            for u, v in itertools.product(Vertices[k2], Vertices[k1])))
                except KeyError:
                        continue

            print(G.edges(data=True))
            print(Looping_Graph.edges(data=True))
            

            #Adding weighted pagerank
            try:
                #with PageRank
                # rankingDict = networkx.pagerank(Looping_Graph, alpha=0.85, max_iter = 50)
                #with eigenvector
                # rankingDict = networkx.eigenvector_centrality(Looping_Graph, max_iter=100, tol=1e-06, nstart=None, weight=None)
                #with Katz 
                rankingDict = networkx.katz_centrality(Looping_Graph, alpha=0.1, beta=1.0, max_iter=1000, tol=1e-06, nstart=None, normalized=True, weight=None)
                print("rankingDict katz:",rankingDict)
                
            except networkx.exception.PowerIterationFailedConvergence:
                rankingDict = {}
                print("networkx.exception.PowerIterationFailedConvergence handled!")
                #Q: should i only add more score for the nodes in primary graph or all?!
                for node in Looping_Graph.nodes:
                # for node in G.nodes:
                    rankingDict[node] =  0.1
                print("rankingDict after:",rankingDict)    
            
        #------------------------------------------------------------------
            disambiguated_rows = {}
            for index, row in T.iterrows():
                
                #we dont loop over row, just take label column entity
                rowcolval = row[labelColumn]
                MaxRank = 0
                #for rowcolval in row:
                print("it is rowcolval:",rowcolval)

                if(pd.isnull(rowcolval)):
                    rowcolval = ""
                    
                if(rowcolval.isdigit()):
                            continue    
                #remvove floating point 
                rowcolval = re.sub("(\d*\.\d+)|(\d+\.[0-9 ]+)","",rowcolval) 
                                    
                rowcolval = rowcolval.lower()
                rowcolval = re.sub("\s\s+" , " ", rowcolval)
                if(rowcolval):
                    disambiguated_entity = None
                    for c in Vertices[rowcolval]:# c is the candidates of the mention e
                            if(Looping_Graph.has_node(c)):
                                print("and it exists in graph!")
                                print("RANKINGDICT[C]:",rankingDict[c])
                                print("MAXRANK:",MaxRank)
                                if (rankingDict[c] > MaxRank):
                                        print("The Max Rank:" , MaxRank, "The candidate:", c)
                                        MaxRank = rankingDict[c]
                                        disambiguated_entity = c
                                elif (rankingDict[c] == MaxRank):
                                    print("The Max Ranks are the same. current c:",c,"disambiguated_entity value:",disambiguated_entity)
                                    if not(disambiguated_entity==None):
                                        t1 = getTypes(disambiguated_entity) 
                                        print("t1:",t1)
                                        t2 = getTypes(c)
                                        print("t2:",t2)
                                        print("top3_LoopingGraph_types",top3_LoopingGraph_types)
                                        #choose the candidate with more frequent types in it.
                                        if(len(intersection(top3_LoopingGraph_types,t1))<len(intersection(top3_LoopingGraph_types,t2))):
                                            disambiguated_entity = c
                                            print("The Max Rank:" , MaxRank, "The candidate:", c)

                    if not(disambiguated_entity==None):
                            disambiguated_rows[rowcolval] = disambiguated_entity
                            Vertices[rowcolval] = [disambiguated_entity]
                            print("We changed the value of vertices in this loop")
                            
                            #we dont care how anymore, cause it is annotated and we imagine it is correct
                            if(rowcolval in IsWithLevenshtein):
                                IsWithLevenshtein.remove(rowcolval)
                    else:
                            disambiguated_rows[rowcolval] = " "

            print("Disambiguated rows are: " , disambiguated_rows)

        #End of the Looping method
        #-------------------------------------------------------------------------------
        for i in range(0, len(T)):
                for col in T: #gives column header names
                    if(col == labelColumn):
                        if(pd.isnull(T.iloc[i][col])):
                            T.iloc[i][col] = ""                              
                        T.iloc[i][col] = T.iloc[i][col].lower()
                        T.iloc[i][col] = re.sub("\s\s+" , " ", T.iloc[i][col])
        TPrimeIsAnnotated = defaultdict(list) 
        TPrimeAnnotation = defaultdict(list)

        for index, row in T.iterrows():
            rowcolval = row[labelColumn]
            if(rowcolval):
                try:
                    if not(disambiguated_rows[rowcolval]== " "):
                        TPrimeIsAnnotated[index].append(True)
                        TPrimeAnnotation[index].append(disambiguated_rows[rowcolval])
                    else:
                        TPrimeIsAnnotated[index].append(False)
                        TPrimeAnnotation[index].append(" ")
                except KeyError:
                    print("rowcolval not exists in disambiguated_rows")
                    TPrimeIsAnnotated[index].append(False)
                    TPrimeAnnotation[index].append(" ")

        print("before choosing the Labelcolumn:")
        print("-" * 50)
        print("TPrime is annotated: \n", TPrimeIsAnnotated)

        print("TPrime annotation: \n", TPrimeAnnotation)
        print("-" * 50)    
            
        #RefBased is not needed cause we have only label column!
        
        #change the list to entry - we keep annotation only for the labelcolumn
        for k in TPrimeAnnotation:
                TPrimeAnnotation[k] = TPrimeAnnotation[k][0]
        for k in TPrimeIsAnnotated:
                TPrimeIsAnnotated[k] = TPrimeIsAnnotated[k][0]


        #-------End no ref Col

    #Start at least one textual reference column
    #-----------------------------------------------
    else:

        for RefColIndex in referenceColumns:
            #all the text fields in the table --> e
            Entities = []

            disambiguated_rows = {}
            for index, row in T.iterrows():
                    for e in row:
                        if(e == row[labelColumn] or e == row[RefColIndex]):
                            try:
                                if(len(e)>3):
                        # #just put label column entity    
                        # if(e == row[labelColumn]):
                                        Entities.append(e)
                            except Exception:
                                    print("Cant get length")
                                    continue

            for index, row in T.iterrows():
                for e in row:
                    if(e == row[labelColumn] or e == row[RefColIndex]):
                        try:
                            # print("labelcolumn:",labelColumn)
                            # print("RefColIndex:",RefColIndex)

                            if(len(e)>3):
                    # #just put label column entity    
                    # if(e == row[labelColumn]):
                                    Entities.append(e)
                        except Exception:
                                print("Cant get length")
                                continue

            print("Entities:",Entities)
            #done till here --> keeping 2 cols = 1ref + LC
            #Vertices = Union of (Candidates from SF of e --> m(e))
            CandidateVertices = defaultdict(list)
            
            #to understand wether we solved by levenshtein, keep the key
            IsWithLevenshtein = []
            #look the text fields in the SF
            for e in Entities:
                if(e.isdigit()):
                    continue
                if(pd.isnull(e)):
                        e = ""
                e = e.lower()
                e = re.sub("\s\s+" , " ", e)
                try:
                        print("the entity we are going to search:",e)
                    
                        results = search[e]
                        results.sort()
                        # keep top 5 candidates - u can choose any number
                        results = results[0:5]
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
                                #uncomment to calculate new levenshtein
                                #--------------------------------------
                                # levenshtein_candidate = []
                                # for i in search:
                                #         similarityRatio = Levenshtein.ratio(i,e)
                                #         if(similarityRatio>0.80):
                                #                 if(i in levenshtein_candidate):
                                #                         for j in search[i]:
                                #                                 levenshtein_candidate.append(j)
                                #                 else:
                                #                         levenshtein_candidate = search[i]
                                # levenshtein_candidate.sort()
                                # #keep top 5
                                # levenshtein_candidate = levenshtein_candidate[0:5]
                                # if not(levenshtein_candidate == []):
                                #         results = levenshtein_candidate
                                #         #SFDistanceDict[e] = levenshtein_candidate[0]
                                #         print("solved with levenshtein")
                                #         print("e:",e)
                                #         print("solved result",results)
                                #         for pv in results:
                                #                 CandidateVertices[e].append(pv)
                                #--------------------------------------
                                results = levenshtein_pckl[e]
                                print("Solved with levenshtein")
                                print("e:",e)
                                print("Solved result:",results)
                                IsWithLevenshtein.append(e)
                                for pv in results:
                                    CandidateVertices[e].append(pv)
                        except (IndexError,KeyError) as err:
                                print("no result from levenshtein on SF")
                                continue
                        continue
                
                
            print("Candidate vertices:",CandidateVertices)
            #Check if we have the embedding for all vertices:
            Vertices = defaultdict(list) 
            NoUnique_Candidate = defaultdict(list)

            for ckey in CandidateVertices:
                    for cval in CandidateVertices[ckey]:
                            try:
                                    model.wv.word_vec("Q" + str(cval))
                                    if not(ckey == ''):
                                        Vertices[ckey].append(cval)
                                        print("The value:",cval,"with key",ckey,"added to Vertices" )
                            except Exception:
                                    print(cval, " Was not in the embedding \n")
                                    continue

            #create the graph
            G = networkx.DiGraph()
            Keys = []
            print("Is With Levenshtein?",IsWithLevenshtein)
            #create the graph
            """ each time we add one not annotated (row/ or entity ?)"""
            Looping_Graph = networkx.DiGraph()
            Keys_Looping = []

            print("Vertices:",Vertices)

            for v_k in Vertices:
                #remove duplicates from candidateVertices:
                Vertices[v_k] = list(dict.fromkeys(Vertices[v_k]))
                #sort their candidates to have lower first:
                Vertices[v_k].sort()

                if(len(Vertices[v_k])==1 and (v_k not in IsWithLevenshtein)):
                    Keys.append(v_k)
                    #adding candidates of the entity
                    for v in Vertices[v_k]:
                        G.add_node(v)

                elif(len(Vertices[v_k])>1 or (v_k in IsWithLevenshtein)):
                    Keys_Looping.append(v_k)
                    #later, eah of the values in nounique_candidate will be added to looping graph, one by one.
                    NoUnique_Candidate[v_k] = Vertices[v_k]      
                    print("v_k:",v_k,"added to NoUnique_Candidate",NoUnique_Candidate)

            #keep the number of nodes in initial looping graphs
            Nodes_Cnt_initialLoopingGraph = len(list(G.nodes))


            #if there is no node in the initial graph, get the most frequent type of all the candidates
            #just used to take types of table, for looping method, we make the graph empty.
            #---------------------------
            print("HELL G.nodes:",G.nodes)
            if(list(G.nodes) == []):
                for v_k in Vertices:
                    #remove duplicates from candidateVertices:
                    Vertices[v_k] = list(dict.fromkeys(Vertices[v_k]))
                    #sort their candidates to have lower first:
                    Vertices[v_k].sort()
                    for v in Vertices[v_k]:
                            G.add_node(v) 
                            print("Hell v:",v,"Added to graph G(nounique nodes added as well). G:",G)
        #---------------------------


            LoopingGraph_types = []
            #Get the frequent type from the looping graph  
            for node in G:
                node_types = getTypes(node)
                for node_type in node_types:
                    LoopingGraph_types.append(node_type)
            #get top 3 types out of all types of nodes.
            try:
                top3_LoopingGraph_types = getNMostFrequent(LoopingGraph_types,3)
            except Exception:
                print("We dont have the Looping types")

            """we have all the entities with more than one candidate in NoUnique_Candidate 
                each time, one will be choosed to annotate with the nodes already exsit in graph G""" 
            
            print("NoUnique_Candidate:",NoUnique_Candidate)
            
            """if there is no node with more than 1 candidate, add one from the first graph 
            to enter this loop"""
            if(NoUnique_Candidate == {} and Vertices != {}):
                try:
                    first_k = list(Vertices.keys())[0]
                    NoUnique_Candidate[first_k] = Vertices[first_k]
                except Exception:
                    print("problem in initialization of empty NoUnique_Candidate")


            for k in NoUnique_Candidate:
                Keys = []
                Keys_Looping = []
                G = networkx.DiGraph()
                #to add G + already annotated ones + one key of current loop
                Looping_Graph = networkx.DiGraph()

                for v_k in Vertices:
                    print("this is v_k",v_k,"this is Vertices[v_k]",Vertices[v_k])
                    if(len(Vertices[v_k])==1 and (v_k not in IsWithLevenshtein)):
                        Keys.append(v_k)
                        print("k appended")
                        #adding candidates of the entity
                        for v in Vertices[v_k]:
                            print("NODE",v,"ADDED TO GRAPH G for the entity:",v_k)
                            G.add_node(v)

                Looping_Graph = copy.deepcopy(G)
                
                #adding just one node with more than one candidate.
                #add all the candidates of this loop's text entity
                for k_can in Vertices[k]:
                    #k_can is the wikidata ID
                    Looping_Graph.add_node(k_can)
                print("G.Nodes: \n",G.nodes)
                print("Node",k,"is added in this round")
                print("Looping_Graph:\n",Looping_Graph.nodes) 
            
                #add the k of this loop to the keys
                Keys_Looping = copy.deepcopy(Keys)
                Keys_Looping.append(k)
                print("keys:",Keys)
                print("Keys_Looping",Keys_Looping)

                Sigma_kInV_dict = {}
                #calculating the denominator and save in dict:
                for k1, k2 in itertools.permutations(Looping_Graph.nodes,2):
                    if(k1 in Sigma_kInV_dict):
                        Sigma_kInV_dict[k1] += model.similarity("Q" + str(k1), "Q" + str(k2))
                    else:
                        Sigma_kInV_dict[k1] = model.similarity("Q" + str(k1), "Q" + str(k2))

                #TODO: use itertools.permutations and merge two directions of edges
                #first direction of edges
                """instead of giving Keys.Looping, we give Keys, so it will personalized pagerank 
                that starting or end node in random walk must be from the first graph instead of all the nodes"""
                for k1, k2 in itertools.combinations(Keys, 2):
                    try:
                        if(len(Vertices[k1])==1 and len(Vertices[k2])==1):
                            Looping_Graph.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3)+0.7)})
                                        for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                        else:
                            Looping_Graph.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                                        for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                    except KeyError:
                            continue 
                #second direction of edges
                """instead of giving Keys.Looping, we give Keys, so it will personalized pagerank 
                that starting or end node in random walk must be from the first graph instead of all the nodes"""
                for k1, k2 in itertools.combinations(Keys, 2):
                    try:
                        if(len(Vertices[k1])==1 and len(Vertices[k2])==1):
                            Looping_Graph.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3)+0.7)})
                                for u, v in itertools.product(Vertices[k2], Vertices[k1])))
                        else:
                            Looping_Graph.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                                for u, v in itertools.product(Vertices[k2], Vertices[k1])))
                    except KeyError:
                            continue

                print(G.edges(data=True))
                print(Looping_Graph.edges(data=True))
            
                #Adding weighted pagerank
                try:
                    #with PageRank
                    # rankingDict = networkx.pagerank(Looping_Graph, alpha=0.85, max_iter = 50)
                    #with eigenvector
                    # rankingDict = networkx.eigenvector_centrality(Looping_Graph, max_iter=100, tol=1e-06, nstart=None, weight=None)
                    #with Katz 
                    rankingDict = networkx.katz_centrality(Looping_Graph, alpha=0.1, beta=1.0, max_iter=1000, tol=1e-06, nstart=None, normalized=True, weight=None)
                    print("rankingDict katz:",rankingDict)
                    
                except networkx.exception.PowerIterationFailedConvergence:
                    rankingDict = {}
                    print("networkx.exception.PowerIterationFailedConvergence handled!")
                    #Q: should i only add more score for the nodes in primary graph or all?!
                    for node in Looping_Graph.nodes:
                    # for node in G.nodes:
                        rankingDict[node] =  0.1
                    print("rankingDict after:",rankingDict)    
            
            #------------------------------------------------------------------
                disambiguated_rows = {}
                for index, row in T.iterrows():
                    for rowcolval in row:
                            MaxRank = 0
                            print("it is rowcolval:",rowcolval)
                        #rowcolval = row[labelColumn]
                            if(pd.isnull(rowcolval)):
                                rowcolval = ""

                            if(rowcolval.isdigit()):
                                continue

                            #remvove floating point 
                            rowcolval = re.sub("(\d*\.\d+)|(\d+\.[0-9 ]+)","",rowcolval) 
                            
                            rowcolval = rowcolval.lower()
                            rowcolval = re.sub("\s\s+" , " ", rowcolval)
                            if(rowcolval):
                                disambiguated_entity = None
                                for c in Vertices[rowcolval]:# c is the candidates of the mention e
                                    #if it was one of the added nodes to the looping_graph
                                    if(Looping_Graph.has_node(c)):
                                        print("and it exists in graph!")
                                        print("RANKINGDICT[C]:",rankingDict[c])
                                        print("MAXRANK:",MaxRank)
                                        if (rankingDict[c] > MaxRank):
                                                MaxRank = rankingDict[c]
                                                print("The Max Rank:" , MaxRank, "The candidate:", c)
                                                disambiguated_entity = c
                                        #if the ranking has the same as MaxRank, choose by type filtering.
                                        elif (rankingDict[c] == MaxRank):
                                            print("The Max Ranks are the same. current c:",c,"disambiguated_entity value:",disambiguated_entity)
                                            if not(disambiguated_entity==None):
                                                t1 = getTypes(disambiguated_entity)
                                                print("t1:",t1)
                                                t2 = getTypes(c)
                                                print("t2:",t2)
                                                print("top3_LoopingGraph_types",top3_LoopingGraph_types)
                                                #choose the candidate with more frequent types in it.
                                                if(len(intersection(top3_LoopingGraph_types,t1))<len(intersection(top3_LoopingGraph_types,t2))):
                                                    disambiguated_entity = c
                                                    print("The Max Rank:" , MaxRank, "The candidate:", c)

                                if not(disambiguated_entity==None):
                                        disambiguated_rows[rowcolval] = disambiguated_entity
                                        Vertices[rowcolval] = [disambiguated_entity]
                                        print("We changed the value of vertices in this loop")
                                        
                                        #we dont care how anymore, cause it is annotated and we imagine it is correct
                                        if(rowcolval in IsWithLevenshtein):
                                            IsWithLevenshtein.remove(rowcolval)
                                else:
                                        disambiguated_rows[rowcolval] = " "

                print("Disambiguated rows are:" , disambiguated_rows)
        


        #End of the Looping method
        #-------------------------------------------------------------------------------

            for i in range(0, len(T)):
                    for col in T: #gives column header names
                        if(pd.isnull(T.iloc[i][col])):
                            T.iloc[i][col] = ""                              
                        T.iloc[i][col] = T.iloc[i][col].lower()
                        T.iloc[i][col] = re.sub("\s\s+" , " ", T.iloc[i][col])
            TPrimeIsAnnotated = defaultdict(list) 
            TPrimeAnnotation = defaultdict(list)

            for index, row in T.iterrows():
                for rowcolval in row:
                    try:
                        if not(disambiguated_rows[rowcolval]== " "):
                            TPrimeIsAnnotated[index].append(True)
                            TPrimeAnnotation[index].append(disambiguated_rows[rowcolval])
                        else:
                            TPrimeIsAnnotated[index].append(False)
                            TPrimeAnnotation[index].append(" ")
                    except KeyError:
                        print("rowcolval not exists in disambiguated_rows")
                        TPrimeIsAnnotated[index].append(False)
                        TPrimeAnnotation[index].append(" ")
    #------------------------------------------------------------------------------------------
    # #improved result from TP 159 to 172 in one case. keep for later
    #     """if there are annotations for the other entities in a row but not
    #     the one that we want 
    #     Warning: we consider that we have only one column apart from the label column at the moment"""
    #     chosen_candidate = ''
    #     for k in TPrimeIsAnnotated:
    #             for col in T:
    #                     try:
    #                         if not(col == labelColumn):
    #                             print("Col:",col)
    #                             print("label column:",labelColumn)        
    #                             if(TPrimeIsAnnotated[k][labelColumn]==False):
    #                                 if(TPrimeIsAnnotated[k][col] == True):
    #                                     candidate_similarity = dict()
    #                                     #compute similarity of candidates and annotation id in row!
    #                                     #get it from the model!
    #                                     #get candidates of label column
    #                                     print("T.iloc[k][labelcolumn]:",T.iloc[k][labelColumn])
    #                                     early_candidates = Vertices[T.iloc[k][labelColumn]]
    #                                     print("The k is:",k)
    #                                     print("Early candidates:", early_candidates)
    #                                     max_similarity = 0
    #                                     chosen_candidate = ''
    #                                     for can in early_candidates:
    #                                         candidate_similarity[can] = model.similarity("Q"+str(TPrimeAnnotation[k][col]), "Q"+ str(can))
    #                                         print("candidate Similarity:",candidate_similarity[can])
    #                                         print("can:",can)
    #                                         if(candidate_similarity[can] > max_similarity):
    #                                             #max_similarity is the highest similarity of the already annotated and the candidate of not annotated entity
    #                                             #chosen_candidate = contains wikidata id that we have chosen to annotate with.
    #                                             max_similarity = candidate_similarity[can]
    #                                             chosen_candidate = can
    #                                             print("Chosen Candidate:",chosen_candidate)
    #                                         else:
    #                                             chosen_candidate = ''    
                                            
    #                                 if not(chosen_candidate == ''):
    #                                         #header line is ommited! i guess :D
    #                                         TPrimeAnnotation[k+1][labelColumn] = chosen_candidate
    #                                         TPrimeIsAnnotated[k+1][labelColumn] = True 


    #                     #early candidates always empty ?? :-? why ? !
    #                     except IndexError:
    #                             #since we have removed some columns of the table(numeric ...)
    #                             continue
    #user other entities of the row
    #------------------------------------------------------------------------------------------
            print("before choosing the Labelcolumn:")
            print("-" * 50)
            print("TPrime is annotated: \n", TPrimeIsAnnotated)
        
            #contains all entities annotation in a row
            print("TPrime annotation: \n", TPrimeAnnotation)
            print("-" * 50)    
            
            print("RefBased_TPrimeAnnotation before deep copy")
            RefBased_TPrimeAnnotation[RefColIndex] = copy.deepcopy(TPrimeAnnotation) 
            #If needed u can add isannotation as well. 
            print("RefBased_TPrimeAnnotation",RefBased_TPrimeAnnotation)
            print("RefBased_TPrimeAnnotation after deep copy from TPrimeAnnotation")

        #change the list to entry - we keep annotation only for the labelcolumn
        print("Embedding:")
        #why the hell u keep only 0 ??now u dont have only the label column annotation! u have them all!
        #so keep the one for the label column.
        #if u want to use only label column, use 0 instead of label column
    

        #Keep the key, but empty the annotations for the final round.
        #TODO: use new variable like final instead of cleaning them.
        for annotation_key in TPrimeAnnotation:
            TPrimeAnnotation[annotation_key] = " "
        for isannotated_key in TPrimeAnnotation:
            TPrimeIsAnnotated[isannotated_key] = False


        #number of label column cant help cause we dont know if the previous columns still exist or not. 
        #so we should know this label column is which ith number in the existing columns
        print("RefBased_TPrimeAnnotation:",RefBased_TPrimeAnnotation)
        
        for RefColIndex in RefBased_TPrimeAnnotation:
            for Row_k in RefBased_TPrimeAnnotation[RefColIndex]:
                if(TPrimeAnnotation[Row_k] == " "):
                    if not(RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn]== " "):
                        TPrimeAnnotation[Row_k] = RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn]
                        TPrimeIsAnnotated[Row_k] = True
                    else:
                        TPrimeAnnotation[Row_k] = " "
                        TPrimeIsAnnotated[Row_k] = False
                else:
                    #we have annotation for the label column from previous Ref Col run
                    #If they want add model similarity!! now it is the first Ref Col val
                    continue
    #End of having Reference Column part
    #--------------------------------------------------------------------------------


    print(TPrimeIsAnnotated)
    print(TPrimeAnnotation)
    return(TPrimeIsAnnotated,TPrimeAnnotation)


#----------------------------------------------------------------------
#                              FUNCTIONS
#----------------------------------------------------------------------

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


import collections
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

# Python program to illustrate the intersection 
# of two lists in most simple way 
def intersection(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return lst3 
