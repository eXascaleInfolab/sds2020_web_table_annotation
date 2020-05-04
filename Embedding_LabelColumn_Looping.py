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
#WARNING: the section where we dont have any reference column is not added to this code. 
#copy from Embedding.py to fix this.
def getEmbedding(T):
    #----------------------Loading embedding model ------------------------
    #model min_count = 1
    model = LM.Model.load(models_directory="data/models/Model_Creation",
                       filename="3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1")


    print(model.metadata)
    vector = model.wv.word_vec("Q19643")
    print(model.similarity("Q19643", "Q12097")) #queen , king

    #----------------------Loading pickle surface form ------------------------
    #surface_forms = pickle.load(open("data/surface/surfaceForms-20180820.pickle", "rb"))
    search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))    

    #entities of all the columns
    Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein_allcols.pickle", "rb"))    
    # Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein_allcols.pickle", "rb"))    

    #entities of label column only
    #Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein.pickle", "rb"))    
    #Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein.pickle", "rb"))    
    
    #--------------------------- Disambiguation ----------------------------------

    # T = pd.DataFrame({'Country': ['Dominica','Tajikistan','Djibouti','Gabon'],
    #         'Population': [1000,2000,3000,4000],
    #         'Capital': ['Roseau','Dushanbe','Djibouti','Libreville']})

    #now done in preprocessing_table
    #T = T.select_dtypes(exclude=['int_','float_','complex_'])



    T, labelColumn, referenceColumns = PT.Preprocessing_Table(T)


    TPrimeIsAnnotated = defaultdict(list) 
    TPrimeAnnotation = defaultdict(list)

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
                        results = Levenshtein_Limaye[e]
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
                # for v in Vertices[v_k]:
                    # G.add_node(v)
            elif(len(Vertices[v_k])>1):
                Keys_Looping.append(v_k)
                #later, eah of the values in nounique_candidate will be added to looping graph, one by one.
                NoUnique_Candidate[v_k] = Vertices[v_k]      
    """we have all the entities with more than one candidate in NoUnique_Candidate 
            each time, one will be choosed to annotate with the nodes already exsit in graph G""" 
        
    print("NoUnique_Candidate:",NoUnique_Candidate)
        
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
        Keys_Looping = Keys
        Keys_Looping.append(k)
        print("keys:",Keys)
        print("Keys_Looping",Keys_Looping)

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
                if(len(Vertices[k1])==1):
                    G.add_edges_from(((u, v, {'weight':round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3) + 0.7 })
                                        for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                else:
                    G.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                                for u, v in itertools.product(Vertices[k1], Vertices[k2])))
            except KeyError:
                    continue
    
        #second direction of edges
        for k1, k2 in itertools.combinations(Keys, 2):
            try:
                if(len(Vertices[k1])==1):
                    G.add_edges_from(((u, v, {'weight': round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3)+0.7 })
                            for u, v in itertools.product(Vertices[k2], Vertices[k1])))
                else:
                    G.add_edges_from(((u, v, {'weight': (round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                        for u, v in itertools.product(Vertices[k2], Vertices[k1])))
            except KeyError:
                    continue
        print(G.edges(data=True))
        print(Looping_Graph.edges(data=True))
        

        #Adding weighted pagerank
        try:
            rankingDict = networkx.pagerank(Looping_Graph, alpha=0.85)
            print("rankingDict:",rankingDict)
        except networkx.exception.PowerIterationFailedConvergence:
            rankingDict = {}
            print("networkx.exception.PowerIterationFailedConvergence handled!")
            #Q: should i only add more score for the nodes in primary graph or all?!
            for node in Looping_Graph.nodes:
            # for node in G.nodes:
                rankingDict[node] =  rankingDict[node] + 0.2
            print("rankingDict after:",rankingDict)    
           
    #------------------------------------------------------------------
        disambiguated_rows = {}
        for index, row in T.iterrows():
            MaxRank = 0
            
            #we dont loop over row, just take label column entity
            rowcolval = row[labelColumn]
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
                            if (rankingDict[c] > MaxRank):
                                    print("The Max Rank:" , MaxRank, "The candidate:", c)
                                    MaxRank = rankingDict[c]
                                    disambiguated_entity = c
                if not(disambiguated_entity==None):
                        disambiguated_rows[rowcolval] = disambiguated_entity
                        Vertices[rowcolval] = [disambiguated_entity]
                        print("We changed the value of vertices in this loop")
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

    print(TPrimeIsAnnotated)
    print(TPrimeAnnotation)
    
    return(TPrimeIsAnnotated,TPrimeAnnotation)

