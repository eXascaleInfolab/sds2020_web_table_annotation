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
    
    Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein_allcols.pickle", "rb"))    
#     Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein_allcols.pickle", "rb"))    

    #only label column entities
    #Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein.pickle", "rb"))    
    #Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein.pickle", "rb"))    
   
    #--------------------------- Disambiguation ----------------------------------

   


    T, labelColumn, referenceColumns = PT.Preprocessing_Table(T)

    #all the text fields in the table --> e
    Entities = []
    for index, row in T.iterrows():
            for e in row:
                #just put label column entity    
                if(e == row[labelColumn]):
                    try:
                        if(len(e)>3):
                            Entities.append(e)
                    except Exception:
                        print("Cant get length")
                        continue

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
                except (IndexError,KeyError) as err:
                        print("no result from levenshtein on SF")
                        continue
                continue

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
    G = networkx.DiGraph()
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
            G.add_edges_from(((u, v, {'weight':round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3) })
                    for u, v in itertools.product(Vertices[k1], Vertices[k2])))
    
    #second direction of edges
    for k1, k2 in itertools.combinations(Keys, 2):
            G.add_edges_from(((u, v, {'weight': round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3) })
                    for u, v in itertools.product(Vertices[k2], Vertices[k1])))
    
    print(G.edges(data=True))
    

    #Adding weighted pagerank
    try:
        rankingDict = networkx.pagerank(G, alpha=0.85, max_iter = 50)
        print("rankingDict:",rankingDict)
    except networkx.exception.PowerIterationFailedConvergence:
        rankingDict = {}
        print("networkx.exception.PowerIterationFailedConvergence handled!")   
        
        for node in G.nodes:
            rankingDict[node] =  0.1     
    #------------------------------------------------------------------
    disambiguated_rows = {}
    for index, row in T.iterrows():
            MaxRank = 0
            #for rowcolval in row:
            rowcolval = row[labelColumn]
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
                            if (rankingDict[c] > MaxRank):
                                    print("The Max Rank:" , MaxRank, "The candidate:", c)
                                    MaxRank = rankingDict[c]
                                    disambiguated_entity = c
                    if not(disambiguated_entity==None):
                            disambiguated_rows[rowcolval] = disambiguated_entity
                    else:
                            disambiguated_rows[rowcolval] = " "
    print("Disambiguated rows are: " , disambiguated_rows)

    TPrimeIsAnnotated = defaultdict(list) 
    TPrimeAnnotation = defaultdict(list)

    for index, row in T.iterrows():
            rowcolval = row[labelColumn]
            if(pd.isnull(rowcolval)):
                rowcolval = ""
            rowcolval = rowcolval.lower()
            rowcolval = re.sub("\s\s+" , " ", rowcolval)
            if(rowcolval):
            #for rowcolval in row:
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
    
    #change the list to entry - we keep annotation only for the labelcolumn
    for k in TPrimeAnnotation:
            TPrimeAnnotation[k] = TPrimeAnnotation[k][0]
    for k in TPrimeIsAnnotated:
            TPrimeIsAnnotated[k] = TPrimeIsAnnotated[k][0]

    return(TPrimeIsAnnotated,TPrimeAnnotation)

