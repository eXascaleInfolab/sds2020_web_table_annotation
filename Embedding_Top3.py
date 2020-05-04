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
def getEmbedding(T):
    #----------------------Loading embedding model ------------------------
    #model min_count = 1
    model = LM.Model.load(models_directory="data/models/Model_Creation",
                       filename="3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1")
    
    #model min_count = 5
    # model = LM.Model.load(models_directory="data/models/Model_Creation",
    #                    filename="2-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=5")
    
    print(model.metadata)
    vector = model.wv.word_vec("Q19643")
    print(model.similarity("Q19643", "Q12097")) #queen , king

    #----------------------Loading pickle surface form ------------------------
    search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))

    #Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein_allcols.pickle", "rb"))    
    Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein_allcols.pickle", "rb"))    

    #Levenshtein_Limaye = pickle.load(open("data/LimayeLevenshtein.pickle", "rb"))    
    #Levenshtein_T2D = pickle.load(open("data/T2DLevenshtein.pickle", "rb"))    
    #--------------------------- Disambiguation ----------------------------------

    T, labelColumn, referenceColumns = PT.Preprocessing_Table(T)

    #Keep one Reference column and annotate with 2 columns at a time
    #first outer key = Ref Column index that we are using
    #inner key = Table row index
    RefBased_TPrimeAnnotation = defaultdict(lambda: defaultdict(list))

    TPrimeIsAnnotated = defaultdict(list) 
    TPrimeAnnotation = defaultdict(list)
    #keep top 3 annotation for metrics calculation
    # Top3_disambiguated_entity = [None,None,None]
    Top3_disambiguated_entity = defaultdict(list)

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
                        print("labelcolumn:",labelColumn)
                        print("RefColIndex:",RefColIndex)

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
                            results = Levenshtein_T2D[e]
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
                # for v in Vertices[v_k]:
                #     G.add_node(v)
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
                if(len(Vertices[v_k])==1 and v_k not in IsWithLevenshtein):
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

            Sigma_kInV_dict = {}
            #calculating the denominator and save in dict:
            for k1, k2 in itertools.permutations(Looping_Graph.nodes,2):
                if(k1 in Sigma_kInV_dict):
                    Sigma_kInV_dict[k1] += model.similarity("Q" + str(k1), "Q" + str(k2))
                else:
                    Sigma_kInV_dict[k1] = model.similarity("Q" + str(k1), "Q" + str(k2))

            #TODO: use itertools.permutations and merge two directions of edges
            #first direction of edges
            for k1, k2 in itertools.combinations(Keys_Looping, 2):
                try:
                    # check if there is unique candidate and give bigger weight.
                    #TODO : some lists have many candidates but all the same! add them too !
                    if(len(Vertices[k1])==1):
                        G.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3) + 0.7) })
                                    for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                    else:
                        G.add_edges_from(((u, v, {'weight':(round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3))})
                                    for u, v in itertools.product(Vertices[k1], Vertices[k2])))
                except KeyError:
                        continue
            #second direction of edges
            for k1, k2 in itertools.combinations(Keys_Looping, 2):
                try:
                    if(len(Vertices[k1])==1):
                        G.add_edges_from(((u, v, {'weight': (round( model.similarity("Q" + str(u), "Q" + str(v))/Sigma_kInV_dict[u],3)+0.7)})
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
                rankingDict = networkx.pagerank(Looping_Graph, alpha=0.85, max_iter = 50)
                print("rankingDict:",rankingDict)
            except networkx.exception.PowerIterationFailedConvergence:
                rankingDict = {}
                print("networkx.exception.PowerIterationFailedConvergence handled!")
                for node in Looping_Graph.nodes:
                    rankingDict[node] =  rankingDict[node] + 0.2
                print("rankingDict after:",rankingDict)
                
        #------------------------------------------------------------------
            disambiguated_rows = {}
            for index, row in T.iterrows():
                MaxRank = (0,0,0)
                for rowcolval in row:
                    #  keep the top 3 elements of the result
                        sorted(MaxRank)
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
                            #keeping top 3 elements
                            #tupples: (MaxRank, Its candidate)
                            disambiguated_entity = [(0,None),(0,None),(0,None)]
                            
                            for c in Vertices[rowcolval]:# c is the candidates of the mention e
                                #if it was one of the added nodes to the looping_graph
                                if(Looping_Graph.has_node(c)):
                                    print("and it exists in graph!")
                                    # compare to the smallest element.
                                    if (rankingDict[c] > disambiguated_entity[0][0]):

                                            disambiguated_entity[0] = (rankingDict[c],c)

                                            disambiguated_entity = sorted(disambiguated_entity)

                                            print("The Max Rank:" , disambiguated_entity, "The current candidate:", c)
                                            
                            #get the last 3 items stored as disambiguated. means the top 3 rank 
                            # disambiguated_entity = disambiguated_entity[-3:]
                            if not([x[1] for x in disambiguated_entity]==[None,None,None]):
                                    
                                    #now disambiguated_rows is dict of an array of 3 elems
                                    disambiguated_rows[rowcolval] = [x[1] for x in disambiguated_entity]
                                    Vertices[rowcolval] = []
                                    Top3_disambiguated_entity[rowcolval] = []

                                    #change the candidates with only top rank
                                    #if [2] is none, all others were none as well
                                    if not(disambiguated_entity[2][1]== None):
                                        
                                        #if the top 2 have the same rank
                                        if(disambiguated_entity[2][0]==disambiguated_entity[1][0]):
                                            #if the top 3 have the same rank
                                            if(disambiguated_entity[2][0]==disambiguated_entity[0][0]):
                                                #then sort based on id 
                                                disambiguated_entity = sorted( disambiguated_entity, key=lambda element : element[1] )
                                                
                                                #only add the thigh rank with the smallest ID 
                                                Vertices[rowcolval] = [disambiguated_entity[0][1]]
                                            else:
                                                #only top 2 have the same rank, choose the smaller ID between these two
                                                if(disambiguated_entity[2][1]>disambiguated_entity[1][1]):
                                                    Vertices[rowcolval] = [disambiguated_entity[1][1]]
                                                else:
                                                    Vertices[rowcolval] = [disambiguated_entity[2][1]]
                                        else:
                                            #the ranks are not the same, take only the top
                                            Vertices[rowcolval] = [disambiguated_entity[2][1]]


                                    for d_e in disambiguated_entity:
                                        #d_e is tupple: (MaxRank,Candidate)
                                        if not(d_e[1] == None):
                                            #Vertices[rowcolval].append(d_e)
                                            #but keep the top 3 for annotation
                                            Top3_disambiguated_entity[rowcolval].append(d_e[1])

                                    print("We changed the value of vertices in this loop to ",Vertices[rowcolval])
                                    print("Top3_disambiguated_entity[rowcolval]",Top3_disambiguated_entity[rowcolval])
                            else:
                                    disambiguated_rows[rowcolval] = [" "," "," "]

            print("Disambiguated rows are:" , disambiguated_rows)
            print("Top3_disambiguated_entity:" , Top3_disambiguated_entity)


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
                    if not(disambiguated_rows[rowcolval]== [" "," "," "]):
                        TPrimeIsAnnotated[index].append(True)
                        TPrimeAnnotation[index].append(disambiguated_rows[rowcolval])
                    else:
                        TPrimeIsAnnotated[index].append(False)
                        TPrimeAnnotation[index].append([" "," "," "])
                except KeyError:
                    print("rowcolval not exists in disambiguated_rows")
                    TPrimeIsAnnotated[index].append(False)
                    TPrimeAnnotation[index].append([" "," "," "]) 
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
        TPrimeAnnotation[annotation_key] = [" "," "," "]
    for isannotated_key in TPrimeAnnotation:
        TPrimeIsAnnotated[isannotated_key] = False


    #number of label column cant help cause we dont know if the previous columns still exist or not. 
    #so we should know this label column is which ith number in the existing columns
    print("RefBased_TPrimeAnnotation:",RefBased_TPrimeAnnotation)
    # To be cleaned:
    for RefColIndex in RefBased_TPrimeAnnotation:
        for Row_k in RefBased_TPrimeAnnotation[RefColIndex]:
                if(TPrimeAnnotation[Row_k][0]==" "):
                    #TODO: add loop instead of 0,1,2 index using for top 3 elements
                    if not(RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn][0]== " "):
                        TPrimeAnnotation[Row_k][0] = RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn][0]
                        TPrimeIsAnnotated[Row_k] = True
                    else:
                        TPrimeAnnotation[Row_k][0] = " "
                        # TPrimeIsAnnotated[Row_k] = False
                
                #----------------------------------------------
                if(TPrimeAnnotation[Row_k][1]==" "):    
                    if not(RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn][1]== " "):
                        TPrimeAnnotation[Row_k][1] = RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn][1]
                        TPrimeIsAnnotated[Row_k] = True
                    else:
                        TPrimeAnnotation[Row_k][1] = " "
                        # TPrimeIsAnnotated[Row_k] = False
                
                #----------------------------------------------
                if(TPrimeAnnotation[Row_k][2]==" "):
                    if not(RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn][2]== " "):
                        TPrimeAnnotation[Row_k][2] = RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn][2]
                        TPrimeIsAnnotated[Row_k] = True
                    else:
                        TPrimeAnnotation[Row_k][2] = " "
                        # TPrimeIsAnnotated[Row_k] = False

                if (TPrimeAnnotation == [" "," "," "]):
                    TPrimeIsAnnotated[Row_k] = False
            
            # if (TPrimeAnnotation[Row_k][0]!=" " and TPrimeAnnotation[Row_k][1]!=" " and TPrimeAnnotation[Row_k][2]!=" " ):
            # else:
            #     #we have annotation for the label column from previous Ref Col run
            #     #If they want add model similarity!! now it is the first Ref Col val
            #     continue


    print(TPrimeIsAnnotated)
    print(TPrimeAnnotation)
    print("----"*30)
    print("End of Embedding File")
    print("----"*30)
    return(TPrimeIsAnnotated,TPrimeAnnotation)

