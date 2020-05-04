
# import pickle 
# import loadmodel as LM

#metal gear solid :its result: [2713469, 3855447, 6582527]
#next annotated entity : konami
#126: [' ', 45700, ' ']
#row in the table: psx	konami	metal gear solid	2.81

# model = LM.Model.load(models_directory="data/models/Model_Creation",
#                        filename="3-wikidata-20190229-truthy-BETA-cbow-size=100-window=1-min_count=1")

# try:
#     print("Similarity of konami with 2713469:",model.similarity("Q45700", "Q2713469" ))
# except Exception:
#     print("id not found")
# try:
#     print("Similarity of konami with 2713469:",model.similarity("Q45700", "Q3855447" ))
# except Exception:
#     print("id not found")
# try:
#     print("Similarity of konami with 2713469:",model.similarity("Q45700", "Q6582527" )) #correct one!, not annotated now!
# except Exception:
#     print("id not found")


#========---------========----------==========---------==========
# from collections import defaultdict
# import pandas as pd


# T = pd.DataFrame({'Country': [1,2,3,4],
# 	           'Population': [1000,2000,3000,4000],
# 	           'Capital': [100,200,300,400]})

# mydict = defaultdict(list)

# mydict = {'a':[1,'',3],'b':[4,'',''],'c':['','5','6']}
# LabelColumn = 2

# for k in mydict:
#     if(T[k][LabelColumn]==)

#========---------========----------==========---------==========
# from collections import defaultdict

# TPrimeIsAnnotated = defaultdict(list) 
# TPrimeIsAnnotated = {0: [True, False, False, False], 1: [True, False, False, False], 2: [True, False, False, False], 3: [True, False, False, False], 4: [True, False, False, False], 5: [True, False, False, False]}
# TPrimeAnnotation = defaultdict(list)
# TPrimeAnnotation = {0: [1419, ' ', ' ', ' '], 1: [1421, ' ', ' ', ' '], 2: [1423, ' ', ' ', ' '], 3: [1645078, ' ', ' ', ' '], 4: [1425, ' ', ' ', ' '], 5: [1427, ' ', ' ', ' ']}
# labelColumn = 0
# RefBased_TPrimeAnnotation = defaultdict(lambda: defaultdict(list))

# RefBased_TPrimeAnnotation = {2:{0: [1419, 1421, ' ', ' '], 1: [1421, ' ', ' ', ' '], 2: [1423, ' ', ' ', ' '], 3: [1645078, ' ', ' ', ' '], 4: [1425, ' ', ' ', ' '], 5: [1427, ' ', ' ', ' ']},
#                              3:{0: [1419, ' ', ' ', ' '], 1: [1421, ' ', ' ', ' '], 2: [1423, ' ', ' ', ' '], 3: [1645078, ' ', ' ', ' '], 4: [1425, ' ', ' ', ' '], 5: [1427, ' ', ' ', ' ']},                           
#                              4:{0: [1419, ' ', ' ', ' '], 1: [1421, ' ', ' ', ' '], 2: [1423, ' ', ' ', ' '], 3: [1645078, ' ', ' ', ' '], 4: [1425, ' ', ' ', ' '], 5: [1427, ' ', ' ', ' ']}
#                             }
# for annotation_key in TPrimeAnnotation:
#     TPrimeAnnotation[annotation_key] = " "
# for isannotated_key in TPrimeAnnotation:
#     TPrimeIsAnnotated[isannotated_key] = False

# for RefColIndex in RefBased_TPrimeAnnotation:
#         for Row_k in RefBased_TPrimeAnnotation[RefColIndex]:
#             if(TPrimeAnnotation[Row_k] == " "):
#                 if not(RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn]== " "):
#                     print("RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn]",RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn])
#                     TPrimeAnnotation[Row_k] = RefBased_TPrimeAnnotation[RefColIndex][Row_k][labelColumn]
#                     print("TPrimeAnnotation[Row_k]",TPrimeAnnotation[Row_k])
#                     TPrimeIsAnnotated[Row_k] = True
#             else: 
#                 # we have annotation for the label column from previous Ref Col run
#                 # If they want add model similarity!! now it is the first Ref Col val
#                 continue
# print("TPrimeAnnotation",TPrimeAnnotation)
# print("TPrimeIsAnnotated",TPrimeIsAnnotated)
#========---------========----------==========---------==========

# # from collections import Counter

# # c = Counter([])

# # print("most common:",c.most_common())

# from collections import defaultdict

# d = defaultdict(list)

# d["a"] = ['1']

# print("d",d)

# d = defaultdict(list)

# print("d2",d)

#========---------========----------==========---------==========
# import pandas as pd
# import copy
# Entities = []
# labelColumn = 1
# RefColIndexList = [0,2]
# T = pd.DataFrame({0: ['a','b','c','d'],
#                1: ['Dominica','Tajikistan','Djibouti','Gabon'],
# 	           2: [1000,2000,3000,4000],
# 	           3: ['Roseau','Dushanbe','Djibouti','Libreville']})

# T = T.drop(0, axis=1)
# T = T.T.reset_index(drop=True).T

# print("Table:",T)
# for RefColIndex in RefColIndexList:
#     for index, row in T.iterrows():
#         for e in row:
#             if(e == row[labelColumn] or e == row[RefColIndex]):
#                 try:
#                     if(len(e)>3):
#                     # #just put label column entity    
#                     # if(e == row[labelColumn]):
#                             Entities.append(e)
#                 except Exception:
#                         print("Cant get length")
#                         continue

# print("Entities",Entities)
# print("Hello outside")
#========---------========----------==========---------==========