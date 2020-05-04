# import pickle

# search = pickle.load(open("data/surface/Surface_Lower_NoPunc.pickle", "rb"))

# try:
#     res = search['newyork']
#     print("results of entity newyork:",res)
# except Exception:
#     print("the entity newyork does not exist.")


# try:
#     res = search['new york']
#     print("results of entity new york:",res)
# except Exception:
#     print("the entity new york does not exist.")


# try:
#     res = search['NYC']
#     print("results of entity NYC",res)
# except Exception:
#     print("the entity NYC does not exist.")


# try:
#     res = search['newyorkcity']
#     print("results of entity newyorkcity:",res)
# except Exception:
#     print("the entity NYC does not exist.")


# try:
#     res = search['new york city']
#     print("results of entity new york city:",res)
# except Exception:
#     print("the entity new york city does not exist.")

#========---------========----------==========---------==========

import Hybrid_Limaye

List1 = 'file404549_0_cols1_rows20.csv'
TableList = [List1]
Hybrid_Limaye.RunHybrid(TableList)