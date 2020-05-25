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
import methods

List1 = 'file404549_0_cols1_rows20.csv'
TableList = [List1]
methods.run_methods(TableList)
#========---------========----------==========---------==========
# import pickle

# a = {'hello': 'world'}

# with open('data/surface/Surface_Lower_NoPunc.pickle', 'wb') as handle:
#     pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)

# # with open('filename.pickle', 'rb') as handle:
# #     b = pickle.load(handle)

# # print a == b