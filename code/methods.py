import pandas as pd
import time
import os
import glob
from decouple import config, Csv
import FullPhase as fp
import PrecisionRecallF1 as PRF
import metrics_calcul_alldatasets as MCal
import sys
import copy

DATASET_NAME = config('DATASET_NAME', cast=str)
LIMAYE_DIR = config('LIMAYE_DIR', cast=str)
T2D_DIR = config('T2D_DIR', cast=str)
METHODS = config('METHODS', cast=Csv(str))
BASE_DIR = config('BASE_DIR', cast=str)
EMBEDDING_METHOD = config('EMBEDDING_METHOD', cast=str)

if EMBEDDING_METHOD == 'Baseline_Embedding':
    import Embedding_Baseline as emb
elif EMBEDDING_METHOD == 'Looping_Embedding':
    import Embedding as emb
else:
    print("Wrong env var for Embedding method.")
    sys.exit()

def run_methods(TableList):

    # contains prediction results
    RLU_precision = 0
    RLU_recall = 0
    RLU_F1 = 0
    RLU_TP_Total = 0
    RLU_FN_Total = 0
    RLU_FP_Total = 0
    # ---------------
    Emb_precision = 0
    Emb_recall = 0
    Emb_F1 = 0
    Emb_TP_Total = 0
    Emb_FN_Total = 0
    Emb_FP_Total = 0
    # ---------------
    HybridI_precision = 0
    HybridI_recall = 0
    HybridI_F1 = 0
    HybridI_TP_Total = 0
    HybridI_FN_Total = 0
    HybridI_FP_Total = 0
    # ---------------
    HybridII_precision = 0
    HybridII_recall = 0
    HybridII_F1 = 0
    HybridII_TP_Total = 0
    HybridII_FN_Total = 0
    HybridII_FP_Total = 0

    if DATASET_NAME == 'Limaye':
        # change the path to  have csv filename without the path inside
        os.chdir(LIMAYE_DIR + 'tables_instance/')
    elif DATASET_NAME == 'T2D':
        # change the path to  have csv filename without the path inside
        os.chdir(T2D_DIR + 'tables_instance/')

    start_time = time.time()
    # keeps table files
    T = pd.DataFrame()
    i = 0

    for table_csv in TableList:
        os.chdir(LIMAYE_DIR + 'tables_instance/')
        with open(table_csv, 'r', encoding='utf-8'):
            print("This is the Table file name :\n\n", table_csv)

            T = pd.read_csv(table_csv, header=None)
            # switch path to normal form
            os.chdir(BASE_DIR)

            # Remove the row if there is no GT in entity file for it.
            entity_csv = table_csv
            os.chdir(LIMAYE_DIR + 'entities_instance/')
            with open(entity_csv, 'r', encoding='utf-8'):
                print("This is the Entity file name :\n\n", entity_csv)
                E = pd.read_csv(entity_csv, header=None)
                count_row = T.shape[0]
                for i in range(0, count_row):
                    if not(i in E[E.columns[-1]].values):
                        print("Row ", i, "does not have GT. Entity removed.")
                        T = T.drop([i])

            # switch path to normal form
            os.chdir(BASE_DIR)
            # ----------------------------------------------------------------
            # Refined Lookup
            # ----------------------------------------------------------------
            if('Refined_Lookup' in METHODS):
                RLU_TPrimeIsAnnotated, RLU_TPrimeAnnotation = \
                    fp.getFullPhase(T, table_csv)
                print("Refined Lookup result:")
                print("-"*30)
                print(RLU_TPrimeAnnotation)
                print(RLU_TPrimeIsAnnotated)

            # ----------------------------------------------------------------
            # Embedding
            # ----------------------------------------------------------------
            if('Embedding' in METHODS):
                Emb_TPrimeIsAnnotated, Emb_TPrimeAnnotation = \
                    emb.getEmbedding(T)
                print("Embedding result:")
                print("-"*30)
                print(Emb_TPrimeAnnotation)
                print(Emb_TPrimeIsAnnotated)

            # ----------------------------------------------------------------
            # Hybrid I(you need also embedding and RLU)
            # ----------------------------------------------------------------
            if('Hybrid_I' in METHODS):
                HybridI_TPrimeIsAnnotated = copy.deepcopy(
                                                        RLU_TPrimeIsAnnotated)
                HybridI_TPrimeAnnotation = copy.deepcopy(
                                                        RLU_TPrimeAnnotation)

                for k in HybridI_TPrimeIsAnnotated:
                    if(HybridI_TPrimeIsAnnotated[k]):
                        continue
                    elif not(HybridI_TPrimeIsAnnotated[k]):
                        try:
                            HybridI_TPrimeIsAnnotated[k] = \
                                Emb_TPrimeIsAnnotated[k]
                            HybridI_TPrimeAnnotation[k] = \
                                Emb_TPrimeAnnotation[k]
                        except KeyError:
                            HybridI_TPrimeIsAnnotated[k] = False
                            HybridI_TPrimeAnnotation[k] = ' '

                print("Hybrid I result:")
                print("-"*30)
                print(HybridI_TPrimeAnnotation)
                print(HybridI_TPrimeIsAnnotated)

            # ----------------------------------------------------------------
            # Hybrid II(you need also embedding and RLU)
            # ----------------------------------------------------------------
            if('Hybrid_I' in METHODS):
                HybridII_TPrimeIsAnnotated = copy.deepcopy(
                                                        Emb_TPrimeIsAnnotated)
                HybridII_TPrimeAnnotation = copy.deepcopy(
                                                        Emb_TPrimeAnnotation)

                for k in HybridII_TPrimeIsAnnotated:
                    if HybridII_TPrimeIsAnnotated[k]:
                        continue
                    elif not HybridII_TPrimeIsAnnotated[k]:
                        try:
                            HybridII_TPrimeIsAnnotated[k] = \
                                RLU_TPrimeIsAnnotated[k]
                            HybridII_TPrimeAnnotation[k] = \
                                RLU_TPrimeAnnotation[k]
                        except KeyError:
                            HybridII_TPrimeIsAnnotated[k] = False
                            HybridII_TPrimeAnnotation[k] = ' '

                print("Hybrid II result:")
                print("-"*30)
                print(HybridII_TPrimeAnnotation)
                print(HybridII_TPrimeIsAnnotated)

            # ----------------------------------------------------------------
            # ----------------------------------------------------------------
            # Metrics
            # ----------------------------------------------------------------
            # ----------------------------------------------------------------
            # Hybrid I metrics calculation
            # ----------------------------------------------------------------
            if('Hybrid_I' in METHODS):
                HybridI_TP, HybridI_FN, HybridI_FP =  \
                    MCal.metrics_calcul(
                        T,
                        table_csv,
                        HybridI_TPrimeAnnotation,
                        HybridI_TPrimeIsAnnotated)
                HybridI_TP_Total = HybridI_TP_Total+HybridI_TP
                HybridI_FN_Total = HybridI_FN_Total+HybridI_FN
                HybridI_FP_Total = HybridI_FP_Total+HybridI_FP
                HybridI_precision, HybridI_recall, HybridI_F1 = \
                    PRF.Precision_Recall_F1(
                        HybridI_TP_Total,
                        HybridI_FN_Total,
                        HybridI_FP_Total)
            # ----------------------------------------------------------------
            # Hybrid II metrics calculation
            # ----------------------------------------------------------------
            if('Hybrid_II' in METHODS):
                HybridII_TP, HybridII_FN, HybridII_FP = \
                    MCal.metrics_calcul(
                        T,
                        table_csv,
                        HybridII_TPrimeAnnotation,
                        HybridII_TPrimeIsAnnotated)
                HybridII_TP_Total = HybridII_TP_Total+HybridII_TP
                HybridII_FN_Total = HybridII_FN_Total+HybridII_FN
                HybridII_FP_Total = HybridII_FP_Total+HybridII_FP
                HybridII_precision, HybridII_recall, HybridII_F1 = \
                    PRF.Precision_Recall_F1(
                        HybridII_TP_Total,
                        HybridII_FN_Total,
                        HybridII_FP_Total)
            # ----------------------------------------------------------------
            # Refined Lookup metrics calculation
            # ----------------------------------------------------------------
            if('Refined_Lookup' in METHODS):
                RLU_TP, RLU_FN, RLU_FP =  \
                    MCal.metrics_calcul(
                        T,
                        table_csv,
                        RLU_TPrimeAnnotation,
                        RLU_TPrimeIsAnnotated)
                RLU_TP_Total = RLU_TP_Total + RLU_TP
                RLU_FN_Total = RLU_FN_Total+RLU_FN
                RLU_FP_Total = RLU_FP_Total+RLU_FP
                RLU_precision, RLU_recall, RLU_F1 = \
                    PRF.Precision_Recall_F1(
                        RLU_TP_Total,
                        RLU_FN_Total,
                        RLU_FP_Total)

            # ----------------------------------------------------------------
            # Embedding
            # ----------------------------------------------------------------
            if('Embedding' in METHODS):
                Emb_TP, Emb_FN, Emb_FP =  \
                    MCal.metrics_calcul(
                        T,
                        table_csv,
                        Emb_TPrimeAnnotation,
                        Emb_TPrimeIsAnnotated)
                Emb_TP_Total = Emb_TP_Total+Emb_TP
                Emb_FN_Total = Emb_FN_Total+Emb_FN
                Emb_FP_Total = Emb_FP_Total+Emb_FP
                Emb_precision, Emb_recall, Emb_F1 = \
                    PRF.Precision_Recall_F1(
                        Emb_TP_Total,
                        Emb_FN_Total,
                        Emb_FP_Total)
    # TODO: add to csv file
    # ------------------------------------------------------------------------------------------
    print("Final result of the core partition")
    print("-"*30)
    # ----------------------------------------------------------------
    if 'Refined_Lookup' in METHODS:
        print("Final Refined Lookup:")
        print("Final RLU Precision : ", RLU_precision)
        print("Final RLU Recall : ", RLU_recall)
        print("Final RLU F1 : ", RLU_F1)
        print("-"*30)
        print("Final RLU FN ", RLU_FN_Total)
        print("Final RLU FP ", RLU_FP_Total)
        print("Final RLU TP ", RLU_TP_Total)
        print("-"*30)
    # ----------------------------------------------------------------
    if 'Embedding' in METHODS:
        print("Embedding:")
        print("Final Embedding Precision : ", Emb_precision)
        print("Final Embedding Recall : ", Emb_recall)
        print("Final Embedding F1 : ", Emb_F1)
        print("-"*30)
        print("Final Embedding FN ", Emb_FN_Total)
        print("Final Embedding FP ", Emb_FP_Total)
        print("Final Embedding TP ", Emb_TP_Total)
        print("-"*30)
    # ----------------------------------------------------------------
    if 'Hybrid_I' in METHODS:
        print("Hybrid I:")
        print("Final Hybrid I Precision : ", HybridI_precision)
        print("Final Hybrid I Recall : ", HybridI_recall)
        print("Final Hybrid I F1 : ", HybridI_F1)
        print("-"*30)
        print("Final Hybrid I FN ",	HybridI_FN_Total)
        print("Final Hybrid I FP ",	HybridI_FP_Total)
        print("Final Hybrid I TP ",	HybridI_TP_Total)
        print("-"*30)
    # ----------------------------------------------------------------
    if 'Hybrid_II' in METHODS:
        print("Hybrid II:")
        print("Final Hybrid II Precision : ", HybridII_precision)
        print("Final Hybrid II Recall : ", HybridII_recall)
        print("Final Hybrid II F1 : ", HybridII_F1)
        print("-"*30)
        print("Final Hybrid II FN ", HybridII_FN_Total)
        print("Final Hybrid II FP ", HybridII_FP_Total)
        print("Final Hybrid II TP ", HybridII_TP_Total)
        print("-"*30)
        print("-"*30)
        print("Run time for 1 lookup phase and one embedding phase"
              "Local Endpoint ", time.time() - start_time)
        print("-"*30)
    return
