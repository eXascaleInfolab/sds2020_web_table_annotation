from decouple import config
import pathlib
import multiprocessing
import time
# TODO: change it to 1 Hybrid file for all datasets.
import Hybrid_Limaye


# set env variables
DEBUG = config('DEBUG', cast=bool)
BASE_Dir = config('BASE_Dir', cast=str)
DATASET_NAME = config('DATASET_NAME', cast=str)
LIMAYE_DIR = config('LIMAYE_DIR', cast=str)


# functions
def get_table_names(ds_name, directory, number_of_cores):
    """ splits the table names to multiple groups
        for multiprocessing and to run them concurrently."""

    filenames = pathlib.Path(directory)
    filenames = [str.replace(str(f), directory, '')
                 for f in filenames.iterdir()]
    filenames = [f for f in filenames
                 if not f.startswith('.')]

    # divide table names to groups
    # (number of available cores)
    tables_in_groups = int(len(filenames) / number_of_cores)+1
    table_lists = [filenames[i:i+tables_in_groups]
                   for i in range(0, len(filenames), tables_in_groups)]

    return table_lists


# read
if DATASET_NAME == 'Limaye':
    # read table file names, give the number of cores
    # you want to be used for the multiprocessing
    table_lists = get_table_names(DATASET_NAME, LIMAYE_DIR, 13)
    print(table_lists)

elif DATASET_NAME == 'T2D':
    print("T2D")

elif DATASET_NAME == 'Wikipedia':
    print("Wikipedia")

else:
    print("Dataset name not found.")


if __name__ == '__main__':
    start_time = time.time()
    pool = multiprocessing.Pool()
    pool.map(Hybrid_Limaye.RunHybrid, table_lists)
    pool.close()
    print('That took {} seconds'.format(time.time() - start_time))
