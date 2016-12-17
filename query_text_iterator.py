###################################
## Import modules
###################################
import h5py
import pyodbc
import time, sys, os
import numpy as np
from nltk import word_tokenize, sent_tokenize

##############################################################
## Utility functions, global variables, and paths
##############################################################

def size_gb(a):
    return sys.getsizeof(a) / 1073741824

# Root directory for storing data
ROOT = "C:/Users/cmeaney/Desktop/Py_OA_TestEnv/pyodbc_query_results/"

##############################################################
## Establish a DB connection between Python and MS SQL Server
##############################################################
try:
    CONN = pyodbc.connect('DRIVER={SQL Server}; SERVER=EMRNONSQL1; '
                          'DATABASE=ProjectDBCM; UID=chris; PWD=XXXXX')
    print("Connected")
except:
    print("Not Connected")


#############################################################
## Query the DB and store returned queries as Python objects
#############################################################

def parse_text_vec(table, vec, filename):
    # Create query string for specific DB table and variable name
    query_string = "select d_ICES_patient_id, {} from ProjectDBCM.dbo.{}"\
                   .format(vec, table)
 	start = time.time()
    # Create connection cursor
    with CONN.cursor() as cursor:
        # For each row, store id and text (as is)
        out_list = [ row[:2] for row in cursor.execute(query_string) ]
	end = time.time()
    # Compute time taken and summarize memory usage of returned object
	print("Processing Time: {} sec".format(end - start))
	print("Returned object is: {:.6f} Gb".format(size_gb(out_list)))
	# Write processed data to HDF5 file
    with h5py.File(os.path.join(ROOT, filename), 'w') as f:
        shape = (len(out_list),)
        id_ds = f.create_dataset('id', shape, dtype='i8')
        tx_ds = f.create_dataset('tx', shape,
                                 dtype=h5py.special_dtype(vlen=str),
                                 compression="gzip")
        # Slower method, but memory tight
        for i, row in enumerate(out_list):
            id_ds[i] = row[0]
            tx_ds[i] = row[1]
	# Return processed data to user
	return out_list

test = parse_text_vec(table="OA_CPP", vec="d_treatments", filename="test.h5")

##
## Summary stats on processed text
##

# How many tokens per text vector/document
len_docs = np.array((len(doc[1]) for doc in test))
np.min(len_docs)
np.max(len_docs)
np.mean(len_docs)
