


# del  ilostat



# import  inspect
#  a = inspect.getsource(ilostat.get_ilostat_toc)
# print(a) 


import os
import sys
import importlib
# get current work directory
cwd = os.getcwd()

# change wk directory
# os.chdir('H:/_Sys/packages/ilostat.py/ilostat/')
os.chdir('C:/temp/ilostat.py/ilostat/')

import ilostat


# importlib.reload(sys.modules['ilostat'])

# back to current work directory
os.chdir(cwd)


id = ['UNE_TUNE_SEX_AGE_NB_M']
segment = 'indicator'
cache = True
cache_update = True
cache_dir = 'C:/temp/test'
cache_format = 'csv.gz'
quiet = True

collection = [] 
ref_area = [] 
source = []
indicator = []
sex = []
classif1 = []
classif2 = []
time = []
obs_value = []
obs_status = []
note_classif = []
note_indicator = []
note_source = []


filters = [collection, ref_area, source, indicator, sex, classif1, classif2, time, obs_value, obs_status, note_classif, note_indicator, note_source]

res = ilostat.get_ilostat(id, segment, filters, cache, cache_update, cache_dir, cache_format, quiet)


dic_ref_area = ilostat.get_ilostat_dic('ref_area')
toc = ilostat.get_ilostat_toc('ref_area')


ilostat.label_ilostat(res, dic = 'indicator')






# garbage
import gc
gc.collect()