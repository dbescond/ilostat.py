"""
Module ilostat
"""

import pandas
import os
import numpy
import tempfile
import requests
import shutil
import collections

ilostat_cols_ref = ['collection', 'collection.label', 'ref_area', 'ref_area.label', 'source', 'source.label', 'indicator', 'indicator.label', 
                    'sex', 'sex.label', 'classif1', 'classif1.label', 'classif2', 'classif2.label', "time", 'obs_value', 'obs_status', 'obs_status.label', 
                    'note_classif', 'note_classif.label', 'note_indicator', 'note_indicator.label', 'note_source', 'note_source.label']

def get_ilostat(id,
                segment = 'indicator',
                filters = [],
                cache = True,
                cache_update = True,
                cache_dir = [],
                cache_format = 'csv.gz',
                quiet = False):
  """
  Download datasets from ilostat www.ilo.org/ilostat via bulk download facility
  http://www.ilo.org/ilostat-files/WEB_bulk_download/html/bulk_main.html.
    param id: A code name for the dataset of interest.
           See get_ilostat_toc or details for how to get code.
    param segment: A character, way to get datasets by: "indicator" (default) or "ref_area"., 
    param type a character, type of variables, "code" (default), "label" or "both",
    param lang: a character, code for language. Available are "en" (default), 
           "fr" and "es",
    param time_format a string giving a type of the conversion of the time
           column from the ilostat format. "raw" (default)
           does not do conversion and return time as character (ie. '2017', '2017Q1', '2017M01'). A "date" converted to
           a Date with a first date of the period. A "date_last" converted to a Date with
           a last date of the period and "num" converted to a numeric,
    param filters: a list; [] (default) to get a whole dataset or a list of
              filters to get just part of the table. list objects are
              list values vectors of observation codes ordering by ilostat variable codes.
              filters detect on variables, so could be partial, ie. [[], [], [] [], 'T'] is
              enough but equivalent to [[], [], [] [], 'SEX_T'].
    param fixed a logical, if True (default), filters arguments pattern is a string to be matched as is,
           Change to FALSE if more complex regex matching is needed.
    param detail a character, 'full' (default), whether 'serieskeysonly', return
              only key, no data, no notes or dataonly return key and data, no notes or 'bestsourceonly' return 
           best source only at the series key levels,
    param cache a logical whether to do caching. Default is TRUE. Affects
              only queries from the ilostat bulk download facility,
    param cache_update a logical whether to update cache. Check cache update with last.update store on the cache file name 
              and the one from the table of contents. Default is TRUE,  
    param cache_dir: a path to a cache directory. The directory has to exist.
              The NULL (default) uses and creates
              'ilostat' directory in the temporary directory from tempdir,
    param cache_format: a character, format to store on the cache "rds" (default), but also "csv", "dta", 
           "sav", "sas7bdat". useful for getting ilostat dataset directly on the cache_dir without R,
    param back a logical, TRUE return dataframe on R or not FALSE, useful for just saving file in specific cache_format,
    param cmd a character, R expression use for manipulate internal data frame dat object applied to each datasets retrieved 
           after filters and type setting. Manipulation should return data.frame 'none' (default),
           If use, cache is set to FALSE. see examples. 
    param quiet: a logical, if True , don't return message from processing, False (default),

    author David Bescond email:bescondilo.org

    section others:
    Data sets are downloaded from the
    ilostat bulk download facility. 
    If only the table \code{id} is given, the whole table is downloaded from the
    bulk download facility.

    The bulk download facility is the fastest method to download whole datasets.
    It is also often the only way as the sdmx API has limitation of maximum 
    300 000 records at the same time and whole datasets usually exceeds that. 

    By default datasets from the bulk download facility are cached as they are
    often rather large.

    Cache files are stored in a temporary directory by default or in
    a named directory if cache_dir or option ilostat_cache_dir is defined.
    The cache can be emptied with \link{clean_ilostat_cache.

    The id, a code, for the dataset can be searched with
    the get_ilostat_toc or from the [bulk download facility](http://www.ilo.org/ilostat-files/WEB_bulk_download/html/bulk_main.html).
  """


# get multi id from data frame of list

  if type(id) == pandas.core.frame.DataFrame:

    ref_id = id.id.unique()
    ref_id = list(ref_id)

    if not len(ref_id) > 1:

      ref_id = ref_id[0]

#manage exception modelled estimates

  if not segment.lower().find('model') == -1:

    segment = 'modelled_estimates'
    lang = 'en'


  if type(id) == str:

    dat = get_ilostat_dat(id, segment, filters, cache, cache_update, cache_dir, cache_format, quiet)

  if type(id) == list:

    dat = get_ilostat_dat(id[0], segment, filters, cache, cache_update, cache_dir, cache_format, quiet)

    if len(id) > 1:

      for i in range(1, len(id)):

        dat = dat.append(get_ilostat_dat(id[i], segment, filters, cache, cache_update, cache_dir, cache_format, quiet), ignore_index=True)


  return dat









def get_ilostat_dat(id,
                    segment,
                    filters,
                    cache,
                    cache_update,
                    cache_dir,
                    cache_format,
                    quiet):



  dat = pandas.DataFrame()

  # check id validity and return last update

  last_toc_update = get_ilostat_toc(segment,
                                    lang = 'en',
                                    search = [],
                                    filters = [id] )['last.update']

  if last_toc_update.empty:

    if not quiet:

      print('Dataset with id = ' + id + 'does not exist or is not readable')

    return dat

  if not last_toc_update.empty:

    last_toc_update = last_toc_update.unique()[0]

    last_toc_update = last_toc_update[6:10] + last_toc_update[3:5] + last_toc_update[0:2]

  if not cache_dir :

    cache_dir = tempfile.gettempdir() + '/ilostat'

  if not os.path.isdir(cache_dir):
     os.makedirs(cache_dir)

  cache_file = cache_dir + '/'+ segment + '-' + id + '-' + last_toc_update + '.' + cache_format

  if cache:
    if cache_update and os.path.isfile(cache_file):

      cache_update = False

      if not quiet:

        print('Table ' + id + ' is up to date')




# if cache id False or update or new: download else read from cache

  if not cache or cache_update or not os.path.isfile(cache_file):

    dat = get_ilostat_raw(id, segment, cache_file, cache_dir, cache_format, quiet)

  else:

    dat = pandas.read_csv(cache_file, compression = 'infer', engine  = 'c', dtype = {'collection': str ,'ref_area': str, 'source': str, 'indicator': str, 'sex': str, 'classif1': str, 'classif2': str, 'time': str, 'obs_value':  numpy.float32, 'obs_status': str, 'note_classif': str, 'note_indicator': str, 'note_source': str})

    if not quiet:

      print('Table ' + id + ' read from cache file: ' + os.path.dirname(cache_file))

  if filters:

    test_cols = min(len(dat.columns), len(filters))
    try:

      for index in range(test_cols):

        test_filter = filters[index]

        if test_filter:

          test_item = '|'.join(test_filter)

          dat = dat[dat.iloc[:, index].str.contains(test_item)]

          del test_item

        del test_filter

    except OSError:

      pass


  return dat








def get_ilostat_raw(id,
                    segment,
                    cache_file,
                    cache_dir,
                    cache_format,
                    quiet):



  dat = pandas.DataFrame()


  if segment == 'modelled_estimates':

    base = 'http://www.ilo.org/ilostat-files/WEB_bulk_download/' + segment + '/' + id + '.dta'

    try:

      dat = pandas.read_stata(base)

    except IOError:

      if not quiet:

        print('Dataset with id = ' + id + ' does not exist or is not readable')

      os.remove(tfile)

  else:

    base = 'http://www.ilo.org/ilostat-files/WEB_bulk_download/' + segment + '/' + id + '.csv.gz'

    tfile = cache_file.replace('.' + cache_format, '.csv.gz')

  # download read file
    r = requests.get(base, stream=True)
    with open(tfile, 'wb') as f:
      for chunk in r.iter_content(chunk_size=1024): 
        if chunk: 
          f.write(chunk)

  # read file
    try:  
      dat = pandas.read_csv(tfile, compression = 'infer', engine  = 'c', dtype = {'collection': str ,'ref_area': str, 'source': str, 'indicator': str, 'sex': str, 'classif1': str, 'classif2': str, 'time': str, 'obs_value':  numpy.float32, 'obs_status': str, 'note_classif': str, 'note_indicator': str, 'note_source': str})

      if not cache_format == 'csv.gz':

        os.remove(tfile)  

    except IOError:

      if not quiet:

        print('Dataset with id = ' + id + ' does not exist or is not readable')

      os.remove(tfile)

      return pandas.DataFrame()

  if not quiet:

    print('http://www.ilo.org/ilostat-files/WEB_bulk_download/' + segment + '/' + id + '.' + cache_format)




  return dat









def get_ilostat_toc(segment = 'indicator',
                    lang = 'en',
                    search = [],
                    filters = []):
  """
  Read Ilostat Table of Contents
  Download one table of contents from ilostat www.ilo.org/ilostat via bulk download facility 
  http://www.ilo.org/ilostat-files/WEB_bulk_download/html/bulk_main.html.
   param segment A character, way to get datasets by: "indicator" (default) or "ref_area", 
         Can be set also with options(ilostat_segment = 'ref_area'),
   param lang a character, code for language. Available are "en" (default), 
         "fr" and "es". Can be set also with options(ilostat_lang = 'fr'),
   param search a character vector, "none" (default), datasets with this pattern in
      the description will be returned,
         characters vector will be use as AND, Character with '|' as OR, see example,
         options(ilostat_time_format = 'date'),  
   param filters a list; "none" (default) to get a whole toc or a named list of
            filters to get just part of the table. Names of list objects are
            ilostat toc variable codes and values are vectors of observation codes.
            filters detect on variables.
   param fixed a logical, if TRUE (default), pattern is a string to be matched as is,
         Change to FALSE if more complex regex matching is needed.
   author David Bescond bescond ilo.org
   return A tibble with ten columns depending of the segment: indicator or ref_area

       id : The codename of dataset of theme, will be used by the get_ilostat and get_ilostat_raw functions,
       indicator or ref_area : The indicator or ref_area code of dataset,
       indicator.label or ref_area.label : The indicator or ref_area name of dataset,
       freq  : The frequency code of dataset,
       freq.label : Is freq name of dataset,
       size : Size of the csv.gz files,
       data.start : First time period of the dataset, 
       data.end : Last time period of the dataset,
       last.update : Last update of the dataset,
       ... : Others relevant information

  details The TOC in English by indicator is downloaded from http://www.ilo.org/ilostat-files/WEB_bulk_download/indicator/table_of_contents_en.csv. 
  The values in column 'id' should be used to download a selected dataset.
  details The TOC in English by ref_area is downloaded from http://www.ilo.org/ilostat-files/WEB_bulk_download/ref_area/table_of_contents_en.csv. 
  The values in column 'id' should be used to download a selected dataset.
  details The TOC in English by modelled_estimates is downloaded from http://www.ilo.org/ilostat-files/WEB_bulk_download/modelled_estimates/table_of_contents_en.csv. 
  The values in column 'id' should be used to download a selected dataset.
  references
  See citation("Rilostat")
  ilostat bulk download facility user guidelines http://www.ilo.org/ilostat-files/WEB_bulk_download/ILOSTAT_BulkDownload_Guidelines.pdf

  """



  toc = pandas.DataFrame()

  if not segment.lower().find('model') == -1:

    segment = 'modelled_estimates'
    lang = 'en'


  toc = pandas.read_csv('http://www.ilo.org/ilostat-files/WEB_bulk_download/' + segment + '/' + 'table_of_contents_' + lang + '.csv', engine  = 'c', dtype = {'id': str ,'indicator': str, 'indicator.label': str, 'freq': str, 'freq.label': str, 'size': str, 'data.start': str, 'data.end': str, 'last.update': str, 'n.records': numpy.int64, 'collection': str, 'collection.label': str, 'subject': str, 'subject.label': str})

  # print('http://www.ilo.org/ilostat-files/WEB_bulk_download/' + segment + '/' + 'table_of_contents_' + lang + '.csv')

  if not type(toc) == pandas.core.frame.DataFrame:

    print('the toc file : ' + 'http://www.ilo.org/ilostat-files/WEB_bulk_download/' + segment + '/' + 'table_of_contents_' + lang + '.csv does not exist')

  if search:

    if type(search) == list:
      newsearch = ' and '.join(search)
    else:
      newsearch = search

    print(newsearch)

    toc['titles'] = toc.iloc[:,[1,2,3,4,10,11,12,13]].apply(lambda x: ''.join(x), axis=1)

    toc = toc[toc['titles'].str.contains(newsearch)]

    toc = toc.drop('titles', 1)

  if filters:

    test_cols = min(len(toc.columns), len(filters))

    try:

      for index in range(test_cols):

        test_filter = filters[index]

        if test_filter:

          test_item = '|'.join(test_filter)

          toc = toc[toc.iloc[:, index].str.contains(test_item)]

          del test_item

        del test_filter

    except OSError:

      pass


  return toc










def get_ilostat_dic(dic,
                    lang = 'en'):
  """
  Read Ilostat Dictionary
  Downloads one ilostat dictionary from ilostat www.ilo.org/ilostat via bulk download facility 
  http://www.ilo.org/ilostat-files/WEB_bulk_download/html/bulk_main.html.
    details For a given coded variable from ilostat www.ilo.org/ilostat.
      The dictionaries link codes with human-readable labels.
      To translate codes to labels, use label_ilostat.
    param dic: A character, dictionary for the variable to be downloaded,
    param lang: a character, code for language. Available are "en" (default), 
          "fr" and "es".
    return tibble with two columns: code names and full names.
    author David Bescond bescond@ilo.org

  """

  diclang =  str.lower(dic) + '_' + str.lower(lang)

  dic = pandas.read_csv('http://www.ilo.org/ilostat-files/WEB_bulk_download/dic/' + diclang + '.csv', engine  = 'c', dtype = {str.lower(dic): str ,str.lower(dic)+ '.label': str, str.lower(dic) + '.sort': str})

  # print('http://www.ilo.org/ilostat-files/WEB_bulk_download/dic/' + diclang + '.csv')

  if not type(dic) == pandas.core.frame.DataFrame:

    print('the dic file : ' + 'http://www.ilo.org/ilostat-files/WEB_bulk_download/dic/' + diclang + '.csv does not exist')


  return dic



def label_ilostat(x, 
                  dic = [],
                  code = [], 
                  lang = 'en'):
  """
  Get Ilostat Codes
  Gets definitions/labels for ilostat codes from ilostat dictionaries.
    param x A character or a factor vector or a data_frame to labelled. 
    param dic: A string (vector) naming ilostat dictionary or dictionaries.
      If [] (default) dictionary names are taken from column names of 
      the data_frame. A character or a factor vector or a data_frame to labelled, 
    param lang: a character, code for language. Available are "en" (default), 
           "fr" and "es",
    param code: a vector of names of the column for which code columns
      should be retained. Set to \code{"all"}, keep all the code.  
    details A character or a factor vector of codes returns a corresponding vector of definitions. 
       label_ilostat labels also data_frames from get_ilostat. For vectors a dictionary 
    name have to be supplied. For data_frames dictionary names are taken from column names with suffix ".label". 
      "time" and "values" columns are returned as they were, so you can supply data_frame from get_ilostat 
      and get data_frame with definitions instead of codes.
    author David Bescond bescond@ilo.org
    return a vector or a data_frame. The suffix ".label" is added to code column names.
  """

  y = pandas.DataFrame()


  if not dic:
    print('Dictionary information is missing')
    return y

######## manage vector


  if not type(x) == pandas.core.frame.DataFrame:

    dic_df = get_ilostat_dic(dic, lang)

    if dic.lower().find('note_') == -1:

      y = pandas.merge(pandas.DataFrame(x, columns = [dic]), dic_df, how='left', on=[dic]).iloc[:,[0,1]]

    else:

      z = x.drop_duplicates(keep = 'first')
      z = pandas.DataFrame(z).dropna(thresh=1)
      z = z.reindex(columns=[*z.columns.tolist(), dic + '.label' ], fill_value=numpy.nan).reset_index(drop=True)

      for i in range(len(z.index)):
        test = list(z[dic][i:i+1])[0].split('_')
        print(len(test))
        test = pandas.DataFrame(test, columns = [dic])
        test = pandas.merge(test, dic_df, how='left', on=[dic]).iloc[:,[0,1]]

        test = ' | '.join(list(test.iloc[:,1]))
        z.loc[i,dic + '.label'] = test
      y = pandas.merge(pandas.DataFrame(x, columns = [dic]), z, how='left', on=[dic]).iloc[:,[0,1]]

    y = list(y.iloc[:,1])

######## manage data frame

  else:

    if not code:
      if code[0] == 'all':
        code = ['collection', 'ref_area', 'source', 'indicator', 'sex', 'classif1', 'classif2', 'obs_status', 
                    'note_classif', 'note_indicator', 'note_source']

        mynams = list(code[i] for i in list(x) )
    print(ilostat_cols_ref)


####################### to fix note mapping


  return y
























