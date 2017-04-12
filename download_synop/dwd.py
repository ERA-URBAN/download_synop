'''
description:    Download hourly data from ftp dwd archive
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''
from ftplib import FTP
import os
from download_synop.ncdf import dwd_ncdf
from download_synop.utils import *
import errno
import numpy as np
import glob
import fnmatch
import glob
import fnmatch
import zipfile
import csv
import pandas
from numpy import hstack
from numpy import sort
from numpy import zeros
from datetime import datetime

class download_dwd_data:
  def __init__(self, outputdir):
    '''
    desdc
    '''
    ftphost = 'ftp-cdc.dwd.de'
    self.outputdir = outputdir
    ftp = self.ftp_connect(ftphost)
    self.get_dwd_data(ftp)
    self.ftp_disconnect(ftp)

  def ftp_connect(self, ftphost):
    '''
    connect to ftp host
    '''
    ftp = FTP(ftphost)  # connect to host, default port
    ftp.login()  # user anonymous, passwd anonymous@
    return ftp

  def getbinary(self, ftp, filename, ofile):
    '''
    download binary from ftp
    '''
    if not os.path.isfile(ofile):
      outfile = open(ofile, 'w')
      ftp.retrbinary("RETR " + filename, outfile.write)
      outfile.close()

  def ftp_disconnect(self, ftp):
    '''
    disconnect ftp connection
    '''
    ftp.quit()

  def get_dwd_data(self, ftp):
    '''
    download DWD data from ftp server
    '''
    ftpdir = '/pub/CDC/observations_germany/climate/hourly/'
    datatype = 'historical'
    # change working directory
    ftp.cwd(ftpdir)
    # create list of subdirectories (variables are sort per subdir)
    variables = []
    ftp.dir('-d','*/',lambda L:variables.append(L.split()[-1]))
    variables.remove('solar/')  # nothing to do for this directory
    for variable in variables:
      if not os.path.exists(variable):
        try:
          os.makedirs(os.path.join(self.outputdir, 'data', variable))
        except OSError as exception:
          if exception.errno != errno.EEXIST:
            raise
      ftp.cwd(os.path.join(ftpdir, variable, 'historical'))
      files = ftp.nlst('*.zip')
      [ self.getbinary(ftp, filename, os.path.join(self.outputdir, 'data',
                                                   variable, filename)) for
        filename in files ]

class read_dwd:
  def __init__(self, outputdir):
    self.outputdir = os.path.join(outputdir, 'data')
    dirs = self.get_variables()
    ids = np.sort(self.get_list_of_stations(dirs))
    for st in range(0,len(ids)):
      print (ids[st])
      station_files = self.find_station_files(ids[st])
      station_dicts = []
      metadata_dicts = []
      for sfile in station_files:
        # load data in list of dicts
        print (sfile)
        sdict, mdict = self.load_file(sfile)
        if sdict == None:
          continue
        station_dicts = hstack((station_dicts, sdict))
        metadata_dicts = hstack((metadata_dicts, mdict))
      # merge station data dicts
      results = reduce(self.merge, station_dicts)
      # generate output dictionary
      results = self.convert_dict(results)
      # convert metadata_dicts
      metadata = self.convert_meta_dict(metadata_dicts)
      # split station data based on station location movements as specified
      # in the metadata
      r2 = self.split_data(results, metadata)
      for idx in range(0,len(r2)):
        if idx > 0:
          dwd_ncdf(r2[idx], ids[st] + '_' + str(idx+1), self.outputdir)
        else:
          dwd_ncdf(r2[idx], ids[st], self.outputdir)

  def get_variables(self):
    '''
    get variables/directories from ftp listing
    '''
    dirs = os.listdir(self.outputdir)
    return dirs

  def get_list_of_stations(self, dirs):
    '''
    extract station ids from ftp filenames, create unique list for all 
    variables combined
    '''
    ids = []
    for dir0 in dirs:
      # list of files in current directory
      files = glob.glob(os.path.join(self.outputdir, dir0, '*.zip'))
      ids = ids + [ filename[-32:-27] for filename in files if filename[-32:-27]
            not in ids ]
    return ids

  def find_station_files(self, stationid):
    '''
    find all zip files belonging to a given stationid
    '''
    matches = []
    for root, dirnames, filenames in os.walk(self.outputdir):
        for filename in fnmatch.filter(filenames, '*' + '_' + stationid + '_' +
                                      '*.zip'):
            matches.append(os.path.join(root, filename))
    return matches

  def load_file(self, station_zip):
    '''
    load data files inside zip file and return data in a dictionary
    '''
    # load zipfile
    zipf = zipfile.ZipFile(station_zip)
    # list of files in zip
    data_files = [ filename for filename in zipf.namelist() if 'produkt_' 
                  in filename ]
    metadata_files = [ filename for filename in zipf.namelist() if
                      'Stationsmetadaten' in filename ]
    station_dict = self.read_data(zipf.open(data_files[0]))
    meta_dict = pandas.read_csv(zipf.open(metadata_files[0]), engine='c', sep=';',
                                skipinitialspace=True,
                                header=0).to_dict(orient='records')
    return station_dict, meta_dict


  def read_data(self, filename):
    '''
    Read csv data and return dictionary: index->
    '''
    try:
      csvdict = pandas.read_csv(filename, engine='c', sep=';',
                                parse_dates=['MESS_DATUM'], index_col=['MESS_DATUM'],
                                header=0, skipinitialspace=True).to_dict(
                                orient='index')
    except ValueError:
      try:
        csvdict = pandas.read_csv(filename, engine='c', sep=';',
                                  parse_dates=['Mess_Datum'], index_col=['Mess_Datum'],
                                  header=0, skipinitialspace=True).to_dict(
                                  orient='index')
      except ValueError:
        return None
    return csvdict

  def merge_dicts(*dict_args):
      '''
      Given any number of dicts, shallow copy and merge into a new dict,
      precedence goes to key value pairs in latter dicts.
      '''
      result = {}
      for dictionary in dict_args:
          result.update(dictionary)
      return result

  def merge(self, a, b, path=None):
      "merges b into a"
      if path is None: path = []
      for key in b:
          if key in a:
              if isinstance(a[key], dict) and isinstance(b[key], dict):
                  self.merge(a[key], b[key], path + [str(key)])
              elif a[key] == b[key]:
                  pass # same leaf value
              else:
                  #raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
                  pass  # keep original keys if keys are the same
          else:
              a[key] = b[key]
      return a

  def convert_dict(self, dict_of_dicts):
    time_axis = sort(dict_of_dicts.keys())
    pressure_reduced = zeros(len(time_axis))
    pressure_station = zeros(len(time_axis))
    rltvh = zeros(len(time_axis))
    rltvh = zeros(len(time_axis))
    winddir = zeros(len(time_axis))
    windspeed = zeros(len(time_axis))
    clouds = zeros(len(time_axis))
    precipitation = zeros(len(time_axis))
    temperature = zeros(len(time_axis))
    # remove nans from time_axis
    time_axis = time_axis[~np.isnan(time_axis)]
    for idx, c in enumerate(time_axis):
      try:
        pressure_reduced[idx] = dict_of_dicts[c]['LUFTDRUCK_REDUZIERT']
      except KeyError:
        pressure_reduced[idx] = -999
      try:
        pressure_station[idx] = dict_of_dicts[c]['LUFTDRUCK_STATIONSHOEHE']
      except KeyError:
        pressure_station[idx] = -999
      try:
        rltvh[idx] = dict_of_dicts[c]['REL_FEUCHTE']
      except KeyError:
        rltvh[idx] = -999
      try:
        winddir[idx] = dict_of_dicts[c]['WINDRICHTUNG']
      except KeyError:
        winddir[idx] = -999
      try:
        windspeed[idx] = dict_of_dicts[c]['WINDGESCHWINDIGKEIT']
      except KeyError:
        windspeed[idx] = -999
      try:
        clouds[idx] = dict_of_dicts[c]['GESAMT_BEDECKUNGSGRAD']
      except KeyError:
        clouds[idx] = -999
      try:
        precipitation[idx] = dict_of_dicts[c]['NIEDERSCHLAGSHOEHE']
      except KeyError:
        precipitation[idx] = -999
      try:
        temperature[idx] = dict_of_dicts[c]['LUFTTEMPERATUR']
      except KeyError:
        temperature[idx] = -999
    d = {}
    # convert pressure to Pascal
    d['pressure_reduced'] = [round(float(100 * item), 1) if
                             item != -999 else item for item in
                             pressure_reduced]
    d['pressure_station'] = [round(float(100 * item), 1) if
                             item != -999 else item for item in
                             pressure_station]
    d['rltvh'] = rltvh
    d['winddir'] = winddir
    d['windspeed'] = windspeed
    d['clouds'] = clouds
    d['precipitation'] = precipitation
    d['temperature'] = temperature
    d['time'] = [datetime.strptime(str(int(item)), ('%Y%m%d%H')) for
                item in time_axis]
    return d

  def list_of_dict_to_dict_of_lists(self, tmp) :
    #result = {}
    #for d in l :
    #   for k, v in d.items() :
    #      result[k] = result.get(k,[]) + [v] #inefficient
    #return result
    return {key:[item[key] for item in tmp] for key in tmp[0].keys() }

  def dict_of_list_to_list_of_dicts(self, d) :
    if not d :
        return []
    #reserve as much *distinct* dicts as the longest sequence
    result = [{} for i in range(max (map (len, d.values())))]
    #fill each dict, one key at a time
    for k, seq in d.items() :
        for oneDict, oneValue in zip(result, seq) :
          oneDict[k] = oneValue
    return result

  def convert_meta_dict(self, metadata_dicts):
    # find unique metadata information
    metadata = {v['von_datum']:v for v in metadata_dicts}.values()
    # convert list of dicts to dict of lists
    metadata = self.list_of_dict_to_dict_of_lists(metadata)
    # convert timme to datetime objects
    for timestr in ['von_datum', 'bis_datum']:
      metadata[timestr] = [datetime.strptime(str(int(item)),('%Y%m%d')) if 
                          ~np.isnan(item)  else datetime.now() for item in
                          metadata[timestr]]
    return metadata

  def split_data(self, results, metadata):
    '''
    split station data based on moving station location in time
    '''
    data = []
    for idd in range(0,len(metadata['von_datum'])):
      tmp = [ { key : results[key][idx] for key in results.keys() }
              for idx, x in enumerate(results["time"]) if
              metadata['von_datum'][idd]<=x<=metadata['bis_datum'][idd]]
      if not tmp:
        continue  # no measurements found for time period
      tmp_out = self.list_of_dict_to_dict_of_lists(tmp)
      tmp_out['longitude'] = metadata['Geogr.Breite'][idd]
      tmp_out['latitude'] = metadata['Geogr.Laenge'][idd]
      tmp_out['elevation'] = metadata['Stationshoehe'][idd]
      data = hstack((data,tmp_out))
    return data

