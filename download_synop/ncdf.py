#!/usr/bin/env python2

from download_synop.ukmo import read_ukmo
from download_synop.utils import *
from numpy import unique as npunique
from numpy import where as npwhere
from numpy import array as nparray
from numpy import zeros
from numpy import nan as npnan
from numpy import dtype
import collections
import time
import os
import errno
from netCDF4 import Dataset as ncdf
from netCDF4 import date2num
from datetime import datetime

class ukmo_ncdf:
  def __init__(self, headername, liststations, filename, outputdir):
    self.filename = filename
    self.outputdir = outputdir
    aa = read_ukmo(headername, liststations, filename)
    # unique IDs
    ids = npunique(aa.csvdata['ID'])
    for identifier in ids:
      try:
        lon = aa.stationdata[identifier.strip()]['longitude']
        lat = aa.stationdata[identifier.strip()]['latitude']
        elevation = aa.stationdata[identifier.strip()]['elevation']
      except KeyError:
        continue
      if (lon==-99999) or (lat==-99999) or (elevation==-99999):
        continue
      # list of indices
      idx = npwhere(aa.csvdata['ID']==identifier)[0]
      # extract all keys for selected station identifier
      dataout = collections.defaultdict(list)
      dataout = dict((k, nparray(aa.csvdata[k])[idx]) for k in aa.csvdata.keys())
      stationid = dataout['ID'][0]
      # remove variables from dictionary
      dataout.pop('longitude', None)
      dataout.pop('latitude', None)
      dataout.pop('elevation', None)
      dataout.pop('ID', None)
      # create netcdf file
      filename = self.define_output_file(stationid)
      self.write_netcdf(filename, dataout, lon,lat, elevation)

  def define_output_file(self, stationid):
    '''
    define output netcdf filename
    create output directory if needed
    '''
    outputdir = os.path.join(self.outputdir, 'netcdf',
                             os.path.basename(self.filename))
    try:
      os.makedirs(outputdir)
    except OSError as exception:
      if exception.errno != errno.EEXIST:
        raise
    filename = os.path.join(outputdir,
                            'ukmo_' + str(stationid).strip() + '.nc')
    return filename

  def write_netcdf(self, filename, data, lon, lat, elevation):
    '''
    write data to netcdf file
    '''
    # write time, longitude, latitude, elevation
    ncfile = ncdf(filename, 'w', format='NETCDF4')
    # description of the file
    ncfile.description = 'UK METOFF '
    ncfile.history = 'Created ' + time.ctime(time.time())
    # create time dimension
    timevar = ncfile.createDimension('time', None)
    # create lon/lat dimensions
    lonvar = ncfile.createDimension('longitude', 1)
    latvar = ncfile.createDimension('latitude', 1)
    elevar = ncfile.createDimension('elevation', 1)
    # inititalize time axis
    timevar = ncfile.createVariable('time', 'i4', ('time',),
                                    zlib=True)
    timevar.units = 'minutes since 2010-01-01 00:00:00'
    timevar.calendar = 'gregorian'
    timevar.standard_name = 'time'
    timevar.long_name = 'time in UTC'
    # lon/lat variables
    lonvar = ncfile.createVariable('longitude', 'float32',('longitude',))
    lonvar.units = 'degrees_east'
    lonvar.axis = 'X'
    lonvar.standard_name = 'longitude'
    latvar = ncfile.createVariable('latitude', 'float32',('latitude',))
    latvar.units = 'degrees_north'
    latvar.axis = 'Y'
    latvar.standard_name = 'latitude'
    # elevation 
    elevar = ncfile.createVariable('elevation', 'i4', ('elevation',))
    elevar.units = 'meter'
    elevar.standard_name = 'elevation'
    timeaxis = [int(round(date2num(data['datetime'][idx],
        units='minutes since 2010-01-01 00:00:00',
        calendar='gregorian'))) for idx in range(0,len(data['datetime']))]
    timevar[:] = timeaxis
    lonvar[:] = lon
    latvar[:] = lat
    elevar[:] = elevation
    # create other variables in netcdf file
    for variable in data.keys():
      if variable not in ['YYYMMDD', 'Time', '<br>', 'datetime', '# STN', None]:
        # add variables in netcdf file
        # convert strings to npnan if array contains numbers
        if True in [is_number(c)
          for c in data[variable]]:
            data[variable] = [npnan if isinstance(
              fitem(c), str) else fitem(c) for c in data[
                variable]]
        # check if variable is a string
        if not isinstance(data[variable][0], str):
            # fill variable
            variableName = variable
            values = ncfile.createVariable(
              variableName, type(data[variable][0]),
              ('time',), zlib=True, fill_value=-999)
        else:
          # string variables cannot have fill_value
          values = ncfile.createVariable(
            variable, type(data[variable][0]),
            ('time',), zlib=True)
        try:  # fill variable
          values[:] = data[variable][:]
        except IndexError:
          # for strings the syntax is slightly different
          values = data[variable][:]
          #self.fill_attribute_data()
    ncfile.close()

  def fill_attribute_data():
    '''
    Function that fills the attribute data of the netcdf file
    '''
    if variable == 'DD':
      values.units = 'degrees'
      values.standard_name = 'wind direction'
      values.long_name = 'mean wind direction during the 10-minute period preceding the time of observation (990=variable)'
    elif variable == 'TemperatureF':
      values.units = 'F'
      values.standard_name = 'air_temperature'
      values.long_name = 'air temperature'
    else:
      pass
