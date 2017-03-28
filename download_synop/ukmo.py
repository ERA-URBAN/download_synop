'''
description:    Download hourly data from ftp CEDA archive
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

from ftplib import FTP
import os
import datetime
import urllib2
import csv
import os
import pandas
from numpy import array as nparray
from numpy import vstack
from numpy import unique
from datetime import datetime
from datetime import date
from datetime import timedelta
import collections

class download_ukmo_data:
  def __init__(self, username, password, year, outputdir):
    '''
    Download ukmo data from CEDA ftp
    Each file is a txt csv file containing 1 year of data
      - year: integer year to download data for
      - username, password: username/password for ftp server
      - outputdir: directory where outputfile should be saved
    '''
    self.year = year  # datetime object
    self.outputdir = outputdir
    self.define_filename()
    self.define_outputfile()
    if not os.path.isfile(self.outputfile):
      self.connect_to_ftp(username, password)
      self.change_to_download_directory()
      self.download_file()
    self.define_headerfile()
    if not os.path.isfile(self.headerfile):
      self.download_url_file(
        'http://badc.nerc.ac.uk/artefacts/badc_datadocs/ukmo-midas/Headers/WH_Column_Headers.txt',
        self.headerfile)
    self.define_stationfile()
    if not os.path.isfile(self.stationfile):
      self.download_url_file(
        'http://badc.nerc.ac.uk/artefacts/badc_datadocs/ukmo-midas/Docs/src_id_list.txt',
        self.stationfile)

  def connect_to_ftp(self, username, password):
    '''
    Connect to CEDA ftp server
    '''
    # connect to host, default port
    self.ftp = FTP('ftp.ceda.ac.uk')
    self.ftp.login(username, password)

  def change_to_download_directory(self):
    '''
    Change into the correct download directory on the ftp server
    '''
    download_dir = os.path.join('badc', 'ukmo-midas', 'data',
                                'WH', 'yearly_files')
    self.ftp.cwd(download_dir)

  def define_filename(self):
    '''
    Define the filename to download
    '''
    basename = "midas_wxhrly_"
    ext = '.txt'
    self.filename = (basename + str(self.year) + '01-' + str(self.year) +
                     '12' + ext)

  def download_file(self):
    '''
    Download the tar file from ftp server
    '''
    # Open output file for writing
    self.file = open(self.outputfile, 'wb')
    # retrieve file
    self.ftp.retrbinary('RETR %s' % self.filename, self.file.write)
    # close the output file
    self.file.close()

  def define_outputfile(self):
    '''
    Define name and location of the output file
    '''
    self.outputfile = os.path.join(self.outputdir, self.filename)

  def define_headerfile(self):
    '''
    Define name and location of the output file
    '''
    self.headerfile = os.path.join(self.outputdir, 'ukmo_headers.txt')

  def download_url_file(self, url, filename):
    '''
    Download file from url
    '''
    response = urllib2.urlopen(url)
    data = response.read()
    # Write data to file
    file_ = open(filename, 'w')
    file_.write(data)
    file_.close()

  def define_stationfile(self):
    '''
    Define name and location of the output file
    '''
    self.stationfile = os.path.join(self.outputdir, 'ukmo_station_list.txt')


class read_ukmo:
    def __init__(self, headername, liststations, filename):
        self.filename = filename
        self.headername = headername
        self.liststations = liststations
        self.load_stations()
        self.load_header()
        self.load_file()
        self.process_reference_data()

    def load_header(self):
        '''
        load header file
        '''
        # load the zip file
        reader = csv.reader(open(self.headername))
        # file content is ignored while start_data==False
        # loop through all rows of the txt file
        for row in reader:
          self.header = [item.strip() for item in row]

    def load_file(self):
      ucol = [0,1,9,10,21,35,36,38,98]
      csvdata = pandas.read_csv(self.filename, engine='c',
                                na_values=' ',
                                usecols=ucol).as_matrix()
      self.csvdata = dict(zip(nparray(self.header)[ucol], csvdata.T))

    def load_stations(self):
        '''
        load list of stations
        '''
        df = pandas.read_csv(self.liststations, sep='\t', engine='c',
                             header=None, index_col=None,
                             error_bad_lines=False, skiprows=0)
        self.stations_dict = {k:df[v].tolist() for v,k in enumerate(df.columns)}

    def process_reference_data(self):
        '''
        process the reference csv data
        '''
        self.stationdata = collections.defaultdict(dict)
        # unique ids
        unique_ids = unique(self.csvdata['ID'])
        for id in unique_ids:
          self.stationdata[id.strip()]['longitude'] = self.return_station_value(10, id)
          self.stationdata[id.strip()]['latitude'] = self.return_station_value(9, id)
          self.stationdata[id.strip()]['elevation'] = self.return_station_value(14, id)
        ## Convert time to datetime.datetime object
        # create datetime object
        self.csvdata['datetime'] = [datetime.strptime(
            str(item), ('%Y-%m-%d %H:%M')) for item in self.csvdata['OB_TIME']]

    def return_station_value(self, col, b):
        '''
        return dictionary value for a particular id
        '''
        try:
            return self.stations_dict[col][self.stations_dict[3].index(b.strip())]
        except ValueError:
            return -99999
