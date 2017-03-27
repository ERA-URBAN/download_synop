'''
description:    Download hourly data from ftp CEDA archive
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

from ftplib import FTP
import os
import datetime

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

