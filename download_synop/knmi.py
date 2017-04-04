#!/usr/bin/env python2

'''
Description:    Download KNMI zipped ascii data and create a csv file with
                station information.
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:        -
Last Modified:  -
License:        Apache 2.0
Notes:          -
'''

from lxml.html import parse
import csv
import urllib2
from lxml import html
import numbers
import json
import os
import utils
from numpy import vstack
from numpy import concatenate as npconcatenate
import argparse
from download_synop.ncdf import knmi_ncdf
import pandas
import re
import glob
from numpy import sort
from numpy import concatenate
import collections
import zipfile
from datetime import datetime
from datetime import date
from datetime import timedelta
from numpy import zeros

class get_knmi_reference_data:
    '''
    description
    '''
    def __init__(self, opts):
        #self.csvfile = opts.csvfile
        self.outputdir = opts.outputdir
        self.keep = opts.keep
        self.check_output_dir()
        self.station = opts.stationid
        self.get_station_ids()
        print (self.stationids)
        self.download_station_data()
        #self.get_station_locations()

    def get_station_ids(self):
        '''
        get all stationids from the KNMI website
        '''
        url = 'http://projects.knmi.nl/klimatologie/metadata/index.html'
        page = parse(url)
        url_metadata = page.xpath(".//table/tr/td/a/@href")
        station_name_id = [c.text for c in page.xpath(".//table/tr/td/a")]
        stationids = [s.split()[0] for s in station_name_id]
        bad_chars = '(){}<>'
        self.rgx = re.compile('[%s]' % bad_chars)
        station_names = [re.sub(self.rgx, '', " ".join(s.split()[1:])) for s in station_name_id]
        if len(self.station)==0:
            self.stationids = [stationids[idx] + ' - ' + station_names[idx] for
                              idx in range(0,len(stationids))]
        else:
          idx = stationids.index(self.station)
          self.stationids = [stationids[idx] + ' - ' + station_names[idx]]

    def download_station_data(self):
        ''''
        download zip files containing csv station data
        (complete time series for all KNMI stations)
        '''
        url = 'http://www.knmi.nl/nederland-nu/klimatologie/uurgegevens'
        page = parse(url)
        # find location of stations on web page
        num_stations = len(page.xpath("/html/body/main/div[2]/div"))
        station_elements = [page.xpath("/html/body/main/div[2]/div["+str(idx)+"]/div/div/div[2]/table/thead/tr/th") for idx in range(0,num_stations)]
        station_names = [re.sub(self.rgx, '', x[0].text) if len(x)>0 else 'ndf'
                         for x in station_elements]
        for stationid in self.stationids:
            div_id = str(station_names.index(stationid))
            relpaths = page.xpath("/html/body/main/div[2]/div["+div_id+"]/div/div/div[2]/table/tbody/tr/td/a/@href")
            for path in relpaths:
                try:
                    fullpath = "http:" + path
                    request = urllib2.urlopen(fullpath)
                    filename = os.path.basename(path)
                    outputfile = os.path.join(self.outputdir, filename)
                    if self.keep:
                        if os.path.exists(outputfile):
                            # check if filesize is not null
                            if os.path.getsize(outputfile) > 0:
                                # file exists and is not null, continue next iteration
                                continue
                            else:
                                # file exists but is null, so remove and redownload
                                os.remove(outputfile)
                    elif os.path.exists(outputfile):
                        os.remove(outputfile)
                    #save
                    output = open(outputfile, "w")
                    output.write(request.read())
                    output.close()
                except urllib2.HTTPError:
                      print "Error downloading file " + fullpath
            # get station location
            lat, lon ,elev, name = self.get_station_location(stationid)
            data = self.read_knmi_data(stationid)
            knmi_ncdf(stationid, lat, lon, elev, data, self.outputdir)


    def get_station_location(self, stationid):
        '''
        write station name, id and location to csv file
        '''
        # get station names for stationids
        url = 'http://projects.knmi.nl/klimatologie/metadata/index.html'
        page = parse(url)
        url_metadata = page.xpath(".//table/tr/td/a/@href")
        station_name_id = [c.text for c in page.xpath(".//table/tr/td/a")]
        station_ids = [s.split()[0] for s in station_name_id]
        station_names = [" ".join(s.split()[1:]) for s in station_name_id]
        #for idx, stationid in enumerate(station_id):
        idx = station_ids.index(stationid.split()[0])
        station_name = station_names[idx]
        station_url = os.path.join(os.path.split(url)[0],
                                   url_metadata[idx])
        page = parse(station_url)
        rows = [c.text for c in page.xpath(".//table/tr/td")]
        idx_position = rows.index('Positie:') + 1
        idx_startdate = rows.index('Startdatum:') + 1
        lat, lon = rows[idx_position].encode('UTF-8').replace(
            '\xc2\xb0','').replace(' N.B. ', ',').replace(
                'O.L.','').strip().split(',')
        lat,lon = self.latlon_conversion(lat,lon)
        idx_elevation = rows.index('Terreinhoogte:') + 1
        elevation = float(rows[idx_elevation].encode('UTF-8').split(' ')[0].replace(',','.'))
        return lat, lon, elevation, station_name

    def latlon_conversion(self, lat, lon):
        '''
        conversion of GPS position to lat/lon decimals
            example string for lat and lon input: "52 11'"
        '''
        # latitude conversion
        latd = lat.replace("'","").split()
        lat = float(latd[0]) + float(latd[1])/60
        # longitude conversion
        lond = lon.replace("'","").split()
        lon = float(lond[0]) + float(lond[1])/60
        return lat,lon

    def check_output_dir(self):
        '''
        check if outputdir exists and create if not
        '''
        if not os.path.exists(self.outputdir):
            os.makedirs(self.outputdir)

    def read_knmi_data(self, stationid):
        '''
        Calculate or load KNMI reference data:
            pickled file exists -> load
            pickled file doesn't exist -> calculate
        '''
        # generate filename of KNMI station
        filenames = sort(glob.glob(os.path.join(self.outputdir, 'uurgeg_' + str(stationid.split()[0]) + '*.zip' )))
        # load all csv files in list of dictionaries
        dicts = [load_knmi_data(filename).csvdata for filename in filenames]
        # merge all dictionaries in a super dictionary
        knmi_data = collections.defaultdict(list)
        for idx in range(0,len(dicts)):
          try:
            knmi_data = dict((k, npconcatenate((knmi_data.get(k), dicts[idx].get(k)))) for k in set(knmi_data.keys() + dicts[idx].keys()))
          except ValueError:
            # cannot concatenate empty arrays
            knmi_data = dict((k, dicts[idx].get(k)) for k in dicts[idx].keys())
        # return dictionary with all variables/time steps
        return knmi_data


class load_knmi_data:
    def __init__(self, filename):
        self.filename = filename
        self.load_file()
        self.process_reference_data()

    def load_file(self):
        '''
        function description
        '''
        # load the zip file
        zipf = zipfile.ZipFile(self.filename)
        # name of csv name in zip file
        txtname = os.path.splitext(os.path.basename(self.filename))[0] + '.txt'
        # read the data in the txt file
        header_found = False
        header_row = 0
        with zipfile.ZipFile.open(zipf, txtname) as f:
          for line in f:
            if '# STN' in line:
              break
            if len(line.strip())>0:
              header_row += 1
        self.csvdata = pandas.read_csv(self.filename, engine='c',
                                       header=header_row,
                                       skipinitialspace=True).to_dict('list')
        # TODO: use parse_dates=['YYYYMMDD']

    def process_reference_data(self):
        '''
        process the reference csv data
        '''
        ## Convert time to datetime.datetime object
        # hours should be 0-23 instead of 1-24
        self.csvdata['HH'] = [item if item!=24 else 0 for item in
                              self.csvdata['HH']]
        # combine date and hour into one string
        dstring = [str(self.csvdata['YYYYMMDD'][idx]) +
                   str(self.csvdata['HH'][idx]).zfill(2) for idx in
                   range(0,len(self.csvdata['YYYYMMDD']))]
        # create datetime object
        self.csvdata['datetime'] = [datetime.strptime(
            str(item), ('%Y%m%d%H')) for item in dstring]
        # Correct conversion of the datestring
        # the date of the night -> HH=24 is HH=0 on the next day!
        self.csvdata['datetime'] = [c+ timedelta(days=1) if c.hour==0 else
                                    c for c in self.csvdata['datetime']]
        # rain is (-1 for <0.05 mm), set to 0
        self.csvdata['RH'] = [0 if item == -1 else
                                  item for item in self.csvdata['RH']]
        # process all variables that need to be divided by 10
        for variable in ['T10', 'T', 'RH', 'FF', 'TD']:
            # T10: temperature at 10 cm height, divide by 10 to convert to degC
            # T: temperature at 1.50 m height, divide by 10 to convert to degC
            # RH: rain
            # FF: wind speed
            self.csvdata[variable] = [round(0.1 * item, 1) if item != -999 else
                                item for item in self.csvdata[variable]]
        # SWD
        self.csvdata['Q'] = [round(float(10000 * item)/3600, 5) if
                             item != -999 else item for item in
                             self.csvdata['Q']]
