#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created from the SnowEx 2021 Time Series script on Tue Aug 30 2022
Original created Sept. 2017 [hpm]
Modified in Dec. 2017 [lb]
Major revision April 2020 [hpm]
Modified in June 2020 (cmv) to add LWC calculation, and output WISe # to siteDetails.csv
Modified in Oct 2020 (cv) to calculate total density & SWE for summary files & organize pit files in separate folders
Modified in April 2021 [mmason] to process 2020 Time Series snow pit data (12 time series locations
Modified in August 2022 [mmason] to process 2021 Time Series snow pit data (7 time series locations))
Modified in January 2024 [mmason] to account for 3rd density measurement (avg. w/ 2nd if present)
Modified in January 2024 [mmason] to pull HS for HS in Summary_SWE file (previously pulled top of density)
Modified in January 2025 [mmason] to produce a general snow pit processing workflow for snow school and other field applications
"""

__author__ = "Megan Mason, NASA Goddard / SSAI"
__version__ = "01-1"
__maintainer__ = "HP Marshall" # github https://github.com/SnowEx/snowpits
__email__ = "megan.a.mason@nasa.gov"
__status__ = "Dvp"
__date__ = "08.2022"

import datetime
import glob
import os
import shutil
import numpy as np
import pandas as pd
import csv
from csv import writer
import textwrap
from pathlib import Path
import utm

# ------------------------------------------------------------------------------
# Functions

# get_metadata: pulls all metadata from top of snow pit sheet and stores in dictionary
def get_metadata(xl_file):

    d = pd.read_excel(xl_file)

    # metadata
    location = d['Unnamed: 1'][1]
    site = d['Unnamed: 1'][4]
    pitID = d['Unnamed: 1'][6]
    date = d['Unnamed: 7'][1]
    time = d['Unnamed: 7'][4]
    zone = d['Unnamed: 18'][6]
    easting = int(d['Unnamed: 9'][6])
    northing = int(d['Unnamed: 13'][6])

    print('UTME', easting)
    print('UTMN', northing)

    pit_datetime=datetime.datetime.combine(date, time)
    pit_datetime_str=pit_datetime.strftime('%Y-%m-%dT%H:%M')

    # print('DATETIME:', pit_datetime)

    # convert to Lat/Lon:
    lat, lon = utm.to_latlon(easting, northing, zone, 'Northern')
    lat = round(lat, 5)
    lon = round(lon, 5)

    # other
    hs = d['Unnamed: 5'][6]
    observers = d['Unnamed: 9'][1]
    gps = d['Unnamed: 20'][6]
    WiseSerialNo = d['Unnamed: 7'][6]
    T_start_time = d['Unnamed: 20'][4]
    T_end_time = d['Unnamed: 21'][4]

    s=d['Unnamed: 22'][1]
    if type(s)==str:
        wrapper = textwrap.TextWrapper(width=70) # set wrap length
        PitComments = wrapper.fill(s) # wrapped comments for printing
    else:
        PitComments = 'no additional pit comments'

    # flags --> pulls data flag codes from upper right 'comments' box
    if "Flag: " in PitComments:
        Flag = PitComments.split('Flag: ')[1]
        Flag = Flag.replace('\n', ' ')
    else:
        Flag = None

        metadata = {
        'Location': location,
        'Site': site,
        'PitID': pitID, 
        'Date': date,
        'Time': time,
        'Datetime_str': pit_datetime_str,
        'Zone': str(zone)+'N', #'N'=nortern hemisphere
        'Easting': easting,
        'Northing': northing,
        'Latitude': lat,
        'Longitude': lon,
        'HS (cm)': hs,
        'Observers': str(observers),
        'WiseSerialNo': WiseSerialNo,
        'GPS & Uncert.': gps,
        'T start Time': T_start_time,
        'T end Time': T_end_time,
        'Flag': Flag,
        'Pit Comments': PitComments
        }

    return metadata

# ------------------------------------------------------------------------------
# run main
if __name__ == "__main__":

    # paths
    src_path = Path('.')
    des_basepath = Path('./outputs')
    des_basepath.mkdir(parents=True, exist_ok=True)
    

    for filename in sorted(src_path.rglob('*example.xlsx')):
        print(filename.name)

        metadata = get_metadata(filename)