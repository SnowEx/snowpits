#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 12 22:42:43 2017
Original created Sept. 2017 [hpm]
Modified in Dec. 2017 [lb]
Major revision April 2020 [hpm]
Modified in June 2020 (cmv) to add LWC calculation, and output WISe # to
siteDetails.csv
Modified in Oct 2020 (cv) to calculate total density & SWE for summary
files & organize pit files in separate folders
Modified in April 2021 [mmason] to process 2020 Time Series snow pit data (12 time series site locations)
"""
__author__ = "HP Marshall, CryoGARS, Boise State University"
__version__ = "06"
__maintainer__ = "HP Marshall"
__email__ = "hpmarshall@boisestate.edu"
__status__ = "Dvp"
__date__ = "04.2020"

# imports
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

# custom imports
from metadata_headers import metadata_headers_swe, metadata_headers_enviro

def writeHeaderRows(fname_summaryFile, metadata_headers):
        with open(fname_summaryFile, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in metadata_headers:
                writer.writerow(row)

def readSnowpit(path_in, filename, version, path_out, fname_swe, fname_enviro, fname_length):

    # filename parsing
    pitID = filename.stem.split('_')[0]
    dateString = filename.stem.split('_')[1]
    timeString = filename.stem.split('_')[2]
    uniqueID = '_'.join([dateString, timeString, pitID]) #join date, time, and pitID
    identifierID = '_'.join([pitID, dateString, timeString]) #join date, time, and pitID

    # paths
    pitPath = path_out.joinpath('pits/' + filename.parts[-3] + '/' + filename.stem[:-5] + '/') #snow pits ([:-5] without '_edit')
    boardPath = path_out.joinpath('boards/' + filename.stem[:-5] + '/') #interval boards
    if not Path.exists(pitPath): #try list comp mkdir() for this if doesn't exist?
        pitPath.mkdir(parents=True, exist_ok=True)
    if not Path.exists(boardPath):
        boardPath.mkdir()

    if Path(filename).suffix == '.jpg': #throw an error if there are any other file extentions
        pass

    elif Path(filename).suffix == '.xlsx':

        newfilename = Path('SNEX20_TS_SP_' + uniqueID + '_pitSheet_' + version + '.xlsx')
        shutil.copyfile(filename, pitPath.joinpath(newfilename))
        print('wrote: .../' + newfilename.name)


    # open excel file
    xl = pitPath.joinpath(newfilename)

    # create individual output file names
    fname_density         = pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_density_' + version +'.csv')
    fname_LWC             = pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_LWC_' + version +'.csv')
    fname_temperature     = pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_temperature_' + version +'.csv')
    fname_stratigraphy    = pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_stratigraphy_' + version +'.csv')
    fname_siteDetails     = pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_siteDetails_' + version +'.csv')
    fname_perimeterDepths = pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_perimeterDepths_' + version +'.csv')
    # fname_newSnow         = boardPath.joinpath('SNEX20_TS_IB_' + uniqueID + '_newSnow_' + version +'.csv')
    fname_gapFilledDensity= pitPath.joinpath('SNEX20_TS_SP_'+ uniqueID + '_gapFilledDensity_' + version +'.csv')

    # location / pit name
    d = pd.read_excel(xl, sheet_name=0, usecols='B')
    Location = d['Location:'][0]
    Site = d['Location:'][2]  # grab site name
    PitID = d['Location:'][4]  # unique identifier
    d = pd.read_excel(xl, sheet_name=0, usecols='L')
    UTME = int(d['Observers:'][2]) #int
    d = pd.read_excel(xl, sheet_name=0, usecols='Q')
    UTMN = int(d['Unnamed: 16'][2]) #int
    d = pd.read_excel(xl, sheet_name=0, usecols='X')
    UTMzone = int(d['Unnamed: 23'][2]) #for northern hemisphere
    LAT = utm.to_latlon(UTME, UTMN, UTMzone, "Northern")[0] #tuple output, save first
    LON = utm.to_latlon(UTME, UTMN, UTMzone, "Northern")[1] #tuple output, save second


    # height of snow (HS)
    d = pd.read_excel(xl, sheet_name=0, usecols='G')
    HeightOfSnow = d['Unnamed: 6'][4]  # grab total depth (i.e. Height of Snow, HS)

    # Wise serial number
    d = pd.read_excel(xl, sheet_name=0, usecols='I')
    WiseSerialNo = d['Unnamed: 8'][4]  # grab Wise serial number
    # if pd.isna(WiseSerialNo):

    # date and time
    d = pd.read_excel(xl, sheet_name=0, usecols='X')
    pit_time = d['Unnamed: 23'][4]
    d = pd.read_excel(xl, sheet_name=0, usecols='S')
    pit_date = d['Unnamed: 18'][4]

    # combine date and time into one datetime variable, and format
    if type(pit_time) is datetime.datetime:
        pit_time = pit_time.time()
    pit_datetime=datetime.datetime.combine(pit_date, pit_time)
    pit_datetime_str=pit_datetime.strftime('%Y-%m-%dT%H:%M')

    # remaining header info
    d = pd.read_excel(xl, sheet_name=0, usecols='L') #, skip_blank_lines=False --> not working MM
    Observers=d['Observers:'][0]
    Tair=d['Observers:'][4]
    d = pd.read_excel(xl, sheet_name=0, usecols='N')
    Slope=str(d['Unnamed: 13'][4]).rstrip(u'\N{DEGREE SIGN}') #remove degree symbol from text
    d = pd.read_excel(xl, sheet_name=0, usecols='Q')
    Aspect=d['Unnamed: 16'][4]
    d = pd.read_excel(xl, sheet_name=0, usecols='Y')
    s=d['Comments/Notes:'][0]
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

    # create minimal header info for other files
    index = ['# Location', '# Site', '# PitID', '# Date/Local Standard Time',
         '# UTM Zone', '# Easting', '# Northing', '# Latitude', '# Longitude', '# Flags', '# Pit Comments', '# Parameter Codes']
    column = ['value']
    df = pd.DataFrame(index=index, columns=column)
    df['value'][0] = Location
    df['value'][1] = str(Site)
    df['value'][2] = str(PitID)
    df['value'][3] = pit_datetime_str
    df['value'][4] = str(UTMzone)+hsphere # Only for Grand Mesa 2020!! - '12N'
    df['value'][5] = UTME
    df['value'][6] = UTMN
    df['value'][7] = round(LAT,5)
    df['value'][8] = round(LON,5)
    df['value'][9] = Flag
    df['value'][10] = PitComments
    df['value'][11] = 'n/a for this parameter'

    # add minimal header to each data file
    df.to_csv(fname_density, sep=',', header=False)
    df.to_csv(fname_LWC, sep=',', header=False)
    df.to_csv(fname_temperature, sep=',', header=False)
    df.to_csv(fname_perimeterDepths, sep=',', header=False)
    # df.to_csv(fname_newSnow, sep=',', header=False)
    df.to_csv(fname_gapFilledDensity, sep=',', header=False)

    df['value'][11] = "Grain Size: <1mm, 1-2mm, 2-4mm, 4-6mm, >6mm; Grain Type: SH=Surface Hoar, PP=New Snow, DF=Decomposing Forms, RG=Rounded Grains, FC=Faceted Crystals, MF=Melt Forms, IF=Ice Lens, MFcr=Melt Crust, FCsf=Near-surface Facets, PPgp=Graupel; Hand Hardness: F=Fist, 4F=4-finger, 1F=1-finger, P=Pencil, K=Knife, I=Ice; Manual Wetness: D=Dry, M=Moist, W=Wet, V=Very Wet, S=Soaked"
    df.to_csv(fname_stratigraphy, sep=',', header=False)

    #~~~~~~MMn's big update:
    d = pd.read_excel(xl, sheet_name=0, usecols='B:M')
    rIx = (d.iloc[:,0] == 'Weather:').idxmax() #locate 'Weather:' cell in spreadsheet (row Index)
    d = d.loc[rIx:,:].reset_index(drop=True) # subset dataframe from 'Weather:' cell down to bottom, and reset index (not always fixed due to extra rows in above measurements)
    Weather = d['Location:'][1] # this works too: d.iloc[1,0]
    Precip=d['Unnamed: 5'][4]
    Sky=d['Unnamed: 5'][5]
    Wind=d['Unnamed: 5'][6]
    GroundCondition=d['Unnamed: 6'][7]
    GroundRoughness=d['Unnamed: 6'][8]
    VegInfo=d.iloc[9:12, 5:12:2] #rows 9-12, cols 5:12 skipping every other (9= veg type options, 10=Veg boolean, 11=Veg Height, 12 is a python thing)
    if VegInfo.at[10,'Unnamed: 12'] == 0:
        VegInfo.at[10,'Unnamed: 12'] = False
    VegBool=VegInfo.iloc[1] #2nd two with True/Falses
    if VegBool.iloc[0]: # if Bare is true, assign veg height of 0 cm.
        VegInfo.at[11, 'Unnamed: 6'] = 0
    VegType=VegInfo.iloc[0].where(VegBool).dropna().tolist()
    GroundVeg=" | ".join(VegType)

    VegHts = []
    for val, bool_val in zip(VegInfo.iloc[2], VegInfo.iloc[1]):
        if pd.notna(val): # if the value exist, append it to the list of Veg Heights
            VegHts.append(val)
        elif bool(bool_val): # if no value exists, but bool=True, append with -9999
            VegHts.append(-9999)
    VegHts=" | ".join(map(str, VegHts))
    TreeCanopy = d['Unnamed: 6'][12]

    # create complete header
    index = ['# Location', '# Site', '# PitID', '# Date/Local Standard Time', '# UTM Zone', '# Easting (m)',
         '# Northing (m)', '# Latitude (deg)', '# Longitude (deg)', '# Slope (deg)', '# Aspect (deg)', '# Air Temp (deg C)',
          '# HS (cm)',  '# Observers', '# WISe Serial No', '# Weather', '# Precip Type', '# Precip Rate', '# Sky', '# Wind',
          '# Ground Condition', '# Ground Roughness', '# Ground Vegetation', '# Vegetation Height (cm)',
          '# Tree Canopy', '# Comments', '# Flags']
    column = ['value']
    df = pd.DataFrame(index=index, columns=column)
    df['value'][0] = Location
    df['value'][1] = str(Site)
    df['value'][2] = str(PitID)
    df['value'][3] = pit_datetime_str
    df['value'][4] = str(UTMzone)+hsphere # Only for Grand Mesa 2020!! '12N', otherwise: str(UTMzone)
    df['value'][5] = str(UTME)
    df['value'][6] = str(UTMN)
    df['value'][7] = round(LAT,5)
    df['value'][8] = round(LON,5)
    df['value'][9]  = Slope
    df['value'][10] = Aspect
    df['value'][11] = Tair
    df['value'][12] = HeightOfSnow
    df['value'][13] = str(Observers).replace('\n', ' ')
    df['value'][14] = WiseSerialNo
    df['value'][15] = str(Weather).replace('\n', ' ')
    df['value'][16] = Precip
    df['value'][17] = 'see Weather comments in _siteDetails file' # Precip rate - didn't make it on the digital sheet, rate added to wx comments.
    df['value'][18] = Sky
    df['value'][19] = Wind
    df['value'][20] = GroundCondition
    df['value'][21] = GroundRoughness
    df['value'][22] = GroundVeg
    df['value'][23] = VegHts
    df['value'][24] = TreeCanopy
    df['value'][25] = str(PitComments.split('Flag:')[0].replace('\n', ' ')) # removes the list of Flags if any.
    df['value'][26] = str(Flag)
    # print(f"siteDetails:\n {df}")
    df.replace('nan', np.nan, inplace=True) # this line turns all the empty cells (nan type=float) to real NaNs, and should then become -9999 in file
    df.fillna(-9999, inplace=True)
    # print(f"siteDetails UPDATE:\n {df}")
    df.to_csv(fname_siteDetails, sep=',', header=False, na_rep=-9999)
    print('wrote: .../' + fname_siteDetails.name)


    newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, LAT, LON,
              df['value'][16], # Precip Type
              df['value'][17], # Precip Rate, #2020 digital file doesn't have a space for this. If observed, added to weather comments portion
              df['value'][18], # Sky
              df['value'][19], # Wind
              df['value'][20], # Ground Condition
              df['value'][21], # Ground Roughness
              df['value'][22], # Ground Vegetation (list, i.g. Bare | Grass | Tree)
              df['value'][23], # Vegetation Height (list)
              df['value'][24]] # Tree Canopy # ugly and hard to read, but this way the -9999's come through from the cleaned dataframe

    with open(fname_enviro, 'a', newline='') as fd:
        csv_writer = writer(fd, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(newrow)

    # get density
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='B:G')#.replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['top\n(cm)'].isnull().values == True))[0]
    d = d.iloc[0:first_nan]
    d.columns = ['# Top (cm)', '-', 'Bottom (cm)', 'Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)']
    den_cols = ['# Top (cm)', 'Bottom (cm)', 'Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)'] #gets rid of the '-' column
    density = d[den_cols].astype(float)
    # print('RAW density:\n', density)
    lenDen = len(density.index)
    density.to_csv(fname_density, sep=',', index=False, mode='a', na_rep=-9999) #write density csv (with -9999's for no data)
    # average 3rd sample (if taken) with profile B (this overwrites B in the dataframe)
    density['Density B (kg/m3)'] = density[['Density B (kg/m3)', 'Density C (kg/m3)']].mean(axis=1) # mean of profile B with any "extra" density samples (i.e. C)
    AvgDensity=density[['Density A (kg/m3)', 'Density B (kg/m3)']].mean(axis=1)# B is averaged in the line above with C, now mean(A,B) (tech. mean(A,(B,C)))
    # density.dropna(subset = ['Density A (kg/m3)', 'Density B (kg/m3)'], how='all', inplace=True) #remove NaN's for calculations below
    # density.reset_index(drop=True, inplace=True) #probs a better solution, but helps the code run below.


    if not density.empty and density['Density A (kg/m3)'].notna().any(): # if the Density A profile isn't ALL NAN's ("true" if there are 'any' non-nans)
        first_non_nan_index = density['Density A (kg/m3)'].first_valid_index()
        first_density_height = density.loc[first_non_nan_index, '# Top (cm)']
        # print(f"TOP OF DENSITY V. HS: {first_density_height} --> {HeightOfSnow}")
    else:
        first_density_height = None
    print('wrote: .../' + fname_density.name) #.split('/')[-1]

    # get LWC
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='B:J').replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['top\n(cm)'].isnull().values == True))[0] # get the index of the first nan value
    d = d.iloc[0:first_nan]
    d.columns = ['# Top\n(cm)', '-', 'Bottom\n(cm)', 'kg/m3', 'kg/m3.1', 'kg/m3.2',
       'Permittivity A', 'Unnamed: 8', 'Permittivity B']
    d = d.rename(columns={'# Top\n(cm)': '# Top (cm)', 'Bottom\n(cm)': 'Bottom (cm)'}) # rename without \n for snowex database
    lwc_cols=['# Top (cm)','Bottom (cm)','Permittivity A','Permittivity B']
    LWC = d[lwc_cols].astype(float)
    LWC.insert(2, 'Avg Density (kg/m3)', AvgDensity, False)

    # Calculate LWC - note this uses 'AvgDensity' - avg of raw density values so that measurement intervals line up, below when SWE is calculated, density get's further cleaned up (interpolated, etc.)
    LWCA_calc = [0.0] * LWC.shape[0]
    LWCB_calc = [0.0] * LWC.shape[0]
    for i in range(0, LWC.shape[0]):
        if(pd.isna(LWC['Permittivity A'][i])):
            LWCA_calc[i] = np.nan
        if AvgDensity.isna().all():
            LWCA_calc[i] = np.nan
        else:
        # Conversion to LWC from WISe User's Manual
            wv = 0
            try: #if Density values not available for row, set LWC = NaN
                for j in range(0, 5):
                    ds = AvgDensity[i] / 1000 - wv  # Convert density to g/cm3
                    wv = (LWC['Permittivity A'][i] - 1 - (1.202 * ds) - (0.983 * ds**2)) / 21.3

                if(wv < 0):   # if computed LWC is less than zero, set it equal to zero
                    LWCA_calc[i] = 0.0
                else:
                    LWCA_calc[i] = wv * 100 #convert to percentage
            except:
                LWCA_calc[i] = np.nan

        if(pd.isna(LWC['Permittivity B'][i])):
            LWCB_calc[i] = np.nan
        if AvgDensity.isna().all():
            LWCB_calc[i] = np.nan
        else:
        # Calculate percentage LWC by volume from WISe User's Manual
            wv = 0
            try: #if Density values not available for row, set LWC = NaN
                for j in range(0, 5):
                    ds = AvgDensity[i] / 1000 - wv  # Convert density to g/cm3
                    wv = (LWC['Permittivity B'][i] - 1 - (1.202 * ds) - (0.983 * ds**2)) / 21.3

                if(wv < 0):   # if computed LWC is less than zero, set it equal to zero
                    LWCB_calc[i] = 0.0
                else:
                    LWCB_calc[i] = wv * 100 #convert to percentage
            except:
                LWCB_calc[i] = np.nan

    # Add calculated LWC values to dataframe and set number of significant digits
    LWC.insert(5, "LWC-vol A (%)", LWCA_calc , False)
    LWC.insert(6, "LWC-vol B (%)", LWCB_calc, False)
    LWC[['LWC-vol A (%)', 'LWC-vol B (%)']] = LWC[['LWC-vol A (%)', 'LWC-vol B (%)']].astype(float).round(2) # if values are floats, round them
    LWC.to_csv(fname_LWC, sep=',', index=False, mode='a', na_rep=-9999, encoding='utf-8')
    print('wrote: .../' + fname_LWC.name)

    # get temperature
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='L:M')#.replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['(cm)'].isnull().values == True))[0]
    temperature = d.iloc[0:first_nan].astype(float)
    temperature.columns = ['# Depth (cm)', 'Temperature (deg C)']
    temperature.to_csv(fname_temperature, sep=',', index=False, mode='a', na_rep=-9999)
    lenTemp = len(temperature.index)
    print('wrote: .../' + fname_temperature.name)

    # get stratigraphy
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='O:Z')#, engine='openpyxl')#.replace(r'^\s*$', np.nan, regex=True)
    first_double_nan = min(np.where(pd.Series(d['top\n(cm).1'].isnull().values).rolling(2).sum().values == 2))[0] # because .xlsx cells are merged
    d = d.iloc[0:first_double_nan]
    d.columns = ['# Top (cm)', '-.1', 'Bottom (cm)', 'Grain Size (mm)', '1-2 mm', '2-4 mm',
       '4-6 mm', '> 6 mm', 'Grain Type', 'Hand Hardness', 'Manual Wetness', 'Comments'] #rename them here, all of them
    strat_cols = ['# Top (cm)', 'Bottom (cm)', 'Grain Size (mm)', 'Grain Type',
                    'Hand Hardness','Manual Wetness', 'Comments'] #select which onces for the CSV file
    stratigraphy = d[strat_cols].dropna(how='all')

    # Capital letter for grain type, hand hardness, and Wetness letter code (skip when NaN/empty)
    if stratigraphy['Grain Type'].count() > 0:
        stratigraphy['Grain Type'] = stratigraphy['Grain Type'].apply(
            lambda x: np.nan if isinstance(x, float) else x.replace('\n', '')) # removed x.upper - it made MFcr --> MFCR
            # df['Grain Type'] = df['Grain Type'].str.replace('\n', '')

    if stratigraphy['Hand Hardness'].count() > 0:
        stratigraphy['Hand Hardness'] = stratigraphy['Hand Hardness'].apply(
            lambda x: np.nan if isinstance(x, float) else x.upper())

    if stratigraphy['Manual Wetness'].count() > 0:
        stratigraphy['Manual Wetness'] = stratigraphy['Manual Wetness'].apply(
            lambda x: np.nan if isinstance(x, float) else x.upper())

    if stratigraphy['Grain Size (mm)'].count() > 0:
        stratigraphy['Grain Size (mm)'] = stratigraphy['Grain Size (mm)'].apply(
            lambda x: np.nan if isinstance(x, float) else x.replace('\n', ''))



    stratigraphy.to_csv(fname_stratigraphy, sep=',', index=False,
                        mode='a', na_rep=-9999, encoding='utf-8')
    lenStrat = len(stratigraphy.index)
    print('wrote: .../' + fname_stratigraphy.name)


    # # get new snow (interval board data)
    # d = pd.read_excel(xl, sheet_name=0, usecols='B:E')
    # rIx = (d.iloc[:,0] == 'Interval board measurements\nUse SWE tube').idxmax() #
    # d = d.iloc[rIx+4:, 2:].reset_index(drop=True) #four down from the interval board section
    # d.columns = ['HN (cm)', 'SWE (mm)']
    # d2=d.iloc[0:3].values
    # d2=np.array(d2.flatten(order='F'))#, dtype=float) #flatten array to match csv style
    # d3=d['HN (cm)'].iloc[3] #evidence of melt (y/n)
    # d4= np.append(d2, d3) #combine HN and SWE array with Evidence of Melt
    # columns = ['# HN (cm) A', 'HN (cm) B', 'HN (cm) C',
    #             'SWE (mm) A', 'SWE (mm) B', 'SWE (mm) C',
    #             'Evidence of Melt']
    # # DO THIS ~~~~~ --> comments - grab weather box, split at IB and save the last).
    # newSnow=pd.DataFrame(d4.reshape(-1, len(d4)), columns=columns)
    # newSnow.to_csv(fname_newSnow, sep=',', index=False, mode='a', na_rep=-9999)
    # print('wrote: .../' + fname_newSnow.name)

    # get perimeter Depths (depths)
    d = pd.read_excel(xl, sheet_name=0, usecols='B:D')
    rIx = (d.iloc[:,0] == 'Plot Perimeter\nSnow Depth Measurements').idxmax() #
    d = d.loc[rIx+2:,:].reset_index(drop=True) # two down from the perimeter section
    d.columns = ['# Count', 'HS (cm)', 'Null']
    perimeterDpts = d[['# Count', 'HS (cm)']]
    perimeterDpts.to_csv(fname_perimeterDepths, sep=',', index=False, mode='a', na_rep=-9999)
    print('wrote: .../' + fname_perimeterDepths.name)

    # temparay for DF length count - one way to check the "length" of the density, temp, and strat data to look for 1's that shouldn't be len=1. (i.e. Many xlxs used formulas to  enter measurement intervals and the script does NOT pick these up, hence parameter files only have a single row and SWE is way under estimated)
    newrow = [Location, Site, PitID, pit_datetime_str, lenDen, lenTemp, lenStrat]

    with open(fname_summaryLength,'a',newline='') as fd:
        csv_writer = writer(fd, delimiter=',')
        csv_writer.writerow(newrow)


    # SWE calculation for summary file
    if density.empty: # (n=1)
        print(f"{filename.name} ~~~~~~~~~~~~~~~~~ file SKIP")

        avgDensityA = -9999 # if density is empty, write it as -9999 in the SWE summary file
        avgDensityB = -9999
        avgDens     = -9999
        sumSWEA     = -9999
        sumSWEB     = -9999
        avgSWE      = -9999
        avgSWE      = -9999

        newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, LAT, LON, avgDensityA,
              avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] # density['# Top (cm)'][0]

        with open(fname_swe,'a', newline='') as fd:
            csv_writer = writer(fd, delimiter=',')
            csv_writer.writerow(newrow)

    elif density['Density A (kg/m3)'].notna().any(): #if Density A has values compute SWE
        # print(f"there are densites")
        SWEA_calc = [0.0] * len(density) #density.shape[0] #len(density) should also work here.
        SWEB_calc = [0.0] * len(density) #density.shape[0]
        sumSWEA=0
        sumSWEB=0
        densityA = 0
        sumDensityA = 0
        avgDensityA = 0
        densityB=0
        sumDensityB=0
        avgDensityB=0
        avgSWE = 0
        avgDens = 0


        # Steps to clean up density profiles:

        # 1. Does top of density match HS? If not, set it to HS
        if density.at[0, '# Top (cm)'] != HeightOfSnow: # a few cases where top of density doesn't match HS
            density.at[0,'# Top (cm)'] = HeightOfSnow # e.g if there's a gap, extrapolate to the top.

        # 2. Is the last density height 0cm? If not, set it to zero (swe is extrapolated to bottom)
        if density.at[len(density)-1, 'Bottom (cm)'] != 0:
            density.at[len(density)-1, 'Bottom (cm)'] = 0 # e.g if it ends at 5cm, make it 0.

        # 3. Is there overlap at the bottom of the snow pit? If so, shorten the bottom segment (more likely to be a worse measurement near the bottom, e.g 16-6, 12-2 --> 16-6, 6-0)
        for i in range(1, len(density)):

            if density.at[i, '# Top (cm)'] > density.at[i-1, 'Bottom (cm)']:
                density.at[i, '# Top (cm)'] = density.at[i-1, 'Bottom (cm)']

        # 4. Are there missing measurements in the duel (A, B) profile? (i.e A=235, B=NaN)
        density['Density A (kg/m3)'].fillna(density['Density B (kg/m3)'], inplace=True)
        density['Density B (kg/m3)'].fillna(density['Density A (kg/m3)'], inplace=True)

        # 5. Are there any places that need interpolation or extrapolation? (the answer is, yes there are some with middle density gaps (MDG) or even one with HS=60 and first density cut at 36)
        density['Density A (kg/m3)'] = density['Density A (kg/m3)'].interpolate(method='linear', limit_direction='both')
        density['Density B (kg/m3)'] = density['Density B (kg/m3)'].interpolate(method='linear', limit_direction='both')

        # 6. Drop Density 'C' column since it's already been averaged by 'B' and will not be further used to compute SWE
        density.drop('Density C (kg/m3)', axis=1, inplace=True)
        # print(f"gapFilled Density:\n {density}")

        # 7. Save the density dataframe that has been gapfilled and used to compute SWE
        density.to_csv(fname_gapFilledDensity, sep=',', index=False, mode='a', na_rep=-9999)


        for i in range(0, len(density)):

            densityA=density['Density A (kg/m3)'][i] #relic code assignment here, cleaner to read densityA and densityB, so it's been left as is.
            densityB=density['Density B (kg/m3)'][i]

            # Calculate SWE for each layer
            SWEA_calc[i] = (density['# Top (cm)'][i] - density['Bottom (cm)'][i])*densityA/100 # SWEA by layer
            SWEB_calc[i] = (density['# Top (cm)'][i] - density['Bottom (cm)'][i])*densityB/100 # SWEB by layer
            sumSWEA = round(sumSWEA + SWEA_calc[i]) # sum SWEA
            sumSWEB = round(sumSWEB + SWEB_calc[i]) # sum SWEB
            sumDensityA = sumDensityA + densityA*(density['# Top (cm)'][i] - density['Bottom (cm)'][i])
            sumDensityB = sumDensityB + densityB*(density['# Top (cm)'][i] - density['Bottom (cm)'][i])


        # calculate weighted average density
        avgDensityA = round(sumDensityA/density['# Top (cm)'][0]) # top of density is now the same as HeightOfSnow
        avgDensityB = round(sumDensityB/density['# Top (cm)'][0]) # top of density is now the same as HeightOfSnow
        avgDens = (avgDensityA + avgDensityB)/2
        avgSWE = (sumSWEA + sumSWEB)/2

        newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, LAT, LON, avgDensityA,
              avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] # density['# Top (cm)'][0]


        with open(fname_swe,'a', newline='') as fd:
            csv_writer = writer(fd, delimiter=',')
            csv_writer.writerow(newrow)


    else:
        print(f"{filename.name} ~~~~~~~~~~~~~~~~~ file SKIP")

        avgDensityA = -9999 # if density is all NAN's, write it as -9999 in the SWE summary file
        avgDensityB = -9999
        avgDens     = -9999
        sumSWEA     = -9999
        sumSWEB     = -9999
        avgSWE      = -9999
        avgSWE      = -9999

        newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, LAT, LON, avgDensityA,
              avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] # density['# Top (cm)'][0]

        with open(fname_swe,'a', newline='') as fd:
            csv_writer = writer(fd, delimiter=',')
            csv_writer.writerow(newrow)

        print('\n')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # static variables
    version = 'v02'
    hsphere = 'N' # northern hemisphere

    # paths
    # path_in = Path('/Users/mamason6/meganmason491@boisestate.edu - Google Drive/My Drive/SnowEx-2020/SnowEx-2020-timeseries-pits/timeseries_pitbook_sheets_EDIT/COMPLETE')
    path_in = Path ('/Users/mamason6/Documents/snowex/campaigns/TS-20/COMPLETE')
    path_out = Path('/Users/mamason6/Documents/snowex/core-datasets/ground/snow-pits/run-WSTS20/outputs')
    pits = path_out.joinpath('pits').mkdir(parents=True, exist_ok=True) # make pit_output path
    boards = path_out.joinpath('boards').mkdir(parents=True, exist_ok=True) # make boards_output path
    fname_summarySWE = path_out.joinpath('SNEX20_TS_SP_Summary_SWE_' + version + '.csv')
    fname_summaryEnviro = path_out.joinpath('SNEX20_TS_SP_Summary_Environment_' + version + '.csv')
    fname_summaryLength = path_out.joinpath('SNEX20_TS_SP_Summary_Length_' + version + '.csv')


    r = writeHeaderRows(fname_summarySWE, metadata_headers_swe)
    r = writeHeaderRows(fname_summaryEnviro, metadata_headers_enviro)


    column = ['Location', 'Site', 'PitID', 'Date/Local Standard Time', 'lenDen', 'lenTemp', 'lenStrat']
    df_lenStat = pd.DataFrame(columns=column)
    df_lenStat.to_csv(fname_summaryLength, index=False, sep=',', header=True)


    # loop over all pit sheets
    for i, filename in enumerate(sorted(path_in.rglob('*.xlsx'))):#CONWFF_20200212_1145, IDBRLT_20200212_1400*, CASHOP_20200304_0957_edit, NMJRBA_20200304_1116_edit, COCPMR_20200212_1348_edit
    # Density A is NAN: COGMCT_20200219_1115_edit
    # empty denisty: NMJRBA_20200212_1337_edit

        print(i, filename.name)

        r = readSnowpit(path_in, filename, version, path_out, fname_summarySWE, fname_summaryEnviro, fname_summaryLength)


    print('.....done for real .....')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# old summary file generation stuff
    # column = [['Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone', 'Easting (m)',
    #            'Northing (m)', 'Latitude (deg)', 'Longitude (deg)', 'Density A Mean (kg/m^3)', 'Density B Mean (kg/m^3)', 'Density Mean (kg/m^3)',
    #            'SWE A (mm)',  'SWE B (mm)', 'SWE (mm)', 'HS (cm)', 'Flag'],
    #            ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17"]]

    # with open(fname_summarySWE, 'w', newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for row in metadata_headers:
    #         writer.writerow(row)

    # df_swe = pd.DataFrame(columns=column)
    # df_swe.to_csv(fname_summarySWE, index=False, sep=',', header=True)
    #
    # column = ['Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone', 'Easting (m)',
    #                'Northing (m)', 'Latitude (deg)', 'Longitude (deg)', 'Precipitation', 'Sky', 'Wind', 'Ground Condition', 'Ground Roughness',
    #                'Ground Vegetation', 'Height of Ground Vegetation (cm)', 'Canopy']
    # df_enviro = pd.DataFrame(columns=column)
    # df_enviro.to_csv(fname_summaryEnviro, index=False, sep=',', header=True)



# old (but modifed) script to compute swe

            # for i in range(0, density.shape[0]): #for each row in the density columns
            #
            #     if(density['# Top (cm)'][0] != HeightOfSnow):  # a few cases where top of density doesn't match HS
            #        density['# Top (cm)'][i] = HeightOfSnow # e.g if there's a gap, interpolate to the top.
            #        # print('#1 Density: ', density[i])
            #
            #     if((i == density.shape[0]-1) & (density['Bottom (cm)'][i] != 0)):
            #         density['Bottom (cm)'][i] = 0 # e.g if it ends at 5cm, make it 0.
            #         # print('#2 Density: ', density)
            #
            #     if i>0 and density['# Top (cm)'][i] != density['Bottom (cm)'][i-1]:  # # if overlapping, shorten the bottom segment...
            #         density['# Top (cm)'][i] = density['Bottom (cm)'][i-1] # e.g 16-6, 12-2 --> 16-6, 6-0
            #         print("~~~~HAS OVERLAP~~~")
            #
            #     # Account for middle density gaps - FLAG=MDG
            #     if density['Density A (kg/m3)'].isnull().any() and density['Density B (kg/m3)'].isnull().any():
            #         print("~~~~HAS GAP~~~")
            #         # print(Flag)
            #
            #         # Interpolate for middle density gap - note this is why the Flags are in place, a user might decide to deal with middle density gaps different. For example, they might look at Stratigraphy.
            #         density.interpolate(method='linear', limit_direction='backward', inplace=True) #limit_area='inside',
            #
            #         # density_interp_and_extrapolate = density.interpolate(method='linear',  limit_direction='backward') #limit_area='inside',
            #         # print('interp results:', density_middle_interp)
            #
            #         # density = density_interp_and_extrapolate
            #         # print('#4 Density: ', density)
            #     if i == density.shape[0]-1:
            #         print("ADJUSTED :\n", density)
            #         density.to_csv(fname_gapFilledDensity, sep=',', index=False, mode='a', na_rep=-9999)

                # # Account for missing density values, row by row
                # if((pd.isna(density['Density A (kg/m3)'][i])) & (pd.isna(density['Density B (kg/m3)'][i]))):
                #     # print('option#1: ')
                #     # print('i is: ', i)
                #     # print('i-1 is: ', i-1)
                #     # densityA=density['Density A (kg/m3)'][i-1]
                #     # densityB=density['Density B (kg/m3)'][i-1]
                #     pass # this is now handled below, with the middle gap...
                #
                # elif((pd.isna(density['Density A (kg/m3)'][i])) & (pd.notna(density['Density B (kg/m3)'][i]))):
                #     densityA=density['Density B (kg/m3)'][i]
                #     densityB=density['Density B (kg/m3)'][i]
                #
                # elif((pd.notna(density['Density A (kg/m3)'][i])) & (pd.isna(density['Density B (kg/m3)'][i]))):
                #     print('option#333333333333')
                #     densityA=density['Density A (kg/m3)'][i]
                #     densityB=density['Density A (kg/m3)'][i]
                #     print(densityB)
                #
                # else:
                #     densityA=density['Density A (kg/m3)'][i]
                #     densityB=density['Density B (kg/m3)'][i]

                # # Account for middle density gaps - FLAG=MDG
                # if density['Density A (kg/m3)'].isnull().any() and density['Density B (kg/m3)'].isnull().any():
                #     # print("~~~~HAS GAP~~~")
                #     # print(Flag)
                #
                #     # Interpolate for middle density gap - note this is why the Flags are in place, a user might decide to deal with middle density gaps different. For example, they might look at Stratigraphy.
                #     density_middle_interp = density.interpolate(method='linear', limit_area='inside', limit_direction='backward')
                #     print('interp results:', density_middle_interp)
                #
                #     density = density_middle_interp
                #     print('HERE!!', density)

                # else:
                #     print("~~~NO GAP~~~")

# old veg stuff
        # VegType=d.iloc[9,5:12:2] # get ground veg info
        # VegBool=d.iloc[10,5:12:2] # Boolean value
    # if VegBool['Unnamed: 12'] == 0: #sloppy fix...but no idea why some 'deadfall - FALSE's' appear as Zero??
    #         VegBool['Unnamed: 12'] = False
    #         # s=VegType[VegBool] # grab veg == True
    #         # GroundVeg=s.values.tolist() # make list of veg type for file
    #         VegType[VegBool].values.tolist()
    # if VegBool.isna().all(): #for pits that don't have any record of True/False
    #     GroundVeg = None
    # else:
    #     s=VegType[VegBool] # grab veg == True
    #     GroundVeg=s.values.tolist() # make list of veg type for file
    # VegHt  = str(d['Unnamed: 8'][11]) # string to allow for "ranges" (e.g. 7-10 cm), 1=True, 0=False (correction below)
    # VegHt2 = str(d['Unnamed: 10'][11])
    # VegHt3 = str(d['Unnamed: 12'][11])
    # VegHts = [VegHt, VegHt2, VegHt3]
    # VegHts = [x for x in VegHts if (str(x) != 'nan' and str(x) != 'cm')] #create a list of VegHts that does not include 'nan'
    # VegHts = [x.replace('False', '') for x in VegHts] # replace any "false" with blank (0 cm means no veg height)
    # VegHts = [x.replace('True', '1') for x in VegHts] # replace any "true" with 1 cm
    # df['value'][9] = str(Slope).replace('nan', 'N/O')
    # df['value'][10] = str(Aspect).replace('nan', 'N/O')
    # df['value'][11] = str(Tair).replace('nan', 'N/O')
    # df['value'][12] = str(HeightOfSnow).replace('nan', 'N/O')
    # df['value'][13] = str(Observers).replace('\n', ' ')
    # df['value'][14] = str(WiseSerialNo).replace('nan', 'N/O')
    # df['value'][15] = str(Weather).replace('\n', ' ').replace('nan', 'N/O')
    # df['value'][16] = str(Precip).replace('nan', 'N/O')
    # df['value'][17] = str(Sky).replace('nan', 'N/O')
    # df['value'][18] = str(Wind).replace('nan', 'N/O')
    # df['value'][19] = str(GroundCondition).replace('nan', 'N/O')
    # df['value'][20] = str(GroundRoughness).replace('nan', 'N/O')
    # df['value'][21] = ", ".join(GroundVeg)
    # if len(df['value'][21])==0:
    #     df['value'][21] = 'N/O'
    # df['value'][22] = str(",".join(VegHts))
    # if len(df['value'][22])==0:
    #     df['value'][22] = -9999
    # df['value'][23] = str(TreeCanopy).replace('nan', 'N/O')
    # df['value'][24] = str(PitComments.split('Flag:')[0].replace('\n', ' ').replace('nan', 'N/O')) # removes the list of Flags if any.
    # df['value'][25] = str(Flag)
        # print(f"GROUND VEG {", ".join(GroundVeg)}")
        # if len(df['value'][21])==0:
        #     df['value'][21] = -9999

        # print(f"VEG HEIGHTS {str(",".join(VegHts)}")
        # if len(df['value'][22])==0:
        #     df['value'][22] = -9999

        # print(f"siteDetails:\n {df}")


        # dataframe to dictionary to write environmental file?

        # newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, LAT, LON, Precip, Sky, Wind,
        #            str(GroundCondition).replace('nan', 'N/O'), str(GroundRoughness).replace('nan', 'N/O'),
        #            df['value'][21], df['value'][22],
        #            str(TreeCanopy).replace('nan', 'N/O')] #str(",".join(GroundVeg)).replace('nan', 'N/O')
        # newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, LAT, LON, Precip, Sky, Wind,
        #            GroundCondition, GroundRoughness, " | ".join(GroundVeg), " | ".join(VegHts), TreeCanopy]
