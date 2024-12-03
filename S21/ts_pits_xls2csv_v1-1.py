#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created from the SNEX21 Time Series script on Tue Aug 30 2022
Original created Sept. 2017 [hpm]
Modified in Dec. 2017 [lb]
Major revision April 2020 [hpm]
Modified in June 2020 (cmv) to add LWC calculation, and output WISe # to
siteDetails.csv
Modified in Oct 2020 (cv) to calculate total density & SWE for summary
files & organize pit files in separate folders
Modified in April 2021 [mmason] to process 2020 Time Series snow pit data (12 time series locations
Modified in August 2022 [mmason] to process 2021 Time Series snow pit data (7 time series locations))
Modified in January 2024 [mmason] to account for 3rd density measurement (avg. w/ 2nd if present)
Modified in January 2024 [mmason] to pull HS for HS in Summary_SWE file (previously pulled top of density)
"""
__author__ = "Megan Mason, NASA Goddard / SSAI"
__version__ = "01-1"
__maintainer__ = "HP Marshall" # github SnowEx2020_GrandMesa_core
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

# custom imports
from metadata_headers import metadata_headers_swe, metadata_headers_enviro

# utm zone dictionary
utmzone_dict = {
'Senator Beck': 13,
'Little Cottonwood Canyon': 12,
'Boise River Basin': 11,
'Central Ag Research Center': 12,
'Cameron Pass': 13,
'Fraser Experimental Forest': 13,
'Grand Mesa': 12
}

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

    # paths
    pitPath = path_out.joinpath('pits/' + filename.parts[-3] + '/' + filename.stem[:-5] + '/') #snow pits ([:-5] without '_edit')
    # boardPath = path_out.joinpath('boards/' + filename.stem[:-5] + '/') #interval boards
    if not Path.exists(pitPath): #try list comp mkdir() for this if doesn't exist?
        Path(pitPath).mkdir(parents=True, exist_ok=True)
    # if not Path.exists(boardPath):
    #     boardPath.mkdir()

    if Path(filename).suffix == '.jpg': #throw an error if there are any other file extentions
        pass

    elif Path(filename).suffix == '.xlsx':
        newfilename = Path('SNEX21_TS_SP_' + uniqueID + '_pitSheet_' + version + '.xlsx')

        shutil.copyfile(filename, pitPath.joinpath(newfilename))


    # open excel file
    xl = pd.ExcelFile(pitPath.joinpath(newfilename))

    # create individual output file names
    fname_density         = pitPath.joinpath('SNEX21_TS_SP_'+ uniqueID + '_density_' + version +'.csv')
    fname_LWC             = pitPath.joinpath('SNEX21_TS_SP_'+ uniqueID + '_LWC_' + version +'.csv')
    fname_temperature     = pitPath.joinpath('SNEX21_TS_SP_'+ uniqueID + '_temperature_' + version +'.csv')
    fname_stratigraphy    = pitPath.joinpath('SNEX21_TS_SP_'+ uniqueID + '_stratigraphy_' + version +'.csv')
    fname_siteDetails     = pitPath.joinpath('SNEX21_TS_SP_'+ uniqueID + '_siteDetails_' + version +'.csv')
    fname_gapFilledDensity= pitPath.joinpath('SNEX21_TS_SP_'+ uniqueID + '_gapFilledDensity_' + version +'.csv')

    # header data
    d = pd.read_excel(xl, nrows=7) # stops at end of header data, row 7 (this could work to grab everything until 'density'-->  d.loc[:(d['Unnamed: 1'] == 'Density').idxmax()-1])
    Location = d['Unnamed: 1'][1] # get location name
    Site = d['Unnamed: 1'][4]  # get site name
    PitID = d['Unnamed: 1'][6] #+ '_' + d['Unnamed: 7'][4].strftime('%H%M') # unique identifier
    Datetime = d['Unnamed: 7'][1] # get date
    lat = d['Unnamed: 9'][6] # get lat
    lon = d['Unnamed: 13'][6] # get lon

    # empty
    if lat == None:
        lat = np.nan
        lon = np.nan

    # If Lat = negative, swap Lat/Lon #shouldn't be true after qa/qc, but good check
    if lat < 0: #recorded as Longitude (negative value)
        lat, lon = lon, lat # swap coards

    # if Lat = Lon, swap Lat/Lon
    if lat > 4000000:
        lat, lon = lon, lat # swap coards

    # convert UTMs to Lat/Lon
    if lat > 90: #recorded as UTMs
        UTME = lat
        UTMN = lon

        lat = utm.to_latlon(UTME, UTMN, utmzone_dict.get(Location), "Northern")[0] #tuple output, save first

    if lon > 0:
        lon = utm.to_latlon(UTME, UTMN, utmzone_dict.get(Location), "Northern")[1] #tuple output, save second

    lat = round(lat, 5)
    lon = round(lon, 5)

    UTME = round(utm.from_latlon(lat, lon)[0])
    UTMN = round(utm.from_latlon(lat, lon)[1])
    UTMzone = utmzone_dict.get(Location)


    HeightOfSnow = d['Unnamed: 5'][6]  # get total depth
    WiseSerialNo = d['Unnamed: 7'][6]
    pit_time = d['Unnamed: 7'][4]
    pit_date = d['Unnamed: 7'][1]
    pit_datetime=datetime.datetime.combine(pit_date, pit_time)
    pit_datetime_str=pit_datetime.strftime('%Y-%m-%dT%H:%M')
    Observers=d['Unnamed: 9'][1]
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


    # create minimal header info for other files
    index = ['# Location', '# Site', '# PitID', '# Date/Local Standard Time',
         '# UTM Zone', '# Easting', '# Northing', '# Latitude', '# Longitude', '# Flags', '# Pit Comments', '# Parameter Codes']
    column = ['value']
    df = pd.DataFrame(index=index, columns=column)
    df['value'][0] = Location
    df['value'][1] = str(Site)
    df['value'][2] = str(PitID)
    df['value'][3] = pit_datetime_str
    df['value'][4] = str(UTMzone)+hsphere
    df['value'][5] = UTME
    df['value'][6] = UTMN
    df['value'][7] = lat
    df['value'][8] = lon
    df['value'][9] = Flag
    df['value'][10] = PitComments
    df['value'][11] = 'n/a for this parameter'


    # add minimal header to each data file
    df.to_csv(fname_density, sep=',', header=False)
    df.to_csv(fname_LWC, sep=',', header=False)
    df.to_csv(fname_temperature, sep=',', header=False)
    # df.to_csv(fname_stratigraphy, sep=',', header=False)
    df.to_csv(fname_gapFilledDensity, sep=',', header=False)

    df['value'][11] = "Grain Size: <1mm, 1-2mm, 2-4mm, 4-6mm, >6mm; Grain Type: SH=Surface Hoar, PP=New Snow, DF=Decomposing Forms, RG=Rounded Grains, FC=Faceted Crystals, MF=Melt Forms, IF=Ice Lens, MFcr=Melt Crust, FCsf=Near-surface Facets, PPgp=Graupel; Hand Hardness: F=Fist, 4F=4-finger, 1F=1-finger, P=Pencil, K=Knife, I=Ice; Manual Wetness: D=Dry, M=Moist, W=Wet, V=Very Wet, S=Soaked"
    df.to_csv(fname_stratigraphy, sep=',', header=False)


    #~~~~~~MMn's big update:
    d = pd.read_excel(xl, usecols='B:U')
    rIx = (d.iloc[:,0] == 'Weather Description:').idxmax() #locate 'Weather:' cell in spreadsheet (row Index)
    d = d.loc[rIx:,:].reset_index(drop=True) # subset dataframe from 'Weather:' cell down to bottom, and reset index (not always fixed due to extra rows in above measurements)
    Weather = d['Unnamed: 1'][1] # this works too: d.iloc[1,0]
    PrecipType=d['Unnamed: 11'][4]
    PrecipRate=d['Unnamed: 11'][2]
    Sky=d['Unnamed: 11'][5]
    Wind=d['Unnamed: 11'][6]
    GroundCondition=d['Unnamed: 12'][7]
    GroundRoughness=d['Unnamed: 12'][8]
    # Veg=d.iloc[9,11:19]
    VegType=d.iloc[9,11:18:2] # get ground veg info (Bare True, Grass False, etc.) - grabs type option
    VegBool=d.iloc[9,12:19:2] # Boolean value - True/False bool to right of each type
    VegHts = d.iloc[10,11:18:2] # Veg Heights - one row lower, under veg type
    Veg= pd.DataFrame({'VegType': VegType.values, 'VegBool': VegBool.values, 'VegHts': VegHts.values})
    Veg['VegBool'] = Veg['VegBool'].astype(bool) # assign as bool type
    Veg['VegHts'] = Veg['VegHts'].where((~Veg['VegBool']) | (Veg.index != 0), 0) # if 'Bare' is true, assign veg height of 0 cm
    Veg['VegHts'] = Veg['VegHts'].where(~(Veg['VegBool'] & Veg['VegHts'].isna()), -9999) # if veg type=TRUE and veg htn=NaN, assign -9999.

    if VegBool.isna().all(): #for pits that don't have any record of True/False
        GroundVeg = np.nan
    else:
        GroundVeg=Veg.where(Veg.VegBool).dropna().VegType.values.tolist() # list of ground veg (e.g. ['Bare', 'Shrub'])
        VegHts=Veg.where(Veg.VegBool).dropna().VegHts.values.tolist() # list of veg heights (e.g. [0, 15])

    TreeCanopy=d['Unnamed: 12'][11]


    # fix sky --> fractions interpreted as symbols in csv:
    # print('SKY IS~~~', Sky, type(Sky))
    if isinstance(Sky, str):
        Sky = Sky.replace('\n', ' ')
        if "Few" in Sky:
            Sky = 'Few (<1/4 of sky)'
        if "Scattered" in Sky:
            Sky = 'Scattered (1/4 - 1/2 of sky)'
        if "Broken" in Sky:
            Sky = 'Broken (>1/2 of sky)'

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
    df['value'][7] = lat
    df['value'][8] = lon
    df['value'][9] = None # (Slope) data NOT collected in SNEX-21, but left in to match data template
    df['value'][10] = None # (Aspect) data NOT collected in SNEX-21, but left in to match data template
    df['value'][11] = None # (Air Temp) data NOT collected in SNEX-21, but left in to match data template
    df['value'][12] = HeightOfSnow
    df['value'][13] = str(Observers).replace('\n', ' ')
    df['value'][14] = WiseSerialNo
    df['value'][15] = str(Weather).replace('\n', ' ')
    df['value'][16] = PrecipType
    df['value'][17] = PrecipRate
    df['value'][18] = Sky
    df['value'][19] = Wind
    df['value'][20] = GroundCondition
    df['value'][21] = GroundRoughness
    df['value'][22] = " | ".join(GroundVeg)
    df['value'][23] = " | ".join(str(x) for x in VegHts)
    df['value'][24] = TreeCanopy
    df['value'][25] = str(PitComments.split('Flag:')[0].replace('\n', ' ')) # removes the list of Flags if any.
    df['value'][26] = str(Flag)
    df.to_csv(fname_siteDetails, sep=',', header=False, na_rep=-9999, encoding='utf-8-sig')
    print('wrote: .../' + fname_siteDetails.name)

    newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, lat, lon,
              df['value'][16], # Precip Type
              df['value'][17], # Precip Rate
              df['value'][18], # Sky str(Sky).replace('\n', ' ')
              df['value'][19], # Wind
              df['value'][20], # Ground Condition
              df['value'][21], # Ground Roughness
              df['value'][22], # Ground Vegetation (list, i.g. Bare | Grass | Tree)
              df['value'][23], # Veg height (list)
              df['value'][24]] # TreeCanopy


    with open(fname_enviro, 'a', newline='') as fd:
        csv_writer = writer(fd, delimiter=',')
        csv_writer.writerow(newrow)

    # get density
    d = pd.read_excel(xl, header=10, usecols='B:G').replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['top\n(cm)'].isnull().values == True))[0]
    d = d.iloc[0:first_nan]
    d.columns = ['# Top (cm)', '-', 'Bottom (cm)', 'Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)']
    den_cols = ['# Top (cm)', 'Bottom (cm)', 'Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)'] #gets rid of the '-' column
    density = d[den_cols].astype(float)
    print('RAW density:\n', density)
    lenDen = len(density.index)
    density.to_csv(fname_density, sep=',', index=False, mode='a', na_rep=-9999) #write density csv (with NaN's)
    # average 3rd sample (if taken) with profile B (this overwrites B in the dataframe)
    density['Density B (kg/m3)'] = density[['Density B (kg/m3)', 'Density C (kg/m3)']].mean(axis=1) # mean of profile B with any "extra" density samples (i.e. C)
    # density.drop(columns=['Density C (kg/m3)'], inplace=True) # drop 'C', no way to accidently use it now.
    AvgDensity=density[['Density A (kg/m3)', 'Density B (kg/m3)']].mean(axis=1) # B is averaged in the line above with C, now mean(A,B) (tech. mean(A,(B,C)))
    # density.dropna(subset= ['Density A (kg/m3)', 'Density B (kg/m3)'], how='all', inplace=True) #remove NaN's for calculations below
    # density.reset_index(drop=True, inplace=True) #probs a better solution, but helps the code run below.

    if not density.empty and density['Density A (kg/m3)'].notna().any(): # if the Density A profile isn't ALL NAN's ("true" if there are 'any' non-nans)
        first_non_nan_index = density['Density A (kg/m3)'].first_valid_index()
        first_density_height = density.loc[first_non_nan_index, '# Top (cm)']
        print(f"TOP OF DENSITY V. HS: {first_density_height} --> {HeightOfSnow}")
    else:
        first_density_height = None

    print('wrote: .../' + fname_density.name) #.split('/')[-1]

    # get LWC
    d = pd.read_excel(xl, header=10, usecols='B:I').replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['top\n(cm)'].isnull().values == True))[0] # get the index of the first nan value
    d = d.iloc[0:first_nan]
    d.columns = ['# Top\n(cm)', '-', 'Bottom\n(cm)', 'kg/m3', 'kg/m3.1', 'kg/m3.2',
       'Permittivity A', 'Permittivity B'] # last col is temp distance, without it stuff breaks below...sloppy fix, but it's not used here.
    d = d.rename(columns={'# Top\n(cm)': '# Top (cm)', 'Bottom\n(cm)': 'Bottom (cm)'}) # rename without \n for snowex database
    lwc_cols=['# Top (cm)','Bottom (cm)','Permittivity A','Permittivity B']
    LWC = d[lwc_cols].astype(float)
    LWC.insert(2, 'Avg Density (kg/m3)', AvgDensity, False)

    #Calculate LWC
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
            LWCA_calc[i] = np.nan
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
    # LWC['LWC-vol A (%)'] = LWC['LWC-vol A (%)'].map(lambda x: '%2.1f' % x) # one way to set sig figs, but turns np.NaN to nan
    LWC.insert(6, "LWC-vol B (%)", LWCB_calc, False)
    # LWC['LWC-vol B (%)'] = LWC['LWC-vol B (%)'].map(lambda x: '%2.1f' % x)
    LWC[['LWC-vol A (%)', 'LWC-vol B (%)']] = LWC[['LWC-vol A (%)', 'LWC-vol B (%)']].astype(float).round(2) # if values are floats, round them
    AvgPerm=LWC[['Permittivity A', 'Permittivity B']].mean(axis=1)# pd.Series
    AvgLWC=LWC[['LWC-vol A (%)', 'LWC-vol B (%)']].mean(axis=1) # pd.Series
    LWC.to_csv(fname_LWC, sep=',', index=False, mode='a', na_rep=-9999, encoding='utf-8')
    print('wrote: .../' + fname_LWC.name)


    # get temperature
    d = pd.read_excel(xl, header=10, usecols='J:K').replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['(cm)'].isnull().values == True))[0]
    temperature = d.iloc[0:first_nan].astype(float)
    lenTemp = len(temperature.index)
    d = pd.read_excel(xl, header=4, usecols='U:V').replace(r'^\s*$', np.nan, regex=True)
    last_row_value = temperature.shape[0]-1
    temperature['Time start/end'] = None # add column for start/end time (new SNEX21)
    temperature.at[0, 'Time start/end'] = d['START'][0] if not pd.isnull(d['START'][0]) else -9999
    temperature.at[last_row_value, 'Time start/end'] = d['END'][0] if not pd.isnull(d['END'][0]) else -9999
    temperature.columns = ['# Depth (cm)', 'Temperature (deg C)', 'Time start/end']
    temperature.to_csv(fname_temperature, sep=',', index=False, mode='a', na_rep=-9999)
    print('wrote: .../' + fname_temperature.name)


    # get stratigraphy
    d = pd.read_excel(xl, header=10, usecols='M:X').replace(r'^\s*$', np.nan, regex=True)
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
            lambda x: np.nan if isinstance(x, float) else x) # removed x.upper - it made MFcr --> MFCR

    if stratigraphy['Hand Hardness'].count() > 0:
        stratigraphy['Hand Hardness'] = stratigraphy['Hand Hardness'].apply(
            lambda x: np.nan if isinstance(x, float) else x.upper())

    if stratigraphy['Manual Wetness'].count() > 0:
        stratigraphy['Manual Wetness'] = stratigraphy['Manual Wetness'].apply(
            lambda x: np.nan if isinstance(x, float) else x.upper())

    if stratigraphy['Grain Size (mm)'].count() > 0:
        stratigraphy['Grain Size (mm)'] = stratigraphy['Grain Size (mm)'].apply(
            lambda x: np.nan if isinstance(x, float) else x.replace('\n', ' '))

    stratigraphy.to_csv(fname_stratigraphy, sep=',', index=False,
                        mode='a', na_rep=-9999, encoding='utf-8')
    lenStrat = len(stratigraphy.index)
    print('wrote: .../' + fname_stratigraphy.name)

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

        newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, lat, lon,
        avgDensityA,avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] # density['# Top (cm)'][0]

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
        print(f"gapFilled Density:\n {density}")

        # 7. Save the density dataframe that has been gapfilled and used to compute SWE
        density.to_csv(fname_gapFilledDensity, sep=',', index=False, mode='a', na_rep=-9999)

                # print(density)
        for i in range(0, len(density)):

            densityA=density['Density A (kg/m3)'][i] #relic code assignment here, cleaner to read densityA and densityB, so it's been left as is.
            densityB=density['Density B (kg/m3)'][i]

            # Calculate SWE for each layer
            SWEA_calc[i] = (density['# Top (cm)'][i] - density['Bottom (cm)'][i])*densityA/100
            SWEB_calc[i] = (density['# Top (cm)'][i] - density['Bottom (cm)'][i])*densityB/100
            sumSWEA = round(sumSWEA + SWEA_calc[i])
            sumSWEB = round(sumSWEB + SWEB_calc[i])
            sumDensityA = sumDensityA + densityA*(density['# Top (cm)'][i] - density['Bottom (cm)'][i])
            sumDensityB = sumDensityB + densityB*(density['# Top (cm)'][i] - density['Bottom (cm)'][i])


        # calculate weighted average density
        avgDensityA = sumDensityA/density['# Top (cm)'][0]
        avgDensityB = sumDensityB/density['# Top (cm)'][0]
        avgDens = round((avgDensityA + avgDensityB)/2) #bulk density weighted by sample height
        avgSWE = round((sumSWEA + sumSWEB)/2) #bulk SWE " "
        avgPerm = AvgPerm.mean()
        avgLWC = AvgLWC.mean()

        # print('AVG density', avgDens)

        avgDensityA = round(avgDensityA)
        avgDensityB = round(avgDensityB)

        newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, lat, lon, avgDensityA,
              avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] #HeightOfSnow is HS value in pitsheet

        with open(fname_swe,'a',newline='') as fd:
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

        newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, lat,lon, avgDensityA,
              avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] # density['# Top (cm)'][0]

        with open(fname_swe,'a', newline='') as fd:
            csv_writer = writer(fd, delimiter=',')
            csv_writer.writerow(newrow)

        print('\n')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == "__main__":
    # static variables
    version = 'v01'
    hsphere = 'N' # northern hemisphere

    # paths
    # path_in = Path('/Users/mamason6/Google Drive/My Drive/SnowEx-2021/SnowEx-2021-campaign-core/SnowEx2021_TimeSeries_Pits/2_edits')
    path_in = Path('/Users/mamason6/Documents/snowex/campaigns/TS-21/SnowEx2021_TimeSeries_Pits/2_edits')
    path_out = Path('/Users/mamason6/Documents/snowex/core-datasets/ground/snow-pits/run-WSTS21/output')
    pits = path_out.joinpath('pits').mkdir(parents=True, exist_ok=True) # make pit_output path
    # boards = path_out.joinpath('boards').mkdir(parents=True, exist_ok=True) # make boards_output path
    fname_summarySWE = path_out.joinpath('SNEX21_TS_SP_Summary_SWE_' + version + '.csv')
    fname_summaryEnviro = path_out.joinpath('SNEX21_TS_SP_Summary_Environment_' + version + '.csv')
    fname_summaryLength = path_out.joinpath('SNEX21_TS_SP_Summary_Length_' + version + '.csv')


    r = writeHeaderRows(fname_summarySWE, metadata_headers_swe)
    r = writeHeaderRows(fname_summaryEnviro, metadata_headers_enviro)

    # # headers and summary files
    # column = ['Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone', 'Easting (m)',
    #             'Northing (m)', 'Latitude (deg)', 'Longitude (deg)', 'Density A Mean (kg/m^3)', 'Density B Mean (kg/m^3)', 'Density Mean (kg/m^3)',
    #             'SWE A (mm)',  'SWE B (mm)', 'SWE (mm)', 'Snow Depth (cm)', 'HS (cm)', 'Flag']
    # df_swe = pd.DataFrame(columns=column)
    # df_swe.to_csv(fname_summarySWE, index=False, sep=',', header=True)
    #
    # column = ['Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone', 'Easting (m)',
    #             'Northing (m)', 'Latitude (deg)', 'Longitude (deg)', 'Precipitation', 'Sky', 'Wind', 'Ground Condition', 'Ground Roughness',
    #             'Ground Vegetation', 'Height of Ground Vegetation (cm)', 'Canopy']
    # df_enviro = pd.DataFrame(columns=column)
    # df_enviro.to_csv(fname_summaryEnviro, index=False, sep=',', header=True, encoding='utf-8-sig')


    column = ['Location', 'Site', 'PitID', 'Date/Local Standard Time', 'lenDen', 'lenTemp', 'lenStrat']
    df_lenStat = pd.DataFrame(columns=column)
    df_lenStat.to_csv(fname_summaryLength, index=False, sep=',', header=True)


    # loop over all pit sheets
    # for i, filename in enumerate(sorted(path_in.rglob('*.xlsx'))):
    for i, filename in enumerate(sorted(path_in.rglob('*.xlsx'))):
        print('\n')
        print(i, filename.name)

        # xl = pd.ExcelFile(path_in.joinpath(filename)) #THIS THIS OFF RUNNING FULL SCRIPT
        r = readSnowpit(path_in, filename, version, path_out, fname_summarySWE, fname_summaryEnviro, fname_summaryLength) #coord_dict,

    # run everything with try/except
    #     print(filename.name)
    #     try:
    #         r = readSnowpit(path_in, filename, coord_dict, version, path_out, fname_summarySWE, fname_summaryEnviro)
    #     except:
    #         print("This file won't run: ", filename.name)
    print('.....done for real .....')





    # OLD - density
        # if not density.empty: #if 'density' isn't an empty dataframe do the following: (CHANGE TO NOTNA().all())
        #     SWEA_calc = [0.0] * density.shape[0] #len(density) should also work here.
        #     SWEB_calc = [0.0] * density.shape[0]
        #     sumSWEA=0
        #     sumSWEB=0
        #     densityA = 0
        #     sumDensityA = 0
        #     avgDensityA = 0
        #     densityB=0
        #     sumDensityB=0
        #     avgDensityB=0
        #     avgSWE = 0
        #     avgDens = 0

                # for i in range(0, density.shape[0]): #for each row in the density columns
                #
                #     if(density['# Top (cm)'][0] != HeightOfSnow):  # a few cases where top of density doesn't match HS
                #        density['# Top (cm)'][i] = HeightOfSnow
                #
                #     if((i == density.shape[0]-1) & (density['Bottom (cm)'][i] != 0)):  # assume last density measurement to ground surface
                #        density['Bottom (cm)'][i] = 0 # e.g if it ends at 5cm, make it 0.
                #
                #     if i>0 and density['# Top (cm)'][i] != density['Bottom (cm)'][i-1]:  # # if overlapping, shorten the bottom segment...
                #         density['# Top (cm)'][i] = density['Bottom (cm)'][i-1] # e.g 16-6, 12-2 --> 16-6, 6-0
                #
                #
                #     # Account for missing density values
                #     if((pd.isna(density['Density A (kg/m3)'][i])) & (pd.isna(density['Density B (kg/m3)'][i]))):
                #         densityA=density['Density A (kg/m3)'][i-1]
                #         densityB=density['Density B (kg/m3)'][i-1]
                #         # print('first if') # if both rows are blank grab the density above (This should never be a case using the post-prossed data)
                #
                #     elif((pd.isna(density['Density A (kg/m3)'][i])) & (pd.notna(density['Density B (kg/m3)'][i]))):
                #         densityA=density['Density B (kg/m3)'][i]
                #         densityB=density['Density B (kg/m3)'][i]
                #         # print('second if') # if profile A is NAN, use the adjacent row value in B
                #
                #     elif((pd.notna(density['Density A (kg/m3)'][i])) & (pd.isna(density['Density B (kg/m3)'][i]))):
                #         densityA=density['Density A (kg/m3)'][i]
                #         densityB=density['Density A (kg/m3)'][i]
                #         # print('third if') # if profile B is NAN, use the adjacent row value in A (VERLY LIKELY CASE)
                #
                #     else:
                #         densityA=density['Density A (kg/m3)'][i]
                #         densityB=density['Density B (kg/m3)'][i]
                        # print('fourth if') # ideal case for fully sampled duel profile (interpolation to bottom handled above)
