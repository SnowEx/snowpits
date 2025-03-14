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

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------

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

    pit_datetime=datetime.datetime.combine(date, time)
    pit_datetime_str=pit_datetime.strftime('%Y-%m-%dT%H:%M')

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

#-------------------------------------------------------------------------------
# write_parameter_header: writes header rows to parameter files
def write_parameter_header(metadata, file_path):

    p_codes = 'n/a for this parameter'
    p_codes_lwc = f"LWC Sensor Serial No. {metadata['WiseSerialNo']}"
    p_codes_strat = (
        "Grain Size: <1mm, 1-2mm, 2-4mm, 4-6mm, >6mm; "
        "Grain Type: SH=Surface Hoar, PP=Precipitation Particles, DF=Decomposing and Fragments, "
        "RG=Rounded Grains, FC=Faceted Crystals, MF=Melt Forms, IF=Ice Formations, MFcr=Melt Crust, "
        "FCsf=Near-surface Facets, PPgp=Graupel; "
        "Hand Hardness: F=Fist, 4F=4-finger, 1F=1-finger, P=Pencil, K=Knife, I=Ice; "
        "Manual Wetness: D=Dry, M=Moist, W=Wet, V=Very Wet, S=Soaked"
    )

    # Check if file_path contains '_stratigraphy' and adjust p_codes header line
    if '_lwc' in str(file_path):
        p_codes = p_codes_lwc
    elif '_stratigraphy' in str(file_path):
        p_codes = p_codes_strat  

    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)

        writer.writerow(["# Location", metadata["Location"]])
        writer.writerow(["# Site", metadata["Site"]])
        writer.writerow(["# PitID", metadata["PitID"]])
        writer.writerow(["# Date/Local Standard Time", metadata["Datetime_str"]])
        writer.writerow(["# UTM Zone", metadata["Zone"]])
        writer.writerow(["# Easting", metadata["Easting"]])
        writer.writerow(["# Northing", metadata["Northing"]])
        writer.writerow(["# Latitude", metadata["Latitude"]])
        writer.writerow(["# Longitude", metadata["Longitude"]])
        writer.writerow(["# GPS & Uncert.", metadata["GPS & Uncert."]])
        writer.writerow(["# Observer(s)", metadata["Observers"]])
        writer.writerow(["# Flag", metadata["Flag"]])
        writer.writerow(["# Pit Comments", metadata["Pit Comments"]])
        writer.writerow(["# Parameter Codes", p_codes])
  
        
#-------------------------------------------------------------------------------    
# get_density() get density data from pit sheet
def get_density(filename, HeightOfSnow, fname_density):
    d = pd.read_excel(filename, header=10, usecols='B:G').replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['top\n(cm)'].isnull().values == True))[0] # located the bottom (i.e. find first null)
    d = d.iloc[0:first_nan] # keep top to bottom
    d.columns = ['# Top (cm)', '-', 'Bottom (cm)', 'Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)']
    den_cols = ['# Top (cm)', 'Bottom (cm)', 'Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)'] #gets rid of the '-' column
    density = d[den_cols].astype(float)
    # print('RAW density:\n', density)
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
    
    return density, AvgDensity

#------------------------------------------------------------------------------- 
# get_SWE() computes SWE and creates a gapFilled Density dataframe, that is saved
def get_SWE(filename, density, metadata, fname_swe, fname_gapFilledDensity):
    
   print('I AM SWE FILE NAME:', fname_swe)
   if density.empty: # this wont run if density isn't in the parameter list
       print(f"{filename.name} ~~~~~~~~~~~~~~~~~ file SKIP")

       avgDensityA = -9999 # if density is empty, write it as -9999 in the SWE summary file (e.g. several snow pits completed, and one is missing a density profile)
       avgDensityB = -9999
       avgDens     = -9999
       sumSWEA     = -9999
       sumSWEB     = -9999
       avgSWE      = -9999       

       newrow = [metadata["Location"], 
                 metadata["Site"],
                 metadata["PitID"], 
                 metadata["Datetime_str"], 
                 metadata["Zone"], 
                 metadata["Easting"], 
                 metadata["Northing"], 
                 metadata["Latitude"], 
                 metadata["Longitude"],
                 avgDensityA,
                 avgDensityB, 
                 avgDens, 
                 sumSWEA, 
                 sumSWEB, 
                 avgSWE, 
                 metadata["HS (cm)"], 
                 metadata["Flag"]] 
       
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
       if density.at[0, '# Top (cm)'] != metadata["HS (cm)"]: 
           density.at[0,'# Top (cm)'] = metadata["HS (cm)"] # e.g if there's a gap, extrapolate to the top

       # 2. Is the last density height 0cm? If not, set it to zero (swe is extrapolated to bottom)
       if density.at[len(density)-1, 'Bottom (cm)'] != 0:
           density.at[len(density)-1, 'Bottom (cm)'] = 0 # e.g if it ends at 5cm, make it 0. (NOTE, if working with large/frequent bottom of the snow air gaps, you sould modify this or look to S23 for SnowEx23 in Alaska)

       # 3. Is there overlap at the bottom of the snow pit? If so, shorten the bottom segment (more likely to be a worse measurement near the bottom, e.g 16-6, 12-2 --> 16-6, 6-0)
       for i in range(1, len(density)):
           if density.at[i, '# Top (cm)'] > density.at[i-1, 'Bottom (cm)']:
               density.at[i, '# Top (cm)'] = density.at[i-1, 'Bottom (cm)']

       # 4. Are there missing measurements in the dual (A, B) profile? (i.e A=235, B=NaN)
       density['Density A (kg/m3)'].fillna(density['Density B (kg/m3)'], inplace=True) # fill empty B with A (better than interpolating)
       density['Density B (kg/m3)'].fillna(density['Density A (kg/m3)'], inplace=True) # fill empty A with B (better than interpolating)

       # 5. Are there any places that need interpolation or extrapolation? (the answer is, yes possible to have middle density gaps (MDG) due to observer sampling strategy
       density['Density A (kg/m3)'] = density['Density A (kg/m3)'].interpolate(method='linear', limit_direction='both')
       density['Density B (kg/m3)'] = density['Density B (kg/m3)'].interpolate(method='linear', limit_direction='both')

       # 6. Drop Density 'C' column since it's already been averaged by 'B' and will not be further used to compute SWE (NOTE, probably better to average 'C' with 'A' and 'B', but to compute sweA and sweB you need denA and denB column and can't average all to a single row, bulk density.)
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
       
       print('AVG-DEN:', avgDens)
       print('AVG-SWE:', avgSWE)


       # print('AVG density', avgDens)

       avgDensityA = round(avgDensityA)
       avgDensityB = round(avgDensityB)

       newrow = [metadata["Location"], 
                 metadata["Site"],
                 metadata["PitID"], 
                 metadata["Datetime_str"], 
                 metadata["Zone"], 
                 metadata["Easting"], 
                 metadata["Northing"], 
                 metadata["Latitude"], 
                 metadata["Longitude"],
                 avgDensityA,
                 avgDensityB, 
                 avgDens, 
                 sumSWEA, 
                 sumSWEB, 
                 avgSWE, 
                 metadata["HS (cm)"], 
                 metadata["Flag"]] 
       
       print('~~~~~~~~~~~:', newrow)
       
       with open(fname_swe,'a', newline='') as fd:
           csv_writer = writer(fd, delimiter=',')
           csv_writer.writerow(newrow)

   # else:
   #     print(f"{filename.name} ~~~~~~~~~~~~~~~~~ file SKIP")

   #     avgDensityA = -9999 # if density is all NAN's, write it as -9999 in the SWE summary file
   #     avgDensityB = -9999
   #     avgDens     = -9999
   #     sumSWEA     = -9999
   #     sumSWEB     = -9999
   #     avgSWE      = -9999
   #     avgSWE      = -9999

   #     newrow = [Location, Site, PitID, pit_datetime_str, str(UTMzone)+hsphere, UTME, UTMN, lat,lon, avgDensityA,
   #           avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, HeightOfSnow, Flag] # density['# Top (cm)'][0]

   #     with open(fname_swe,'a', newline='') as fd:
   #         csv_writer = writer(fd, delimiter=',')
   #         csv_writer.writerow(newrow)

   #     print('\n')

#-------------------------------------------------------------------------------
# get_LWC: grabs liquid water content and solves %-vol water in profile
def get_lwc(filename, fname_lwc, AvgDensity):
    d = pd.read_excel(filename, header=10, usecols='B:I').replace(r'^\s*$', np.nan, regex=True)
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
    LWC.insert(6, "LWC-vol B (%)", LWCB_calc, False)
    LWC[['LWC-vol A (%)', 'LWC-vol B (%)']] = LWC[['LWC-vol A (%)', 'LWC-vol B (%)']].astype(float).round(2) # if values are floats, round them
    LWC.to_csv(fname_lwc, sep=',', index=False, mode='a', na_rep=-9999, encoding='utf-8')
    print('wrote: .../' + fname_lwc.name)
    
#-------------------------------------------------------------------------------
# get_temp() gets the temperature profile from the pit sheet & start/end time
def get_temp(filename, fname_temperature):
   
    d = pd.read_excel(filename, header=10, usecols='J:K').replace(r'^\s*$', np.nan, regex=True)
    first_nan = min(np.where(d['(cm)'].isnull().values == True))[0]
    temperature = d.iloc[0:first_nan].astype(float)

    d = pd.read_excel(filename, header=4, usecols='U:V').replace(r'^\s*$', np.nan, regex=True)
    last_row_value = temperature.shape[0]-1
    temperature['Time start/end'] = None # add column for start/end time (new SNEX21)
    temperature.at[0, 'Time start/end'] = d['START'][0] if not pd.isnull(d['START'][0]) else -9999
    temperature.at[last_row_value, 'Time start/end'] = d['END'][0] if not pd.isnull(d['END'][0]) else -9999
    temperature.columns = ['# Depth (cm)', 'Temperature (deg C)', 'Time start/end']
    temperature.to_csv(fname_temperature, sep=',', index=False, mode='a', na_rep=-9999)
    print('wrote: .../' + fname_temperature.name)

#-------------------------------------------------------------------------------
# get_stratigraphy() gets stratigraphy profile 
def get_stratigraphy(filename, fname_stratigraphy):
    d = pd.read_excel(filename, header=10, usecols='M:X').replace(r'^\s*$', np.nan, regex=True)
    first_double_nan = min(np.where(pd.Series(d['top\n(cm).1'].isnull().values).rolling(2).sum().values == 2))[0] # because .xlsx cells are merged
    d = d.iloc[0:first_double_nan]
    d.columns = ['# Top (cm)', '-.1', 'Bottom (cm)', 'Grain Size (mm)', '1-2 mm', '2-4 mm',
       '4-6 mm', '> 6 mm', 'Grain Type', 'Hand Hardness', 'Manual Wetness', 'Comments'] #rename them here, all of them
    strat_cols = ['# Top (cm)', 'Bottom (cm)', 'Grain Size (mm)', 'Grain Type',
                    'Hand Hardness','Manual Wetness', 'Comments'] #select which onces for the CSV file
    stratigraphy = d[strat_cols].dropna(how='all')
    stratigraphy.to_csv(fname_stratigraphy, sep=',', index=False,
                        mode='a', na_rep=-9999, encoding='utf-8')
    print('wrote: .../' + fname_stratigraphy.name)
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------



# -----------------------------------------------------------------------------
# run main
#------------------------------------------------------------------------------

if __name__ == "__main__":
    
    # -------------------------------------------------------------------------
    # initial processing set up - CHANGE SCRIPT HERE
    #--------------------------------------------------------------------------
    
    # static variables
    campaign_prefix = 'campaign_prefix' # ENTER YOUR FIELD CAMPAIGN PREFIX (e.g. SnowEx23_MAR23_AKIOP, SNOWWI_MoresCreek, etc.)
    # version = 'v01'
    parameter_list = ['density', 'gapFilledDensity', 'temperature', 'lwc', 'stratigraphy'] # REMOVE ANY NOT RELEVANT TO YOUR SNOW PITS, NOTE 'gapFiledDensity' should remain to extrapolate raw density to the ground and gap fill other areas, this is used to compute SWE
    summary_files_list = ['SWE', 'environment'] # REMOVE ITEM IF YOU DON'T WANT SUMMARY FILE TO GENERATE
    
    # paths
    src_path = Path('.')
    des_path = Path('./outputs/pits')
    des_path.mkdir(parents=True, exist_ok=True)
    
    ## MODIFICATIONS TO CONSIDER:
        # 1. 'Local Standard Time' - is the Datetime header, but this script doesn't convert to local standard time.
        
    ## TROUBLESHOOTING ISSUES:
        # PitID - use 2-letter state, 2-letter lcoation, and 2-letter site (e.g. Idaho Boise River Basin Upper Bogus = IDBRUB; Colorado Cameron Pass Michigan River = COCPMR)
        # Having issues with 'UTM zone'? Enter a 2-digit integer only (e.g. 10, 06, not 12N). A 'N' for Northern Hemisphere will be appended. Modify if in southern hem.
        # Are your densities top/bottom not being read? Don't use an excel formula to populate. Start the 10cm interval pattern and then drag the right corner downwards to enter a real value (not formula). 
    
    # -------------------------------------------------------------------------
    # NO FURTHER CHANGES BELOW THIS LINE
    #--------------------------------------------------------------------------
      

    # empty dictionary to store summary filenames
    summary_files_dict = {}
    
        
    # Define headers for each file type
    headers = {
        'SWE': [
            'Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone', 'Easting (m)',
            'Northing (m)', 'Latitude (deg)', 'Longitude (deg)', 'Density A Mean (kg/m^3)',
            'Density B Mean (kg/m^3)', 'Density Mean (kg/m^3)', 'SWE A (mm)', 'SWE B (mm)',
            'SWE (mm)', 'Snow Depth (cm)', 'HS (cm)', 'Flag'
        ],
        'environment': [
            'Location', 'Site', 'PitID', 'Date/Local Standard Time', 'UTM Zone', 'Easting (m)',
            'Northing (m)', 'Latitude (deg)', 'Longitude (deg)', 'Precipitation', 'Sky',
            'Wind', 'Ground Condition', 'Ground Roughness', 'Ground Vegetation',
            'Height of Ground Vegetation (cm)', 'Canopy'
        ]
    }
    
    # write summary header files:  
    for item in summary_files_list: 
    
        # # Iterate through summary file types and create CSVs
        # for summary_item in summary_files_list:

         # Name the summary file
         summary_fpath = des_path.parent.joinpath(f"{campaign_prefix}_Summary_{item}.csv")
         
         # Store filename in the dictionary
         summary_files_dict[item] = summary_fpath
              
        # Create an empty DataFrame with the specified headers
         df = pd.DataFrame(columns=headers[item])
         
         # Write to CSV
         df.to_csv(summary_fpath, index=False, sep=',', header=True, encoding='utf-8-sig')
 
    swe_fpath = summary_files_dict.get('SWE')
    env_fpath = summary_files_dict.get('environment')
            


    for filename in sorted(src_path.rglob('./data/*.xlsx')):
        
        
        print('Current file: ', filename.name)
        
        # copy original pit sheet into outputs directory
        shutil.copy2(filename, des_path.joinpath(filename.name))
  
        # extract pit sheet metadata and store in dictionary
        metadata = get_metadata(filename)
        
       
        
        # empty dictionary to store parameter filenames
        parameter_files = {}

        # write parameter header files
        for parameter in parameter_list:
            
            # initiate parameter file names:
            file_path = des_path.joinpath(campaign_prefix + '_' + metadata['PitID'] + '_' + parameter + '.csv')
                
            # write parameter header file
            write_parameter_header(metadata, file_path)
            
            # store output parameter filenames in dictionary, access later to append data
            parameter_files[parameter] = file_path
            
        

                
        # Density & SWE
        if 'density' in parameter_list:
            density, AvgDensity = get_density(filename, metadata['HS (cm)'], parameter_files['density'])  
            print('I SHOULD BE SWE FILE:', str(swe_fpath))
            get_SWE(filename, density, metadata, swe_fpath, parameter_files['gapFilledDensity'])
        
        
        # Liquid Water Content
        if 'lwc' in parameter_list:
            get_lwc(filename, parameter_files['lwc'], AvgDensity)

        # Temperature
        if 'temperature' in parameter_list:
            get_temp(filename, parameter_files['temperature']) 
        
        # Straigraphy
        if 'stratigraphy' in parameter_list:
            get_stratigraphy(filename, parameter_files['stratigraphy'])
            
        # Site Details
        # env_result = get_siteDetails(xl, metadata)
                
            
            
            

        
            
