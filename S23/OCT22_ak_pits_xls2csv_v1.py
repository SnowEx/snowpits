#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script generates parameter files for the NASA SnowEx AK IOP October 2022
from the snow pit books.
"""

_author_ = "Megan A. Mason, NASA GSFC/SSAI"
_email_ = "megan.a.mason@nasa.gov"
_status_ = "Dvp"

# Import necessary modules
import datetime
import glob
import os
import shutil
import numpy as np
import pandas as pd
import csv
from csv import writer
import re
import textwrap
from pathlib import Path
import utm
from openpyxl import load_workbook

# local imports
from metadata_headers_summaryFile import metadata_headers_swe, metadata_headers_substrate, metadata_headers_enviro
import sys
sys.path.append('/Users/mamason6/Documents/snowex/core-datasets/ground/snow-pits/run-AKIOP23/October/code/process-pits') # October dir
from parsers import generate_new_filename

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# ------------------------------------------------------------------------------
# Functions

## extract snow pit data ##

# get_metadata: pulls all metadata from top of snow pit sheet and stores in dictionary
def get_metadata(xl_file):

    d = pd.read_excel(xl_file, sheet_name='FRONT')

    # metadata
    location = d['Unnamed: 0'][1]
    site = d['Unnamed: 0'][4].split(':')[0]
    plotID = d['Unnamed: 0'][6]
    plot_num = plotID[-3:]
    date = d['Unnamed: 6'][1]
    time = d['Unnamed: 6'][4]
    zone = d['Unnamed: 17'][6]
    easting = int(d['Unnamed: 8'][6])
    northing = int(d['Unnamed: 12'][6])
    
    print(site)
    if site == 'CPCW':
        site = 'CPCRW'

    pit_datetime=datetime.datetime.combine(date, time)
    pit_datetime_str=pit_datetime.strftime('%Y-%m-%dT%H:%M')

    # adjust SE corner coordinate to ~middle of 5x5 perimeter (ideal is -/+2.5 m but UTMs will be integers, so we'll restrict to 2 to be closest to the pit face)
    easting = easting -2
    northing = northing +2

    # convert to Lat/Lon:
    lat, lon = utm.to_latlon(easting, northing, zone, 'Northern')
    lat = round(lat, 5)
    lon = round(lon, 5)
    # lat = utm.to_latlon(easting, northing, zone, 'Northern')[0].round(5)
    # lon = utm.to_latlon(easting, northing, zone, 'Northern')[1].round(5)

    # other
    hs = d['Unnamed: 4'][6]
    thickness = d['Unnamed: 6'][6]
    observers = d['Unnamed: 8'][1]
    gps = d['Unnamed: 8'][4]
    WiseSerialNo = d['Unnamed: 6'][6]
    T_start_time = d['Unnamed: 15'][4]
    T_end_time = d['Unnamed: 17'][4]

    # vegCl = d['Unnamed: 19'][2]
    measSum = d['Unnamed: 19'][6] # could rename to co-located
    comments = d['Unnamed: 6'][27]

    # get density cutter type
    rIx = (d.iloc[:,0] == 'Density Cutter \nVolume (cc)\n(circle one or more)').idxmax() #locate 'Weather:' cell in spreadsheet (row Index)
    d = d.loc[rIx:,:].reset_index(drop=True)

    cutter = d['Unnamed: 3'][0]
    swe_tube = 'yes' # hard coded for fall campaigns, no plans to use, just get 'SWE tube' from cutter assignment

    # ~~~~Flags still need to be manually entered, but set up to catch
    if "Flag: " in str(comments):
        flags = comments.split('Flag: ')[1]
        flags = Flag.replace('\n', ' ')
    else:
        flags = None

    if site == 'CPC':
        site = 'CPCRW'

    metadata = {
        'Plot No.': plot_num,
        'Location': location,
        'Site': site,
        'PlotID': plotID, #change to pit?
        'Date': date,
        'Time': time,
        'Datetime_str': pit_datetime_str,
        'Zone': str(zone)+'N', #'N'=nortern hemisphere
        'Easting': easting,
        'Northing': northing,
        'Latitude': lat,
        'Longitude': lon,
        'HS (cm)': hs,
        'Thickness (cm)': thickness,
        'Observers': str(observers),
        'Cutter': cutter,
        'SWE Tube': swe_tube,
        'WiseSerialNo': WiseSerialNo,
        'GPS & Uncert.': gps,
        'T start Time': T_start_time,
        'T end Time': T_end_time,
        'Meas. Summary': measSum,
        'Flags': flags,
        'Pit Comments': comments
        }

    return metadata

#-------------------------------------------------------------------------------
# write_parameter_header: writes header rows to parameter files
def write_parameter_header(metadata, file_path):
    p_codes_strat = (
        "Grain Size: <1mm, 1-2mm, 2-4mm, 4-6mm, >6mm; "
        "Grain Type: SH=Surface Hoar, PP=Precipitation Particles, DF=Decomposing and Fragments, "
        "RG=Rounded Grains, FC=Faceted Crystals, MF=Melt Forms, IF=Ice Formations, "
        "MFcr=Melt Crust, FCsf=Near-surface Facets, PPgp=Graupel; "
        "Hand Hardness: F=Fist, 4F=4-finger, 1F=1-finger, P=Pencil, K=Knife, I=Ice; "
        "Manual Wetness: D=Dry, M=Moist, W=Wet, V=Very Wet, S=Soaked"
    )

    if '_stratigraphy_' in str(file_path):
        p_codes = p_codes_strat
    else:
        p_codes = "n/a for this parameter"

    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["# Location", metadata['Location']])
        writer.writerow(["# Site", metadata['Site']])
        writer.writerow(["# PitID", metadata['PlotID']])
        writer.writerow(["# Date/Local Standard Time", metadata['Datetime_str']])
        writer.writerow(["# UTM Zone", metadata['Zone']])
        writer.writerow(["# Easting", metadata['Easting']])
        writer.writerow(["# Northing", metadata['Northing']])
        writer.writerow(["# Latitude", metadata['Latitude']])
        writer.writerow(["# Longitude", metadata['Longitude']])
        writer.writerow(["# Flags", metadata['Flags']])
        writer.writerow(["# Pit Comments", metadata["Pit Comments"]])
        writer.writerow(["# Parameter Codes", p_codes])
        

#-------------------------------------------------------------------------------
# write_header_rows(): writes rows for summary SWE, substarate, and Envir. file
def write_header_rows(fname_summaryFile, metadata_headers):
        with open(fname_summaryFile, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in metadata_headers:
                writer.writerow(row)

#-------------------------------------------------------------------------------
# XX. get_temperature: grabs snow profile temperature (oC) and depth ((cm)) from pit sheet
def get_temperature(filename, fname_temperature):
    d = pd.read_excel(filename, sheet_name='FRONT', header=10, usecols='I:J')#.replace(r'^\s*$', np.nan, regex=True)
    first_double_nan = min(np.where(pd.Series(d['(cm)'].isnull().values).rolling(2).sum().values==2))[0]
    temperature = d.iloc[0:first_double_nan].dropna(how='all')
    # print('TEMPERATURE DATAFRAME\n', temperature)

    d = pd.read_excel(xl, sheet_name='FRONT', header=4, usecols='P:R')#.replace(r'^\s*$', np.nan, regex=True)
    last_row_value = temperature.shape[0]-1 #temperature.shape[-1] #-1 removed 6/29, 0
    temperature['Time start/end'] = None
    temperature.at[0, 'Time start/end'] = d['START'][0] if not pd.isnull(d['START'][0]) else -9999
    temperature.at[last_row_value, 'Time start/end'] = d['END'][0] if not pd.isnull(d['END'][0]) else -9999

    temperature.columns = ['# Depth (cm)', 'Temperature (deg C)', 'Time start/end']

    df = temperature # just for ease of coding with 'df'

    profile_mask = df['# Depth (cm)'].astype(str).str.contains('Profile') # look for 'Profile' in the dataframe
    profile_indices = df.index[profile_mask].tolist()


    if not profile_indices:
        temperature.to_csv(fname_temperature, sep=',', index=False, mode='a', na_rep=-9999) # no second profile (90% of cases), write the _temperature_ parameter file
        wrote_any = False
        
    else:
        # Dual profile: split and append to temperatureA, temperatureC
        profile_indices.append(len(df))
        for i in range(len(profile_indices) - 1):
            start_idx = profile_indices[i]
            end_idx = profile_indices[i + 1]
            profile_str = df.loc[start_idx, '# Depth (cm)']
            profile_letter = profile_str.split()[-1]
            new_df = df.iloc[start_idx + 1:end_idx].reset_index(drop=True) # create the new df
            new_df = new_df[~new_df['# Depth (cm)'].apply(lambda x: isinstance(x, str))] # remove 'profile' str row from df

            new_filename = fname_temperature.with_name(fname_temperature.name.replace('temperature', f'temperature{profile_letter}'))  # rename the file based on the df name: A, B, C

            # if new_filename.exists():
            shutil.copy2(fname_temperature, new_filename) # copy the temperature file w/ metadata

            # Append to the existing temperature file
            new_df.to_csv(new_filename, sep=',', index=False, mode='a', na_rep=-9999)  # append the df to the existing file

            wrote_any = True
            
        if wrote_any:
            if fname_temperature.exists():
                fname_temperature.unlink()
            

    # print('wrote: .../' + fname_temperature.name)

    #return temperature

#-------------------------------------------------------------------------------
def get_stratigraphy(filename, fname_stratigraphy):

    d = pd.read_excel(filename, sheet_name='FRONT', header=10, usecols='L:AA')
    first_quad_nan = min(np.where(pd.Series(d['top\n(cm).1'].isnull().values).rolling(4).sum().values == 4))[0] # because .xlsx cells are merged & 2nd profile skips a line
    d = d.iloc[0:first_quad_nan]
    d.columns = ['# Top (cm)', '-.1', 'Bottom (cm)', 'Grain Size (mm)', '1-2 mm', '2-4 mm',
       '4-6 mm', '> 6 mm', 'Grain Type', 'Hand Hardness', 'Manual Wetness', 'Comments'] #rename them here, all of them

    strat_cols = ['# Top (cm)', 'Bottom (cm)', 'Grain Size (mm)', 'Grain Type',
                    'Hand Hardness','Manual Wetness', 'Comments'] #select which onces for the CSV file
    if d['# Top (cm)'].astype(str).str.contains('Comments/Notes:').any():
        notes_loc = np.where(d['# Top (cm)'].str.contains('Comments/Notes:').values==1)
        d = d.loc[d.index[0:notes_loc[0][0]]]
    stratigraphy = d[strat_cols].dropna(how='all')
    
    last_col = stratigraphy.columns[-1]
    
    profiles_found = (
        stratigraphy[last_col].astype(str).str.contains("Profile A|Profile B|Profile C").any()
    )
    
    if not profiles_found:
        # No profile markers at all → just write stratigraphy
        stratigraphy.to_csv(fname_stratigraphy, sep=",", index=False, mode="a",
                            na_rep=-9999, encoding="utf-8")
    
    else:
        # Split into A, B, C
        stratigraphyA = stratigraphy[stratigraphy[last_col].astype(str).str.contains("Profile A")]
        stratigraphyB = stratigraphy[stratigraphy[last_col].astype(str).str.contains("Profile B")]
        stratigraphyC = stratigraphy[stratigraphy[last_col].astype(str).str.contains("Profile C")]
    
        wrote_any = False
    
        for suffix, df in zip(["A", "B", "C"], [stratigraphyA, stratigraphyB, stratigraphyC]):
            if not df.empty:
                fname_split = fname_stratigraphy.with_name(
                    fname_stratigraphy.name.replace("stratigraphy", f"stratigraphy{suffix}")
                )
                shutil.copy(fname_stratigraphy, fname_split)  # copy metadata
                df.to_csv(fname_split, sep=",", index=False, mode="a",
                          na_rep=-9999, encoding="utf-8")
                wrote_any = True
                
        if wrote_any:
            if fname_stratigraphy.exists():
                fname_stratigraphy.unlink()
    

#-------------------------------------------------------------------------------
def get_siteDetails(filename, fname_siteDetails, metadata):
    # this function also uses the metadata dictionary (location, site, plotID, etc.)


        d = pd.read_excel(filename, sheet_name='BACK')


        # snow cover condition
        snowCov = d['Unnamed: 3'][9].split(' ')[0] # None, Patchy, Continuous

        # weather
        weather = d['Unnamed: 18'][1]
        precip_type = d['Unnamed: 20'][4]
        precip_rate = d['Unnamed: 20'][6]
        sky = d['Unnamed: 20'][8]
        wind = d['Unnamed: 20'][10]

        # ground cover/vegetation
        d = pd.read_excel(filename, sheet_name='BACK', usecols='A:P')
        rIx = (d.iloc[:,0] == 'Vegetation').idxmax() #locate 'Vegetation:' cell in spreadsheet (row Index)
        d = d.loc[rIx:,:].reset_index(drop=True)
        grdCov = d['Unnamed: 3'][1]

        if isinstance(grdCov, float) and np.isnan(grdCov): # if the section is empty, it's a float nan. need both otherwise str wont run the isnan() arg.
            GroundVeg = np.nan
            VegHts = np.nan
            VegPrs = np.nan
        else:
            grdCov = grdCov.split(',')
            grdCov = [item.strip() for item in grdCov] # some (all?) have leading spaces
            VegType = d.iloc[3, [3,5,7,9,11,13,15]] # list of ALL cover types, not what was selected
            VegHts = d.iloc[5, [3,5,7,9,11,13,15]]
            VegPct = d.iloc[7, [3,5,7,9,11,13,15]]*100
            Veg = pd.DataFrame({'VegType': VegType.values, 'VegHts': VegHts.values, 'VegPct': VegPct.values})
            Veg['VegBool'] = Veg['VegType'].apply(lambda x: x in grdCov)
            Veg['VegBool'] = Veg['VegBool'].astype(bool) # assign as bool type
            Veg['VegHts'] = Veg['VegHts'].where((~Veg['VegBool']) | (Veg.index != 0), 0) # if 'Bare' is true, assign veg height of 0 cm
            Veg['VegHts'] = Veg['VegHts'].where(~(Veg['VegBool'] & Veg['VegHts'].isna()), -9999) # if veg type=TRUE and veg htn=NaN, assign -9999.

            # make outputs, into lists
            GroundVeg=Veg.where(Veg.VegBool).dropna().VegType.values.tolist() # list of ground veg (e.g. ['Bare', 'Shrub'])
            VegHts=Veg.where(Veg.VegBool).dropna().VegHts.values.tolist() # list of veg heights (e.g. [0, 15])
            VegPrs=Veg.where(Veg.VegBool).dropna().VegPct.values.tolist() # list of veg percents (e.g. [5, 95])

            GroundVeg = " | ".join(GroundVeg)
            VegHts = " | ".join(str(x) for x in VegHts)
            VegPrs = " | ".join(str(x) for x in VegPrs)

        # tussocks
        tussock_present = d['Unnamed: 3'][10]
        tuss_vert_ht = d['Unnamed: 9'][10] # if its <1, *100 (NSIDC entered as % sometimes....)
        tuss_horz_sp = d['Unnamed: 13'][10]
        if tuss_vert_ht is not np.nan or tuss_horz_sp is not np.nan:
            tussock_dims = "{} | {}".format(tuss_vert_ht, tuss_horz_sp)
        else:
            tussock_dims = np.nan


        # tree/forest characteristics
        forest_type = d['Unnamed: 3'][12]
        deciduous_pct = d['Unnamed: 6'][14]
        evergreen_pct = d['Unnamed: 14'][14]
        tree_canopy = d['Unnamed: 3'][16]
        avg_tree_ht = d['Unnamed: 3'][18]
        if deciduous_pct is not np.nan and evergreen_pct is not np.nan:
            forest_pct = "{} | {}".format(deciduous_pct, evergreen_pct)
        else:
            forest_pct = np.nan


        # fill in empties

        # % forest type
        if forest_type == 'Evergreen':
            deciduous_pct = 0
            evergreen_pct = 100

        if forest_type == 'Deciduous':
            deciduous_pct = 100
            evergreen_pct = 0

        if forest_type == 'None':
            deciduous_pct = 0
            evergreen_pct = 0

        # tree canopy
        if tree_canopy == 'No trees':
            avg_tree_ht = '0m'

        # vegetation comments section
        veg_forest_cmts = str(d['Unnamed: 0'][21])


        # ground conditions
        d = pd.read_excel(filename, sheet_name='BACK', usecols='S:AA')
        rIx = (d.iloc[:,0] == 'Substrate').idxmax() #locate 'Substrate', but 'plot photos' to get surf. roughness
        d = d.loc[rIx:,:].reset_index(drop=True)
        grd_condition = d['Unnamed: 21'][1]
        grd_roughness = d['Unnamed: 21'][3]
        water = d['Unnamed: 21'][5]
        if water is np.nan:
            water = 'N/A'
        soil_substrate_cmts = str(d['Unnamed: 18'][12]).capitalize()
        if soil_substrate_cmts == 'Nan':
            soil_substrate_cmts = ''


        d = pd.read_excel(filename, sheet_name='FRONT', usecols='T:AA')
        rIx = (d.iloc[:,0] == 'Assigned plot comments').idxmax() #locate 'Substrate', but 'plot photos' to get surf. roughness
        d = d.loc[rIx:,:].reset_index(drop=True)

        AssignedPlotComments = str(d['Unnamed: 19'][2]) # check this

      # create complete header
        index = ['# Location', '# Site', '# PitID', '# Date/Local Standard Time', '# UTM Zone', '# Easting (m)',
           '# Northing (m)', '# Latitude (deg)', '# Longitude (deg)',
            '# HS (cm)', '# Observers', '# WISe Serial No', '# GPS & Uncert.', '# Density Cutter/Instrument', '# Snow Cover Condition',
            '# Weather', '# Precip Type', '# Precip Rate', '# Sky', '# Wind',
            '# Ground Condition', '# Ground Roughness', '# Standing Water Present',
            '# Ground Vegetation/Cover', '# Vegetation Height (cm)', '# Percent Ground Cover (%)', '# Tussocks Present', '# Tussock Vert & Spacing (cm)',
            '# Forest Type', '# Percent Mixed Forest (%) (Deciduous|Evergreen)', '# Tree Canopy', '# Tree Height (m)', '# Vegetation/Forest Comments', '# Soil/Substrate Comments', '# Assigned Plot Cmts']#, '# Flags']
        column = ['value']
        df = pd.DataFrame(index=index, columns=column)
        #
        df['value'][0] = metadata.get('Location')
        df['value'][1] = metadata.get('Site')
        df['value'][2] = metadata.get('PlotID')
        df['value'][3] = metadata.get('Datetime_str')
        df['value'][4] = metadata.get('Zone')
        df['value'][5] = metadata.get('Easting')
        df['value'][6] = metadata.get('Northing')
        df['value'][7] = metadata.get('Latitude')
        df['value'][8] = metadata.get('Longitude')
        df['value'][9] = metadata.get('HS (cm)')
        df['value'][10] = metadata.get('Observers').replace('\n', ' ')
        df['value'][11] = metadata.get('WiseSerialNo')
        df['value'][12] = metadata.get('GPS & Uncert.')
        df['value'][13] = metadata.get('Cutter')
        df['value'][14] = snowCov
        df['value'][15] = weather
        df['value'][16] = precip_type
        df['value'][17] = precip_rate
        df['value'][18] = sky
        df['value'][19] = wind
        df['value'][20] = grd_condition
        df['value'][21] = grd_roughness
        df['value'][22] = water
        df['value'][23] = GroundVeg
        df['value'][24] = VegHts
        df['value'][25] = VegPrs
        df['value'][26] = tussock_present
        df['value'][27] = tussock_dims
        df['value'][28] = forest_type
        df['value'][29] = forest_pct
        df['value'][30] = tree_canopy
        df['value'][31] = avg_tree_ht
        df['value'][32] = veg_forest_cmts.replace('\n', ' ')#.replace('nan', '') #str(PitComments.split('Flag:')[0].replace('\n', ' ')) # removes the list of Flags if any.
        df['value'][33] = soil_substrate_cmts.replace('\n', ' ')
        df['value'][34] = AssignedPlotComments#.replace('\n', ' ').replace('nan', '')
        # df['value'][35] = Flags.replace('\n', ' ')

        df.replace('nan', np.nan, inplace=True)
        
        # note -- this parameter file doesn't 'append' just writes new
        df.to_csv(fname_siteDetails, sep=',', header=False, na_rep=-9999, encoding='utf-8-sig')
        # print('wrote: .../' + fname_siteDetails.name)


        env_data = {
            'Snow Cover Condition': snowCov,
            'Precipitation Type': precip_type,
            'Precipitation Rate': precip_rate,
            'Sky': sky,
            'Wind': wind,
            'Ground Condition': grd_condition,
            'Ground Roughness': grd_roughness,
            'Standing Water Present': water,
            'Ground Vegetation': GroundVeg,
            'Height of Ground Vegetation': VegHts,
            'Percent of Ground Cover': VegPrs,
            'Tussock(s)': tussock_present,
            'Tussock Dimensions': tussock_dims,
            'Forest Type': forest_type,
            'Percent Mixed Forest': forest_pct,
            'Canopy': tree_canopy,
            'Canopy Height': avg_tree_ht
        }

        return env_data

# ------------------------------------------------------------------------------
# parse depth range for soil/substrate values
def parse_depth_range(depth_str):
    """
    Parse a depth range string like '0 to -7' or '-15 to -23+' into top and bottom values.
    Returns (top, bottom, greater_than_flag)
    """
    if pd.isna(depth_str) or depth_str is None:
        return None, None, False
    
    try:
        # Convert to string and split
        depth_parts = str(depth_str).split(' to ')
        if len(depth_parts) != 2:
            return None, None, False
        
        top = depth_parts[0].strip()
        bottom = depth_parts[1].strip()
        
        # Check for '+' sign indicating "greater than"
        greater_than = bottom.endswith('+')
        if greater_than:
            bottom = bottom[:-1]  # Remove the '+' sign
            
        return top, bottom, greater_than
        
    except (AttributeError, IndexError):
        return None, None, False
    
# ------------------------------------------------------------------------------
def safe_get_cell(dataframe, column, row, default=np.nan):
    """
    Safely get a cell value, returning default if index is out of bounds or value is missing.
    """
    try:
        return dataframe[column][row]
    except (IndexError, KeyError):
        return default
    
# ------------------------------------------------------------------------------
def get_substrate(filename):
    """
    Extract soil data from Excel file and parse depth ranges.
    """
    # Read the Excel data
    d = pd.read_excel(filename, sheet_name='BACK', usecols='S:AA')
    rIx = (d.iloc[:,0] == 'Substrate').idxmax()  # locate 'Substrate:' cell
    d = d.loc[rIx:,:].reset_index(drop=True)  
    
    # Extract basic soil information
    soil_substrate_cmts = str(safe_get_cell(d, 'Unnamed: 18', 12, '')).capitalize()
    if soil_substrate_cmts == 'Nan':
        soil_substrate_cmts = ''
    
    SM_sample = safe_get_cell(d, 'Unnamed: 25', 9)
    num_SM_sample = safe_get_cell(d, 'Unnamed: 25', 11)
    depth_sample_at = safe_get_cell(d, 'Unnamed: 25', 13)
    depth_veg = safe_get_cell(d, 'Unnamed: 20', 23)
    depth_org = safe_get_cell(d, 'Unnamed: 20', 24)
    depth_frozen = safe_get_cell(d, 'Unnamed: 20', 25)
    depth_mineral = safe_get_cell(d, 'Unnamed: 20', 26, np.nan)  
    
    
    # Parse all depth ranges using the helper function
    sample_depth_top, sample_depth_bot, sample_gt = parse_depth_range(depth_sample_at)
    depth_veg_top, depth_veg_bot, veg_gt = parse_depth_range(depth_veg)
    depth_org_top, depth_org_bot, org_gt = parse_depth_range(depth_org)
    depth_frozen_top, depth_frozen_bot, frozen_gt = parse_depth_range(depth_frozen)
    depth_mineral_top, depth_mineral_bot, mineral_gt = parse_depth_range(depth_mineral)
    
    # Determine which layer has the "greater than" flag (assuming only one can have it)
    greater_than = None
    for gt_flag, layer_name in [(sample_gt, 'Sample'), (veg_gt, 'Vegetation'), 
                               (org_gt, 'Organic'), (frozen_gt, 'Frozen'), 
                               (mineral_gt, 'Mineral')]:
        if gt_flag:
            greater_than = 'GT'
            break
    
    # Return structured data
    substrate_data = {
        'SM Sample': SM_sample,
        'Num SM Sample': num_SM_sample,
        'Soil Substrate Comments': soil_substrate_cmts,
        'Sample Top': sample_depth_top,
        'Sample Bottom': sample_depth_bot,
        'Frozen Top': depth_frozen_top,
        'Frozen Bottom': depth_frozen_bot,
        'Veg Top': depth_veg_top,
        'Veg Bottom': depth_veg_bot,
        'Organic Top': depth_org_top,
        'Organic Bottom': depth_org_bot,
        'Mineral Top': depth_mineral_top,
        'Mineral Bottom': depth_mineral_bot, 
        'QC GreaterThan flag': greater_than
    }
    
    return substrate_data

# ------------------------------------------------------------------------------

def get_swe(filename, fname_sweTube):
    
    # data sheet labels SWE as 'cm', but field measurements were certainly 'mm'

    d = pd.read_excel(filename, usecols='D:F', sheet_name='FRONT')

    # get swe tube data section
    rIx = (d.iloc[:,0] == 'SWE Tube').idxmax() #locate 'Weather:' cell in spreadsheet (row Index)
    d = d.iloc[rIx:,:].reset_index(drop=True)
    d.columns = d.iloc[0] # set row 0 as new header
    d.rename(columns={'SWE \n(mm)': 'SWE (mm)', 'SWE Tube': "# SWE Sample"}, inplace=True)
    d.columns.name = None # rms weird extra '0' added to index row
    swe = d.drop([0,1,2]).reset_index(drop=True) # drop the first three rows (old header + NaN rows) 
    swe['Depth (cm)'] = pd.to_numeric(swe['Depth (cm)'], errors='coerce')
    swe['SWE (mm)'] = pd.to_numeric(swe['SWE (mm)'], errors='coerce')
    
    # compute density from SWE tube samples
    swe['Density (kg/m^3)'] = (swe['SWE (mm)'] / swe['Depth (cm)'] * 100).round(0).astype('Int64')

    swe.to_csv(fname_sweTube, sep=',', index=False, mode="a", na_rep=-9999, header=True, encoding='utf-8-sig')
    # print('wrote: .../' + fname_sweTube.name)

# compute average to report in Summary SWE file

    avgHS = round(swe['Depth (cm)'].mean()/0.5) *0.5 # rounded to nearest 0.5 cm
    avgSWE= round(swe['SWE (mm)'].mean())
    avgDen= round(swe['Density (kg/m^3)'].mean())
    

    swe_data = {
        'HS_tube': avgHS, # avg. SWE tube heights
        'SWE_tube': avgSWE,
        'Density_tube': avgDen
    }

    return swe_data



# ------------------------------------------------------------------------------
# run main
if __name__ == "__main__":

    # static variables
    campaign_prefix = 'SnowEx23_SnowPits_AKIOP_'
    version = 'v01'

    # paths
    src_path = Path('/Users/mamason6/Documents/snowex/campaigns/AKIOP-23/october/data management/nsidc-downloads/01_pit-sheets')
    des_basepath = Path('/Users/mamason6/Documents/snowex/core-datasets/ground/snow-pits/run-AKIOP23/October/outputs')

    summary_swe_df = []
    summary_sub_df = []
    summary_env_df = []


    # copy raw .xlsx and place a copy in the submission package file structure
    for i, filename in enumerate(sorted(src_path.rglob('*.xlsm'))): #
        print('~~~~~~~', filename.name)

        # get date from fname in datetime
        fdate = pd.to_datetime(filename.stem.split('_')[1], format='%Y%m%d') # stem gets rid of extention

        # reorder filename to match pitID codes (modifed by M.Mason during cleaning process, easier to sort by number!)
        first_part, second_part = filename.stem.split('_')
        new_first_part = f"{first_part[3:]}{first_part[:3]}" #494WA -->WA494, or 006CB -->CB006
        filename_stem = f"{new_first_part}_{second_part}"
        # print('~~~~~~', filename_stem)

        # initialize new directories for parameter files and copy .xlsm into dir.
        new_partial_file_path, flight_line = generate_new_filename(filename) # returns path and string
        pitPath = des_basepath.joinpath('xls2csv/pits/' + new_partial_file_path)
        if not Path.exists(pitPath):
            Path(pitPath).mkdir(parents=True, exist_ok=True)

        # new_filename = Path(campaign_prefix + filename_stem + '_pitSheet_' + version + filename.suffix)
        new_filename = Path(campaign_prefix + filename_stem + '_pitSheet_' + version + '.xlsx') # this seems to work... convert .xlsm --> .xlsx
        shutil.copy(filename, pitPath.joinpath(new_filename))

        ''' Turn back on, speeds up run'''
        # convert to standard time (-1hr if <= Nov 6, 2022) and resave file - this applies to ALL pits from Oct 2022
        if fdate <=pd.Timestamp('2022-11-06'): # could remove the 'if' since applies to all
            xl = pitPath.joinpath(new_filename) # full filename for the copied xlsx
            wb = load_workbook(xl)
            ws_front = wb['FRONT']
            ws_back = wb['BACK']

            # open excel pit sheet
            xl = pitPath.joinpath(new_filename) # full filename for the copied xlsx

            # get pit time 'FRONT', cell G6
            pit_time = ws_front['G6'].value
            ws_front['G6'].value = pit_time.replace(hour=(pit_time.hour - 1) % 24)

            # get pit time 'BACK', cell F9
            pit_time = ws_back['F9'].value
            ws_back['F9'].value = pit_time.replace(hour=(pit_time.hour - 1) % 24)

            # # get Temp start time (if not empty), cell P6
            temp_start_time = ws_front['P6'].value
            if temp_start_time is not None:
                ws_front['P6'].value = temp_start_time.replace(hour=(temp_start_time.hour - 1) % 24)

            # get Temp end time (if not empty), cell R6
            temp_end_time = ws_front['R6'].value
            if temp_end_time is not None:
                ws_front['R6'].value = temp_end_time.replace(hour=(temp_end_time.hour - 1) % 24)

            # resave (or overwrite) "new_filename" .xlsx file
            wb.save(xl)

        # open excel pit sheet
        xl = pitPath.joinpath(new_filename)

        # create a dictionary of metadata from the pit sheet header
        metadata = get_metadata(xl)

        # initiate parameter file names
        fname_sweTube         = pitPath.joinpath(campaign_prefix + filename_stem + '_sweTube_' + version +'.csv') # computed from SWE Tube
        fname_temperature     = pitPath.joinpath(campaign_prefix + filename_stem + '_temperature_' + version +'.csv')
        fname_stratigraphy    = pitPath.joinpath(campaign_prefix + filename_stem + '_stratigraphy_' + version +'.csv')
        fname_siteDetails     = pitPath.joinpath(campaign_prefix + filename_stem + '_siteDetails_' + version +'.csv')

        # write parameter file metadata header rows
        write_parameter_header(metadata, fname_sweTube)
        write_parameter_header(metadata, fname_temperature)
        write_parameter_header(metadata, fname_stratigraphy)
        write_parameter_header(metadata, fname_siteDetails)



        # ---------------- Parameter Files ----------------

        # SWE
        swe_result = get_swe(xl, fname_sweTube)

        # Temperature
        get_temperature(xl, fname_temperature)

        # Stratigraphy
        get_stratigraphy(xl, fname_stratigraphy)
        
        sub_result = get_substrate(filename)

        # SiteDetails
        env_result = get_siteDetails(xl, fname_siteDetails, metadata)

        summary_swe_df.append({
            'Plot No.': metadata.get('Plot No.'),
            'Location': metadata.get('Location'),
            'Site': metadata.get('Site'),
            'PlotID': metadata.get('PlotID'),
            'Date/Local Standard Time': metadata.get('Datetime_str'),
            'Zone': metadata.get('Zone'),
            'Easting': metadata.get('Easting'),
            'Northing': metadata.get('Northing'),
            'Latitude': metadata.get('Latitude'),
            'Longitude': metadata.get('Longitude'),
            'HS-snowpit': metadata.get('HS (cm)'),
            'HS-sweTube': swe_result.get('HS_tube'),
            'SWE': swe_result.get('SWE_tube'),
            'Density': swe_result.get('Density_tube'),
            'Snow Thickness': metadata.get('Thickness (cm)'),
            'Snow Void': metadata.get('HS (cm)') - metadata.get('Thickness (cm)')}) # HS-thickness; would grab from strat but instances of multiple snow voids/profile or profile A/B each having a snow void that you don't want to sum. 
        

        summary_sub_df.append({
            'Plot No.': metadata.get('Plot No.'),
            'Location': metadata.get('Location'),
            'Site': metadata.get('Site'),
            'PlotID': metadata.get('PlotID'),
            'Date/Local Standard Time': metadata.get('Datetime_str'),
            'Zone': metadata.get('Zone'),
            'Easting': metadata.get('Easting'),
            'Northing': metadata.get('Northing'),
            'Latitude': metadata.get('Latitude'),
            'Longitude': metadata.get('Longitude'),
            'Snow Cover Condition': env_result.get('Snow Cover Condition'),
            'Ground Condition': env_result.get('Ground Condition'),
            'Ground Roughness': env_result.get('Ground Roughness'),
            'Standing Water Present': env_result.get('Standing Water Present'),
            'SM sample': sub_result.get('SM Sample'),
            'Sample Top': sub_result.get('Sample Top' ),    
            'Sample Bottom': sub_result.get('Sample Bottom' ),   
            'Frozen Top': sub_result.get('Frozen Top'),    
            'Frozen Bottom': sub_result.get('Frozen Bottom'),     
            'Veg Top': sub_result.get('Veg Top' ),    
            'Veg Bottom': sub_result.get('Veg Bottom'),    
            'Organic Top': sub_result.get('Organic Top'),     
            'Organic Bottom': sub_result.get('Organic Bottom'),     
            'Mineral Top': sub_result.get('Mineral Top'),     
            'Mineral Bottom': sub_result.get('Mineral Bottom'),     
            'QC GreaterThan flag': sub_result.get('QC GreaterThan flag')})
            
                    
                    
        summary_env_df.append({
            'Plot No.': metadata.get('Plot No.'),
            'Location': metadata.get('Location'),
            'Site': metadata.get('Site'),
            'PlotID': metadata.get('PlotID'),
            'Date/Local Standard Time': metadata.get('Datetime_str'),
            'Zone': metadata.get('Zone'),
            'Easting': metadata.get('Easting'),
            'Northing': metadata.get('Northing'),
            'Latitude': metadata.get('Latitude'),
            'Longitude': metadata.get('Longitude'),
            'Snow Cover Condition': env_result.get('Snow Cover Condition'),
            'Precipitation Type': env_result.get('Precipitation Type'),
            'Precipitation Rate': env_result.get('Precipitation Rate'),
            'Sky': env_result.get('Sky'),
            'Wind': env_result.get('Wind'),
            'Ground Condition': env_result.get('Ground Condition'),
            'Ground Roughness': env_result.get('Ground Roughness'),
            'Standing Water Present': env_result.get('Standing Water Present'),
            'Ground Vegetation': env_result.get('Ground Vegetation'),
            'Height of Ground Vegetation': env_result.get('Height of Ground Vegetation'),
            'Percent of Ground Cover': env_result.get('Percent of Ground Cover'),
            'Tussock(s)': env_result.get('Tussock(s)'),
            'Tussock Dimensions': env_result.get('Tussock Dimensions'),
            'Forest Type': env_result.get('Forest Type'),
            'Percent Mixed Forest': env_result.get('Percent Mixed Forest'),
            'Canopy': env_result.get('Canopy'),
            'Canopy Height': env_result.get('Canopy Height')})
        



    # initiate summary files
    fname_summarySWE = des_basepath.joinpath('xls2csv/'+ campaign_prefix + 'Summary_SWE_' + version + '.csv')
    fname_summarySub = des_basepath.joinpath('xls2csv/'+ campaign_prefix + 'Summary_Substrate_' + version + '.csv')
    fname_summaryEnviro = des_basepath.joinpath('xls2csv/'+ campaign_prefix + 'Summary_Environment_' + version + '.csv')


    # write summary file data to file
    r = write_header_rows(fname_summarySWE, metadata_headers_swe)
    r = write_header_rows(fname_summarySub, metadata_headers_substrate)
    r = write_header_rows(fname_summaryEnviro, metadata_headers_enviro)

      # fill in summary files
    df_SWE = pd.DataFrame(summary_swe_df)
    df_SWE.sort_values(by=['Plot No.'], inplace=True)
    df_SWE.drop('Plot No.', axis=1, inplace=True)
    df_SWE.to_csv(fname_summarySWE, mode='a', na_rep=-9999, header=False, index=False)

    df_sub = pd.DataFrame(summary_sub_df)
    df_sub.sort_values(by=['Plot No.'], inplace=True)
    df_sub.drop('Plot No.', axis=1, inplace=True)
    df_sub.to_csv(fname_summarySub, mode='a', na_rep=-9999, header=False, index=False) 
    
    df_env = pd.DataFrame(summary_env_df)
    df_env.sort_values(by=['Plot No.'], inplace=True)
    df_env.drop('Plot No.', axis=1, inplace=True)
    df_env.to_csv(fname_summaryEnviro, mode='a', na_rep=-9999, header=False, index=False)
