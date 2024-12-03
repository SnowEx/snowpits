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
"""
__author__ = "HP Marshall, CryoGARS, Boise State University"
__version__ = "06"
__maintainer__ = "HP Marshall"
__email__ = "hpmarshall@boisestate.edu"
__status__ = "Dvp"
__date__ = "04.2020"

import datetime
import glob
import os
import shutil
import numpy as np
import pandas as pd
from pyproj import Proj, transform
from csv import writer
import textwrap
from win32com import client
import win32api


def readSnowpit(path_in, filename, version, path_out, fname_swe, fname_enviro):
    # generate filenames for all the output csvs
    fname = os.path.basename(filename)  # get just the filename
    pitID = fname[4:8].rstrip("_")
    dateString = fname[-13:-5]
    subString = fname.rstrip(".xlsx")
    pitPath = path_out + subString + '/'
    os.mkdir(pitPath)

    for filenames in glob.glob(os.path.join(path_in, subString + '*')):
        if(filenames[-3:] == 'jpg'):
            res = os.path.basename(filenames.replace(subString, 'SnowEx20_SnowPits_GMIOP'))
            newfilename = res.replace(".jpg", '_' + dateString + '_' + pitID + '_' + version +'.jpg')
            shutil.copyfile(filenames, pitPath + newfilename)
        elif(filenames[-3:] == 'lsx'):
            res = os.path.basename(filenames.replace(subString, 'SnowEx20_SnowPits_GMIOP'))
            newfilename = res.replace(".xlsx", '_' + dateString + '_' + pitID + '_' + version +'.xlsx')
            shutil.copyfile(filenames, pitPath + newfilename)
            # save file as pdf
            pdf_file = pitPath + newfilename.replace(".xlsx",".pdf")
            app = client.DispatchEx("Excel.Application")
            app.Interactive = False
            app.Visible = False
            try:
                Workbook = app.Workbooks.Open(os.path.realpath(pitPath) + newfilename)
                Workbook.SaveAs(os.path.realpath(pdf_file), FileFormat=57)
            except Exception as e:
                print("Failed to convert in PDF format.Please confirm environment meets all the requirements  and try again")
                print(str(e))
            finally:
                Workbook.Close()
                app.Quit()

        else:
            shutil.copy(filenames, pitPath)

    # open excel file
    xl = pd.ExcelFile(filename)

    # create individual output file names
    fname_density = pitPath + 'SnowEx20_SnowPits_GMIOP_density_' + dateString + '_' + pitID + '_' + version +'.csv'
    fname_LWC = pitPath + '/SnowEx20_SnowPits_GMIOP_LWC_' + dateString + '_' + pitID + '_' + version +'.csv'
    fname_temperature = pitPath + '/SnowEx20_SnowPits_GMIOP_temperature_' + dateString + '_' + pitID + '_' + version +'.csv'
    fname_stratigraphy = pitPath + '/SnowEx20_SnowPits_GMIOP_stratigraphy_' + dateString + '_' + pitID + '_' + version +'.csv'
    fname_siteDetails = pitPath + '/SnowEx20_SnowPits_GMIOP_siteDetails_' + dateString + '_' + pitID + '_' + version +'.csv'

    # location / pit name
    d = pd.read_excel(xl, sheet_name=0, usecols='B')
    Location = d['Location:'][0]
    Site = d['Location:'][2]  # grab site name
    PitID = d['Location:'][4]  # grab pit name
    d = pd.read_excel(xl, sheet_name=0, usecols='L')
    UTME = d['Surveyors:'][2]
    d = pd.read_excel(xl, sheet_name=0, usecols='Q')
    UTMN = d['Unnamed: 16'][2]
    d = pd.read_excel(xl, sheet_name=0, usecols='X')
    UTMzone = d['Unnamed: 23'][2]

    # Check Northing must be > Easting
    try:
        if int(UTMN) < int(UTME):
            UTMN, UTME = UTME, UTMN
            print('UTM switched - ONLY VALID IF THIS IS GRAND MESA! -, values are now:\n', UTME, UTMN, UTMzone)
    except ValueError:
        UTME = np.nan
        UTMN = np.nan

    # total depth
    d = pd.read_excel(xl, sheet_name=0, usecols='G')
    TotalDepth = d['Unnamed: 6'][4]  # grab total depth

    # Wise serial number
    d = pd.read_excel(xl, sheet_name=0, usecols='I')
    WiseSerialNo = d['Unnamed: 8'][4]  # grab Wise serial number

    # date and time
    d = pd.read_excel(xl, sheet_name=0, usecols='X')
    pit_time = d['Unnamed: 23'][4]
    d = pd.read_excel(xl, sheet_name=0, usecols='S')
    pit_date = d['Unnamed: 18'][4]

    # combine date and time into one datetime variable, and format
    pit_datetime=datetime.datetime.combine(pit_date,pit_time)
    pit_datetime_str=pit_datetime.strftime('%Y-%m-%dT%H:%M')
    s=str(Site) + ',' + str(UTMzone) + ',' +  pit_datetime_str + ',' + str(UTME) + ',' + str(UTMN) + ',' + str(TotalDepth)

    # remaining header info
    d = pd.read_excel(xl, sheet_name=0, usecols='L', skip_blank_lines=False)
    Surveyors=d['Surveyors:'][0]
    Tair=d['Surveyors:'][4]
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
        PitComments = 'nan'

    # create minimal header info for other files
    index = ['# Location', '# Site', '# PitID', '# Date/Local Time',
         '# UTM Zone', '# Easting', '# Northing']
    column = ['value']
    df = pd.DataFrame(index=index, columns=column)
    df['value'][0] = Location
    df['value'][1] = str(Site)
    df['value'][2] = str(PitID)
    df['value'][3] = pit_datetime_str
    df['value'][4] = str('12N') # Only for Grand Mesa 2020!! str(UTMzone)
    df['value'][5] = str(UTME)
    df['value'][6] = str(UTMN)

    # add minimal header to each data file
    df.to_csv(fname_density, sep=',', header=False)
    df.to_csv(fname_LWC, sep=',', header=False)
    df.to_csv(fname_temperature, sep=',', header=False)
    df.to_csv(fname_stratigraphy, sep=',', header=False)

    # Get page2 pit info (weather, site info)
    d = pd.read_excel(xl, sheet_name=0, usecols='B')
    Weather=d['Location:'][36]
    d = pd.read_excel(xl, sheet_name=0, usecols='F')
    Precip=d['Unnamed: 5'][39]
    Sky=d['Unnamed: 5'][40]
    Wind=d['Unnamed: 5'][41]
    d = pd.read_excel(xl, sheet_name=0, usecols='G')
    GroundCondition=d['Unnamed: 6'][42]
    GroundRoughness=d['Unnamed: 6'][43]
    d = pd.read_excel(xl, sheet_name=0, header=44,usecols='G:M')
    VegType=d.iloc[0,0:7:2] # get ground veg info
    VegBool=d.iloc[1,0:7:2] # Boolean value
    s=VegType[VegBool] # grab veg == True
    GroundVeg=s.values.tolist() # make list of veg type for file
    d = pd.read_excel(xl, sheet_name=0, header=44,usecols='I')
    VegHt=str(d['Unnamed: 8'][2])
    d = pd.read_excel(xl, sheet_name=0, header=44,usecols='K')
    VegHt2=str(d['Unnamed: 10'][2])
    d = pd.read_excel(xl, sheet_name=0, header=44,usecols='M')
    VegHt3=str(d['Unnamed: 12'][2])
    VegHts = [VegHt, VegHt2, VegHt3]
    VegHts = [x for x in VegHts if (str(x) != 'nan' and str(x) != 'cm')] #create a list of VegHts that does not include 'nan'
    d = pd.read_excel(xl, sheet_name=0,usecols='G')
    TreeCanopy=d['Unnamed: 6'][47]

    # create complete header
    index = ['# Location', '# Site', '# PitID', '# Date/Local Time', '# UTM Zone', '# Easting (m)',
         '# Northing (m)', '# Slope (deg)', '# Aspect (deg)', '# Air Temp (deg C)',
          '# Total Depth (cm)',  '# Surveyors', '# WISe Serial No', '# Weather', '# Precip', '# Sky', '# Wind',
          '# Ground Condition', '# Ground Roughness', '# Ground Vegetation', '# Vegetation Height',
          '# Tree Canopy', '# Comments:']
    column = ['value']
    df = pd.DataFrame(index=index, columns=column)
    df['value'][0] = Location
    df['value'][1] = str(Site)
    df['value'][2] = str(PitID)
    df['value'][3] = pit_datetime_str
    df['value'][4] = str('12N') # Only for Grand Mesa 2020!! str(UTMzone)
    df['value'][5] = str(UTME)
    df['value'][6] = str(UTMN)
    df['value'][7] = str(Slope).replace('nan', 'NaN')
    df['value'][8] = str(Aspect).replace('nan', 'NaN')
    df['value'][9] = str(Tair).replace('nan', 'NaN')
    df['value'][10] = str(TotalDepth)
    df['value'][11] = str(Surveyors).replace('\n', ' ')
    df['value'][12] = str(WiseSerialNo)
    df['value'][13] = str(Weather).replace('\n', ' ')
    df['value'][14] = str(Precip)
    df['value'][15] = str(Sky)
    df['value'][16] = str(Wind)
    df['value'][17] = str(GroundCondition)
    df['value'][18] = str(GroundRoughness)
    df['value'][19] = ",".join(GroundVeg)
    df['value'][20] = ",".join(VegHts)
    df['value'][21] = str(TreeCanopy)
    df['value'][22] = str(PitComments)
    df.to_csv(fname_siteDetails, sep=',', header=False, na_rep='NaN')
    print('wrote: ' + fname_siteDetails)
    # newrow = {'Location':Location, 'Site':Site, 'PitID':PitID, 'Date/Local Time':pit_datetime_str,
    #           'UTM Zone':'12N', 'Easting (m)':UTME, 'Northing (m)':UTMN, 'Precipitation':Precip, 'Sky':Sky,
    #           'Wind':Wind, 'Ground Condition':GroundCondition, 'Ground Roughness':GroundRoughness,
    #           'Ground Vegetation':"/".join(GroundVeg), 'Height of Ground Vegetation [cm]':"/".join(VegHts),
    #           'Canopy':TreeCanopy}
    newrow = [Location, Site, PitID, pit_datetime_str, '12N', UTME, UTMN, Precip, Sky, Wind,
               GroundCondition, GroundRoughness, " | ".join(GroundVeg), " | ".join(VegHts), TreeCanopy]

    with open(fname_enviro,'a',newline='') as fd:
        csv_writer = writer(fd, delimiter=',')
        csv_writer.writerow(newrow)

    # get density
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='B:G').replace(r'^\s*$', np.nan, regex=True)
    d=d[:][0:24] # grab density range for one page [this needs improvement for 230cm+ pits]
    d2=np.array(d['top\n(cm)'][:], dtype=float) # get top depths
    d3=d[:][np.isfinite(d2)] # grab rows where top depths are finite
    density_cols=['top\n(cm)','bottom\n(cm)','kg/m3','kg/m3.1','kg/m3.2'] # list of 3 density columns
    d4=np.array(d3[density_cols[2:]],dtype=float) # get the densities
    Ix=np.count_nonzero(np.isfinite(d4),axis=1)>0 # index to rows with finite density values
    density_cols2=['# Top (cm)','Bottom (cm)','Density A (kg/m3)','Density B (kg/m3)','Density C (kg/m3)']
    d4=np.array(d3[density_cols],dtype=float) # get the densities and depths
    density=pd.DataFrame(d4[Ix][:], columns=density_cols2)
    AvgDensity=density[['Density A (kg/m3)', 'Density B (kg/m3)', 'Density C (kg/m3)']].mean(axis=1)
    density.to_csv(fname_density, sep=',', index=False, mode='a', na_rep='NaN')
    print('wrote: ' + fname_density)

    # get LWC
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='B:J').replace(r'^\s*$', np.nan, regex=True)
    d=d[:][0:24]
    d2=np.array(d['top\n(cm)'][:],dtype=float)
    d3 = d[:][np.isfinite(d2)]
    lwc_cols=['top\n(cm)','bottom\n(cm)','dielectric \nconstant','dielectric \nconstant'] # list of 2 permittivity columns
    d4=np.array(d3[lwc_cols[2:]],dtype=float) # get the dielectric constants
    Ix=np.count_nonzero(np.isfinite(d4),axis=1)>0 # index to rows with finite dc values
    LWC_cols2=['# Top (cm)','Bottom (cm)','Permittivity A','Permittivity B']
    d4 = np.array(d3[lwc_cols],dtype=float) # get the dc and depths
    LWC=pd.DataFrame(d4[Ix][:], columns=LWC_cols2)
    LWC.insert(2, "Avg Density (kg/m3)", AvgDensity, False)

    #Calculate LWC
    LWCA_calc = [0.0] * LWC.shape[0]
    LWCB_calc = [0.0] * LWC.shape[0]
    for i in range(0, LWC.shape[0]):
        if(pd.isna(LWC['Permittivity A'][i])):
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
    LWC.insert(5, "LWC-vol A (%)", LWCA_calc, False)
    LWC['LWC-vol A (%)'] = LWC['LWC-vol A (%)'].map(lambda x: '%2.1f' % x)
    LWC.insert(6, "LWC-vol B (%)", LWCB_calc, False)
    LWC['LWC-vol B (%)'] = LWC['LWC-vol B (%)'].map(lambda x: '%2.1f' % x)
    LWC.to_csv(fname_LWC, sep=',', index=False,
                        mode='a', na_rep='NaN', encoding='utf-8')
    print('wrote: ' + fname_LWC)

    # get temperature
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='L:M').replace(r'^\s*$', np.nan, regex=True)
    d=d[:][0:24]
    d2=np.array(d['(cm)'][:],dtype=float)
    temperature = d[:][np.isfinite(d2)]
    temperature.columns = ['# Height (cm)', 'Temperature (deg C)']
    temperature.to_csv(fname_temperature, sep=',', index=False, mode='a', na_rep='NaN')
    print('wrote: ' + fname_temperature)

    # get stratigraphy
    d = pd.read_excel(xl, sheet_name=0, header=8, usecols='O:Z').replace(r'^\s*$', np.nan, regex=True)
    d=d[:][0:24]
    d2=np.array(d['top\n(cm).1'][:],dtype=float)
    d3 = d[:][np.isfinite(d2)]
    strat_cols=d3.columns.values[[0,2,3,8,9,10,11]]
    stratigraphy=d3[strat_cols][:]
    stratigraphy.columns = ['# Top (cm)', 'Bottom (cm)', 'Grain Size (mm)', 'Grain Type',
                    'Hand Hardness','Manual Wetness', 'Comments']

    # Capital letter for grain type, hand hardness, and Wetness letter code (skip when NaN/empty)
    if stratigraphy['Grain Type'].count() > 0:
        stratigraphy['Grain Type'] = stratigraphy['Grain Type'].apply(
            lambda x: [] if isinstance(x, float) else x.upper())

    if stratigraphy['Hand Hardness'].count() > 0:
        stratigraphy['Hand Hardness'] = stratigraphy['Hand Hardness'].apply(
            lambda x: [] if isinstance(x, float) else x.upper())

    if stratigraphy['Manual Wetness'].count() > 0:
        stratigraphy['Manual Wetness'] = stratigraphy['Manual Wetness'].apply(
            lambda x: [] if isinstance(x, float) else x.upper())

    if stratigraphy['Grain Size (mm)'].count() > 0:
        stratigraphy['Grain Size (mm)'] = stratigraphy['Grain Size (mm)'].apply(
            lambda x: [] if isinstance(x, float) else x.replace('\n',' '))

    stratigraphy.to_csv(fname_stratigraphy, sep=',', index=False,
                        mode='a', na_rep='NaN', encoding='utf-8')
    print('wrote: ' + fname_stratigraphy)

    # SWE calculation for summary file
    SWEA_calc = [0.0] * density.shape[0]
    SWEB_calc = [0.0] * density.shape[0]
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
    for i in range(0, density.shape[0]):
        if((i == density.shape[0]-1) & (density['Bottom (cm)'][i] != 0)):  # assume last density measurement to ground surface
           density['Bottom (cm)'][i] = 0

        # Account for missing density values
        if((pd.isna(density['Density A (kg/m3)'][i])) & (pd.isna(density['Density B (kg/m3)'][i]))):
            densityA=density['Density A (kg/m3)'][i-1]
            densityB=density['Density B (kg/m3)'][i-1]

        elif((pd.isna(density['Density A (kg/m3)'][i])) & (pd.notna(density['Density B (kg/m3)'][i]))):
            densityA=density['Density B (kg/m3)'][i]
            densityB=density['Density B (kg/m3)'][i]

        elif((pd.notna(density['Density A (kg/m3)'][i])) & (pd.isna(density['Density B (kg/m3)'][i]))):
            densityA=density['Density A (kg/m3)'][i]
            densityB=density['Density A (kg/m3)'][i]

        else:
            densityA=density['Density A (kg/m3)'][i]
            densityB=density['Density B (kg/m3)'][i]

        # Calculate SWE for each layer
        SWEA_calc[i] = (density['# Top (cm)'][i] - density['Bottom (cm)'][i])*densityA/100
        SWEB_calc[i] = (density['# Top (cm)'][i] - density['Bottom (cm)'][i])*densityB/100
        sumSWEA = sumSWEA + SWEA_calc[i]
        sumSWEB = sumSWEB + SWEB_calc[i]
        sumDensityA = sumDensityA + densityA*(density['# Top (cm)'][i] - density['Bottom (cm)'][i])
        sumDensityB = sumDensityB + densityB*(density['# Top (cm)'][i] - density['Bottom (cm)'][i])

    # calculate weighted average density
    avgDensityA = sumDensityA/density['# Top (cm)'][0]
    avgDensityB = sumDensityB/density['# Top (cm)'][0]
    avgDens = (avgDensityA + avgDensityB)/2
    avgSWE = (sumSWEA + sumSWEB)/2

    # newrow = {'Location':Location, 'Site':Site, 'PitID':PitID, 'Date/Local Time':pit_datetime_str,
    #       'UTM Zone':'12N', 'Easting (m)':UTME, 'Northing (m)':UTMN, 'Density A Mean [kg/m^3]':avgDensityA,
    #       'Density B Mean [kg/m^3]':avgDensityB, 'Density Mean [kg/m^3]':avgDens, 'SWE A [mm]':sumSWEA,
    #       'SWE B [mm]':sumSWEB, 'SWE [mm]':avgSWE, 'Snow Depth [cm]':density['# Top (cm)'][0]}
    newrow = [Location, Site, PitID, pit_datetime_str, '12N', UTME, UTMN, avgDensityA,
          avgDensityB, avgDens, sumSWEA, sumSWEB, avgSWE, density['# Top (cm)'][0]]

    with open(fname_swe,'a',newline='') as fd:
        csv_writer = writer(fd, delimiter=',')
        csv_writer.writerow(newrow)



if __name__ == "__main__":
    # Version ID of the pit files
    version = 'v01'
    path_in='../PITS_XLSX_EDITED/'
    path_out='../PITS_CSV_EDITED/'
    fname_summarySWE = path_out + 'SnowEx20_SnowPits_GMIOP_Summary_SWE_2020_' + version + '.csv'
    fname_summaryEnviro = path_out + 'SnowEx20_SnowPits_GMIOP_Summary_Environment_2020_' + version + '.csv'
    # create complete headers
    column = ['Location', 'Site', 'PitID', 'Date/Local Time', 'UTM Zone', 'Easting (m)',
                'Northing (m)', 'Density A Mean (kg/m^3)', 'Density B Mean (kg/m^3)', 'Density Mean (kg/m^3)',
                'SWE A (mm)',  'SWE B (mm)', 'SWE (mm)', 'Snow Depth (cm)']
    df_swe = pd.DataFrame(columns=column)
    df_swe.to_csv(fname_summarySWE, index=False, sep=',', header=True)

    column = ['Location', 'Site', 'PitID', 'Date/Local Time', 'UTM Zone', 'Easting (m)',
                   'Northing (m)', 'Precipitation', 'Sky', 'Wind', 'Ground Condition', 'Ground Roughness',
                   'Ground Vegetation', 'Height of Ground Vegetation (cm)', 'Canopy']
    df_enviro = pd.DataFrame(columns=column)
    df_enviro.to_csv(fname_summaryEnviro, index=False, sep=',', header=True)

    # loop over all pits
    for filename in glob.glob(path_in + 'COGM*.xlsx'):
        print(filename)
        r = readSnowpit(path_in, filename, version, path_out, fname_summarySWE, fname_summaryEnviro)
