#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 23:45:00 2023

@author: ruchak
"""
import os
import requests
import numpy as np
from PIL import Image
from io import BytesIO
import pandas as pd
from datetime import date, datetime
import re
import shutil
from dbs import *
import logging

urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.ERROR)  
urllib3_logger = logging.getLogger('PIL')
urllib3_logger.setLevel(logging.ERROR)  
def cleanData(source_df):
    '''
    cleanData function is going to remove all rows with no image url and give wafercodes and lot ids

    Parameters
    ----------
    source_df : updated source df
        DataFrame that is an output from keepLatestData

    Returns
    -------
    images : list of image urls to process
        
    hd_num : head serial numbers of all the images
        
    dates : insert date of peg images retrieved from the source table
        
    wafercode : wafercodes of all heads
        
    wafer_substrate_lot_id : wafer substrate lot IDs of all heads

    '''
    source_df = source_df[source_df['PEG_FROM_CORE_URL_BB'].notna()]     
    images = source_df['PEG_FROM_CORE_URL_BB'].tolist()
    hd_num = source_df['HEAD_SERIAL_NUM'].tolist()
    dates = source_df['PEG_SEM_DATE'].tolist()
    wafercode = [x[1:3] for x in hd_num]
    wafer_substrate_lot_id = [x[1:6] for x in hd_num]
    return images, hd_num, dates, wafercode, wafer_substrate_lot_id
    
def storeData(images, hd_num):
    '''
    Downloads and stores peg images in the anomaly folder

    Parameters
    ----------
    images : List of URLs to download from.
        
    hd_num : List of head numbers.

    Returns
    -------
    None.

    '''
    # remove existing test files and output directory
    try:
        os.system('rm -rf /home/ruchak/peg_classification/PFA/1.abnormal')
    except:
        pass
    
    if not(os.path.exists(os.path.join('/home/ruchak/peg_classification/PFA/', '1.abnormal'))):
        os.mkdir('/home/ruchak/peg_classification/PFA/1.abnormal')
    
    
    #download new images
    for img in range(len(images)):
        try:
            im = Image.open(BytesIO(requests.get(images[img], verify = False).content))
            im.save('/home/ruchak/peg_classification/PFA/1.abnormal/' + hd_num[img] + '.png')
            
        except Exception as e:
            print(e)
            pass
    
    #creating inputs for the peg classifiers.
    df_test = pd.DataFrame()
    valid_images = sorted(os.listdir('/home/ruchak/peg_classification/PFA/1.abnormal/'))
    df_test['HEAD_SERIAL_NUM'] = [x[:-4] for x in valid_images]
    df_test.to_csv('/home/ruchak/peg_classification/PFA/scripts/df_test.csv', index = False)
        
    os.environ['MKL_THREADING_LAYER'] = 'GNU'
    os.system('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/ruchak/anaconda3/lib/')

def runModels():
    '''
    Runs the ROD and anomaly detection system on peg CDSEM images.

    Returns
    -------
    None.

    '''

    #running the peg deformation model
    os.system('python /home/ruchak/peg_classification/PFA/scripts/main.py')
    

def storeResults(dates, hd_num, images, wafercode, wafer_substrate_lot_id):
    '''
    

    Parameters
    ----------
    dates : List of insert_dates
        
    images : List of image links to download images from.
        
    wafercode : List of Wafercode derived from head numbers
        
    wafer_substrate_lot_id : List of Wafer substrate lot IDs derived from head numbers
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    #getting peg ROD results
    
    deformation_res = pd.DataFrame()
    deformation_res['HEAD_SERIAL_NUM'] = hd_num
    deformation_res['PEG_SEM_DATE'] = dates
    deformation_res['INPUT_LINKS'] = images
    deformation_res['WAFERCODE'] = wafercode
    deformation_res['WAFER_SUBSTRATE_LOT_ID'] = wafer_substrate_lot_id
    deformation_res['LAST_UPDATE_DATE'] = date.today()
    deformation_res1 = pd.read_csv('/home/ruchak/peg_classification/PFA/scripts/df_test.csv')
    deformation_res = deformation_res.merge(deformation_res1, on = 'HEAD_SERIAL_NUM').drop_duplicates()
    deformation_res = deformation_res.sort_values(by = 'HEAD_SERIAL_NUM')

    rod_preds = np.load('/home/ruchak/peg_classification/PFA/scripts/predictions_test.npy', allow_pickle = False)
    
    deformation_res['PROBABILITY_DEFORMATION'] = rod_preds[:, 1]
    deformation_res['PROBABILITY_INTACT'] = rod_preds[:,0]
    
    deformation_res['PEG_CLASSIFICATION'] = np.argmax(rod_preds, axis = 1)
    deformation_res = deformation_res.rename(columns = {'HEAD_SERIAL_NUM': 'HD_NUM'})
    
    deformation_res['PEG_SEM_DATE'] = pd.to_datetime(deformation_res['PEG_SEM_DATE'])
    deformation_res['LAST_UPDATE_DATE'] = pd.to_datetime(deformation_res['LAST_UPDATE_DATE'])
    
    for i in range(0, len(deformation_res), 999):
        print('in here')
        try:
            subset = deformation_res.iloc[i:i+999]
        except:
            subset = deformation_res[i:]
        print('now upserting ', subset)
        upsert_to_edw_prep(subset, 'hamr_ida_rw', 'peg_deformation_pfa')
        
    return deformation_res
    
     
    
