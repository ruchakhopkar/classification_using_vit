#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 19:34:49 2023

@author: ruchak
"""

#################### DB CONNECT ##############################################
'''
This script includes all code required for uploading data to DB
'''

from utils import *
import Config as cfg
from datetime import datetime
def getInputData(schema, table, schema2, table2, columns, waferlist = [], head_column = 'HEAD_SERIAL_NUM', link_col = 'URL_POLE_METROLOGY', source_date_col = 'INSERTCDSEMDATE', hd_list = []):
    '''
    This function will return a dataframe with all specified columns from source enseapedia database

    Parameters
    ----------
    schema : schema of source table
        
    table : table name of source table
        
    columns : column names to be retrieved
        
    waferlist : list of wafer in case wafer basis
        The default is [].
    
    head_column : Column name of head_serial_num

    Returns
    -------
    source_df : source dataframe 

    '''
    #col_str = ['jmp.'+col for col in columns]
    col_str  = ','.join(columns)
    
    if (len(waferlist) == 0) & (len(hd_list) == 0):
        
        # get the data
        query = f"""
                     select {col_str}
                     from {schema2}.{table2} cdsem right join {schema}.{table} jmp
                     on cdsem.hd_num = jmp.{head_column}
                     where (jmp.{link_col} is not null) and (jmp.{source_date_col}> (select max(last_update_date)-365 from {schema2}.{table2}))
                """
    elif len(hd_list)!= 0 :
        
        # get the data
        query = f"""
                     select {col_str}
                     from {schema2}.{table2} cdsem right join {schema}.{table} jmp
                     on cdsem.hd_num = jmp.{head_column}
                     where (jmp.{link_col} is not null) and (jmp.{head_column} in ('{"','".join(hd_list)}'))
                """
    else:
        
        query = f"""
                     select {col_str}
                     from {schema2}.{table2} cdsem right join {schema}.{table} jmp
                     on cdsem.hd_num = jmp.{head_column}
                     where (jmp.{link_col} is not null) and jmp.wafercode in ('{"','".join(waferlist)}')
                """
    
    nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
    source_df = nrm_ifc.read(query)

    
    return source_df

def keepLatestData(source_df, schema, table, head_col_name, wafer_list = [], hd_list = [], source_data_col = 'INSERTCDSEMDATE'):
    '''
    Parameters
    ----------
    source_df : Dataframe received by querying source table
        
    schema : schema name for update table
        
    table : table name of update table
    
    head_col_name: The column name for head_serial_num from source_table
        
    wafer_basis : List of wafers to update, if an everyday process send empty list
        The default is [].
    
    source_data_col : Date column to check with in the source table

    Returns
    -------
    Updated source dataframe

    '''
    if (len(wafer_list) == 0) & (len(hd_list) == 0):
        #keep only the latest data
        query = f"""
                         select MAX(last_update_date), hd_num
                         from {schema}.{table}
                         
                    """
        
        nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
        latest_date = nrm_ifc.read(query).iloc[0,0]
        print(latest_date)
        if latest_date is None:
            latest_date = '2023-04-01'
        
        try:
            source_df = source_df[source_df[source_data_col] >= datetime.strptime(str(latest_date), '%Y-%m-%d')]
        except ValueError as v:
            if len(v.args) >0 and v.args[0].startswith('unconverted data remains: '):
                
                t = datetime.strptime(str(latest_date), '%Y-%m-%d %H:%M:%S')
                
                source_df = source_df[pd.to_datetime(source_df[source_data_col]) >= t]
            else:
                raise
        print(source_df)
    
    elif len(hd_list)!=0:
        
        #get all heads from the input wafer list that is already updated
        query = f"""
                         select hd_num 
                         from {schema}.{table}
                         where hd_num in ('{"','".join(hd_list)}')
                    """
        
        nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
        existing_hds = nrm_ifc.read(query)
        if existing_hds is None:
            existing_hds = []
        else:
            existing_hds = existing_hds['HD_NUM'].to_list()
        
        
        all_heads = source_df[head_col_name].to_list()
        
        hd_list = list(set(all_heads).difference(set(existing_hds)))
        
        source_df = source_df[source_df[head_col_name].isin(hd_list)]
        print(source_df)
        
    else:
        #get all heads from the input wafer list that is already updated
        query = f"""
                         select hd_num 
                         from {schema}.{table}
                         where wafercode in ('{"','".join(wafer_list)}')
                    """
        
        nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
        existing_hds = nrm_ifc.read(query)
        if existing_hds is None:
            existing_hds = []
        else:
            existing_hds = existing_hds['HD_NUM'].to_list()
        
        
        all_heads = source_df[head_col_name].to_list()
        
        hd_list = list(set(all_heads).difference(set(existing_hds)))
        
        source_df = source_df[source_df[head_col_name].isin(hd_list)]
        
    return source_df
        
        
def upsert_to_edw_prep(df,schema,table):

    hd_num_text = "', 0), ('".join(map(str,df['HD_NUM'].unique()))

    edw_query = f"""select 
                        hd_num
                    from 
                        {schema}.{table} 
                    where 
                        (hd_num,0) in (('{hd_num_text}', 0))
                  """
    reli_df = pd.DataFrame()
    print(edw_query)
    try:
        nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
        reli_df = nrm_ifc.read(edw_query)
        if reli_df is None:
            reli_df = pd.DataFrame()
        else:
            reli_df.columns = reli_df.columns.str.upper()
        logger.info(f"Reading {table} data into EDW database successfully completed")
    except Exception as ex:
        logger.error(f"Error occured while reading {table} table data in EDW",exc_info=True)
        insert_error_tracker(f"Error occured while reading {table} table data in EDW")

    if reli_df.shape[0]==0:
        df['STATUS'] = 'INSERT'
    else:
        reli_df['STATUS'] = 'UPDATE'
        df = df.merge(reli_df,how='left', on = 'HD_NUM')
        df['STATUS'] = df['STATUS'].fillna('INSERT')

    """2. Split into insert and update dataframes"""
    df_insert = df[df['STATUS']=="INSERT"]
    df_update = df[df['STATUS']=="UPDATE"]

    """3. Insert to EDW"""
    if df_insert.shape[0]>0:
        logger.info("Inserting the new heads")
        df_insert = df_insert.drop(['STATUS'],axis=1)
        df_insert = df_insert.where(pd.notnull(df_insert), None) #convert nan values to oracle null before insert
        cols_list = df_insert.columns
        edw_query = f"""INSERT INTO {schema}.{table} ("""
        for idx,cols in enumerate(cols_list):
            if not idx == len(cols_list)-1:
                edw_query +=cols+','
            else:
                edw_query +=cols
        edw_query +=") VALUES ("
        for idx,cols in enumerate(cols_list):
            if not idx == len(cols_list)-1:
                edw_query +=':'+cols+','
            else:
                edw_query +=':'+cols
        edw_query +=  ')'
        try:
            nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
            params = list(df_insert.itertuples(index=False,name=None))

            nrm_ifc.batch_insert(edw_query,params)
            logger.info(f"Inserting {schema}.{table} data into EDW database successfully completed")
        except Exception as ex:
            logger.error("Error occured while inserting {table} table data in EDW".format(table=table),exc_info=True)
            insert_error_tracker("Error occured while inserting {table} table data in EDW".format(table=table))

    """3. Update to EDW"""
    if df_update.shape[0]>0:
        logger.info("Updating the existing heads")
        df_update = df_update.drop(columns = ['STATUS'], axis = 1)
        df_update = df_update.where(pd.notnull(df_update), None) #convert nan values to oracle null before insert
        cols_list = df_update.columns

        count=0
        for index,row in df_update.iterrows():
            sbr = df_update['HD_NUM'].iloc[count]

            edw_query = f"""UPDATE {schema}.{table} SET """
            params = []
            count+=1
            for idx,cols in enumerate(cols_list):
                if not idx == len(cols_list)-1:
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"',"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '"+ str(row[cols])+"'"+","
                        else:
                            edw_query +=cols+'='+str(row[cols])+","
                else:
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"'"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '" + str(row[cols])+"'"
                        else:
                            edw_query +=cols+'='+str(row[cols])
            if edw_query.endswith(","):
                edw_query = edw_query[:-1]
            edw_query +=f" WHERE HD_NUM = '{sbr}'"
            try:
                nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
                params=list(tuple(params))
                nrm_ifc.insert(edw_query,params)
                logger.info("Updating {table} data into EDW database successfully completed".format(table=table))
            except Exception as ex:
                logger.error("Error occured while Updating {table} table data in EDW".format(table=table),exc_info=True)
                insert_error_tracker("Error occured while Updating {table} table data in EDW".format(table=table))
        


