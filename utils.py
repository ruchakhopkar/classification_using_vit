


import os
import cx_Oracle
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import numpy as np
import time
import datetime as dt
import Config as cfg
import shutil
import logging
import logging.config
import json
from sqlalchemy import create_engine
import pymysql
import math
from math import log10, floor
    

'''
'LOGGING INSTATIATION
'''
class Log:
    def __init__(self):
        self.logger = None
        if not os.path.exists("./logs"):
            os.mkdir("./logs")
        

    def get_logger(self):
        if self.logger==None:        
            value = os.getenv(cfg.env_key, None)
            if value:
                path = value
            if os.path.exists(cfg.path):
                with open(cfg.path, 'rt') as f:
                    log_config = json.load(f)
                logging.config.dictConfig(log_config)
            else:
                logging.basicConfig(level=logging.DEBUG)
            self.logger = logging.getLogger(__name__)
            # print(type(self.logger))
        return self.logger
        

# logging.config.fileConfig('logging.ini')
env_key='LOG_CFG'
path = 'logging.json'
value = os.getenv(env_key, None)
if value:
    path = value
if os.path.exists(path):
    with open(path, 'rt') as f:
        log_config = json.load(f)
    logging.config.dictConfig(log_config)
else:
    logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DAO_Interface:
    """
    A class that works a interface for oracle or mysql database connections and access
    """

    def __init__(self,username,passwd,hostname,sid,port,encoding,dbtype='oracle'):
        self.username = username
        self.passwd = passwd
        self.hostname = hostname
        self.sid      = sid
        self.port     = port
        self.encoding = encoding
        self.dbtype = dbtype

    def update(self,df,table_name,update_type='append'):
        start_time = time.time()
        conn = Oracle_DB_Connection(self.username,self.passwd,self.hostname,self.sid,self.port,self.encoding)
        # table_name = 'dvl_pfa_user_feedback_test'
        update_status = conn.updateTableFromDf(df,table_name,update_type)
        # conn.close()
        end_time = round(time.time() - start_time,2)
        logger.info('Writing to oracle DB completed in '+str(dt.timedelta(seconds=end_time)))
        return update_status
    
    def read(self,sql_query):
        start_time = time.time()
        conn = Oracle_DB_Connection(self.username,self.passwd,self.hostname,self.sid,self.port,self.encoding)
        df = conn.retrieveAsDF(sql_query)
        conn.close()
        end_time = round(time.time() - start_time,2)
        logger.info('Reading oracle DB data completed in '+str(dt.timedelta(seconds=end_time)))
        return df
    
    def execute(self,sql_query,params=tuple()):
        start_time = time.time()
        conn = Oracle_DB_Connection(self.username,self.passwd,self.hostname,self.sid,self.port,self.encoding)
        df = conn.execute(sql_query,params)
        conn.close()
        end_time = round(time.time() - start_time,2)
        logger.info('Reading oracle DB data completed in '+str(dt.timedelta(seconds=end_time)))
        return df
    
    def insert(self,sql_query,params):
        start_time = time.time()
        conn = Oracle_DB_Connection(self.username,self.passwd,self.hostname,self.sid,self.port,self.encoding)
        # df=None
        # for param in params:
        df = conn.insert(sql_query,params)
        conn.close()
        end_time = round(time.time() - start_time,2)
        logger.info('Reading oracle DB data completed in '+str(dt.timedelta(seconds=end_time)))
        return df
    
    def batch_insert(self,sql_query,params):
        start_time = time.time()
        conn = Oracle_DB_Connection(self.username,self.passwd,self.hostname,self.sid,self.port,self.encoding)
        df = conn.batch_insert(sql_query,params)
        conn.close()
        end_time = round(time.time() - start_time,2)
        logger.info('Reading oracle DB data completed in '+str(dt.timedelta(seconds=end_time)))
        return df
    def batch_update(self,sql_query,params):
        start_time = time.time()
        conn = Oracle_DB_Connection(self.username,self.passwd,self.hostname,self.sid,self.port,self.encoding)
        df = conn.batch_update(sql_query,params)
        conn.close()
        end_time = round(time.time() - start_time,2)
        logger.info('Reading oracle DB data completed in '+str(dt.timedelta(seconds=end_time)))
        return df



class Oracle_DB_Connection:
    '''
     Description:
     ------------
        A class used to represent an Oracle Database connectivity

     Attributes:
     -----------
        username : str
            a string containing database username
        passwd : str
            a string containing database password
        hostname : str
            a string containing database hostname
        encoding : str
            a string containing database encoding format
        connection : cx_Oracle
            an oracle connection object
     
     Methods:
     --------
        connect     - To establish connection to Oracle DB
        execute     - To perfrom all the CRUD operations
    '''

    def __init__(self,username,passwd,hostname,sid,port,encoding):
        self.username   = username
        self.passwd     = passwd
        self.hostname   = hostname
        self.sid        = sid
        self.port       = port
        self.encoding   = encoding
        self.isAlive    = False
        self.connection = self.connect()
        
        

    def connect(self):
        '''
         Description:
         ------------
            Establishes a singleton connection to Oracle Database

         Parameters:
         -----------
            No parameters
         
         Returns:
         --------
            connection (oracle connection object)

        '''
        if not self.isAlive:
            try:
                dsn = cx_Oracle.makedsn(
                    host=self.hostname,
                    port=self.port,
                    sid=self.sid)
                connection = cx_Oracle.connect(
                    user = self.username,
                    password = self.passwd,
                    dsn=dsn
                )
                logger.info('Connected to Oracle %s'%(connection.version))
                self.isAlive =  True
                return connection
            except cx_Oracle.Error as error:
                # print(error)
                logger.error("Exception during database connection",exc_info=True)
                self.isAlive = False
                return None
        else:
            return self.connection

    def retrieve(self,query_string):
        '''
         Description:
         ------------
            Performs data retrieval operations on Oracle Database

         Parameters:
         -----------
            query_string - str
                Type of query operation to be performed
         
         Returns:
         --------
            Oracle cursor object

        '''
        try:
            self.cursor = self.connection.cursor()
            db_rows = self.cursor.execute(query_string)
            return db_rows
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
        finally:
            self.cursor.close()

    
    
    def retrieveAsDF(self,query_string):
        '''
         Description:
         ------------
            Performs data retrieval operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Select query syntax

         Returns:
         --------
            Dataframe 

        '''
        try:
            # engine = create_engine("oracle+cx_oracle://"+self.username+":"+self.passwd+"@"+self.hostname+'/'+self.sid)
            # connection = engine.connect()
            # db_rows = pd.read_sql_query(query_string,connection)
            db_rows = pd.read_sql_query(query_string,self.connection)
            return db_rows
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
            return None 
    
    # def insert_or_update(sql,params):
    #     self.cursor = self.connection.cursor()
    #         self.cursor.execute(query_string,parmeters)
    #         self.connection.commit()
    #         return True
    #     self.cursor = self.connection.cursor()

    #     self.cursor.setinputsizes(cx_Oracle.CLOB, cx_Oracle.CLOB, 20)
    #     self.cursor.prepare(UPDATE_STATEMENT)
    #     self.cursor.execute(None, params)
    #     self.connection.commit()

    #     try:
    #         cursor.setinputsizes(cx_Oracle.CLOB, cx_Oracle.CLOB, 20)
    #         cursor.prepare(INSERT_STATEMENT)
    #         cursor.execute(None, params)
    #         connection.commit()
    #         print >> sys.stderr, "++ New Entry:", identifier
    #     except cx_Oracle.IntegrityError, e:
    #         if ("%s" % e.message).startswith('ORA-00001:'):
    #             print >> sys.stderr, "Entry already there:", identifier
    #         else:
    #             raise e
    #     finally:
    #         cursor.close()
    
    
    
    def insert(self,query_string,parmeters):
        '''
         Description:
         ------------
            Inserts the given query in the oracle database

         Parameters:
         -----------
            query_string - str
                SQL Query to be executed

         Returns:
         --------
            Boolean (True/False) 
        '''
        try:
            self.cursor = self.connection.cursor()
            print('-'*80)
            print(query_string)
            self.cursor.execute(query_string,parmeters)
            self.connection.commit()
            return True
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
            return False
        finally:
            self.cursor.close()

    def batch_insert(self,query_string,parmeters):
        '''
         Description:
         ------------
            Inserts the given query in the oracle database

         Parameters:
         -----------
            query_string - str
                SQL Query to be executed

         Returns:
         --------
            Boolean (True/False) 
        '''
        try:
            self.cursor = self.connection.cursor()
            print(query_string)
            self.cursor.executemany(query_string,parmeters,batcherrors=False)
            self.connection.commit()
            return True
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
            return False
        finally:
            self.cursor.close()



    
    def execute(self,query_string,sql_parameter):
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query_string,sql_parameter)
            self.connection.commit()
            return True
        except Exception as ex:
            logger.error(f"Exception during execute function",exc_info=True)
            return False
        finally:
            self.cursor.close()
    
    
    def updateTableFromDf(self,df,table_name,update_type='append'):
        '''
         Description:
         ------------
            Performs data retrieval operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Select query syntax

         Returns:
         --------
            Boolean (True/False) 
        '''
        try:
            engine = create_engine("oracle+cx_oracle://"+self.username+":"+self.passwd+"@"+self.hostname+'/'+self.sid)
            connection = engine.connect()
            df.to_sql(con=connection, name=table_name, if_exists=update_type,index=False) #replace | append DVL_PFA_USER_FEEDBACK_TEST
            return True
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
            return False
        finally:
            connection.close()
    
    def cursor_to_df(self,cursor):
        df = pd.DataFrame(cursor.fetchall())
        column_info = cursor.description
        cols = []
        for column in column_info:
            cols.append(column[0])
        df.columns = cols
        return df

    
    def close(self):
        # try:
        #     if self.cursor:
        #         self.cursor = self.cursor.close()
        #     else:
        #         logger.info("No active cursor to close")
        # except Exception as ex:
        #     logger.error("Exception occured when trying to close oracle db cursor",exc_info=True)
        
        try:
            if self.connection:
                self.connection.close()
                logger.info("Oracle DB connection successfully closed")                
            else:
                logger.info("No active connections to close")
        except Exception as ex:
            logger.error("Exception occured when trying to close oracle db connection",exc_info=True)


class MySQL_DB_Connection:
    '''
     Description:
     ------------
        A class used to represent an MySQL Database connectivity

     Attributes:
     -----------
        username : str
            a string containing database username
        passwd : str
            a string containing database password
        hostname : str
            a string containing database hostname
        encoding : str
            a string containing database encoding format
        connection : mysql_connector
            an mysql connection object
        cursor : mysql_cursor
            a mysql cursor object
     
     Methods:
     --------
        connect     - To establish connection to Oracle DB
        retrieve    - To perfrom the data retrieval operation
        insert      - To perform data creation operation
    '''

    def __init__(self,username,passwd,hostname,db_name):
        self.username = username
        self.passwd = passwd
        self.hostname = hostname
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self.isAlive = False
        self.connect()
    
    def connect(self):
        '''
         Description:
         ------------
            Establishes a singleton connection to MySQL Database

         Parameters:
         -----------
            No parameters
         
         Returns:
         --------
            Boolean (True/False)

        '''
        if not self.isAlive:
            try:
                self.connection = mysql.connector.connect(
                    user=self.username,password = self.passwd,
                    host = self.hostname,
                    database = self.db_name
                )
                logger.info("Connected to MySQL Server Version "+'.'.join(str(x) for x in self.connection._server_version))
                self.cursor = self.connection.cursor(buffered=True)
                return True
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    logger.error("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    logger.error("Database does not exist")
                else:
                    logger.error(err)
                return False
    def updateTableFromDf(self,df,table_name,update_type='append'):
        '''
         Description:
         ------------
            Performs data retrieval operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Select query syntax

         Returns:
         --------
            Boolean (True/False) 
        '''
        try:
            engine = create_engine("mysql+pymysql://"+self.username+":"+self.passwd+"@"+self.hostname+"/"+self.db_name)
            connection = engine.connect()
            df.to_sql(con=connection, name=table_name, if_exists=update_type,index=False) #replace | append DVL_PFA_USER_FEEDBACK_TEST
            return True
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
            return False
        finally:
            connection.close()

    
    
    def insert(self,query_string,values):
        '''
         Description:
         ------------
            Performs data creation/insertion operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Insert query operation to be performed
            values - tuple
                Values to be inserted into database
         
         Returns:
         --------
            Boolean (True/False)

        '''
        try:
            # self.cursor = self.connection.cursor(buffered=True)
            print(query_string)
            self.cursor.execute(query_string,values)
            self.connection.commit()
            return True
        except Exception as ex:
            logger.error("Error while inserting data",exc_info=True)
            return False

    def batch_insert(self,query_string,values):
        '''
         Description:
         ------------
            Performs data creation/insertion operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Insert query operation to be performed
            values - tuple
                Values to be inserted into database
         
         Returns:
         --------
            Boolean (True/False)

        '''
        try:
            # self.cursor = self.connection.cursor(buffered=True)
            self.cursor.executemany(query_string,values)
            self.connection.commit()
            self.cursor.close()
            return True
        except Exception as ex:
            logger.error("Error while inserting data",exc_info=True)
            return False
    
    def delete(self,query_string,sql_parameter):
        try:
            # self.cursor.execute('SET SQL_SAFE_UPDATES = 0')
            # self.cursor = self.connection.cursor(buffered=True)
            self.cursor.execute(query_string,sql_parameter)
            self.connection.commit()
            return True
        except Exception as ex:
            logger.error("Exception during delete function",exc_info=True)
            return False
    
    
    def retrieve(self,query_string,values=None):
        '''
         Description:
         ------------
            Performs data retrieval operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Select query syntax
            values - dict
                Dictionary values to subsitute in where clause
         
         Returns:
         --------
            Total number of rows 

        '''
        try:
            db_rows = self.cursor.execute(query_string,values)
            return db_rows
        except Exception as ex:
            logger.error("Exception during retrieve function",exc_info=True)
            return None 


    def retrieveAsDF(self,query_string):
        '''
         Description:
         ------------
            Performs data retrieval operation on MySQL Database Table

         Parameters:
         -----------
            query_string - str
                Select query syntax

         Returns:
         --------
            Dataframe 

        '''
        try:
            engine = create_engine("mysql+pymysql://"+cfg.mys_username+":"+cfg.mys_passwd+"@"+cfg.mys_hostname+"/"+cfg.mys_db_name)
            connection = engine.connect()
            # logger.info(query_string)
            db_rows = pd.read_sql_query(query_string,connection)
            connection.close()
            return db_rows
        except Exception as ex:
            logger.error("Exception during mySQL retrieve function",exc_info=True)
            return None 
        #finally:
        #    connection.close() 
    
    def close(self):
        try:
            if self.isAlive:
                self.connection.close()
                logger.info("MySQL DB connection successfully closed")
            else:
                logger.info("No active connections to close")
        except Exception as ex:
            logger.error("Exception occured when trying to close mysql db connection",exc_info=True)
    
    
    # def close(self):
    #     '''
    #      Description:
    #      ------------
    #         To close DB connection on MySQL Database

    #      Parameters:
    #      -----------
    #         No Parameters
         
    #      Returns:
    #      --------
    #         Boolean (True/False)

    #     '''
    #     try:
    #         if not self.connection == None:
    #             self.cursor.close()
    #             self.cursor = None
    #             self.connection.close()
    #             self.connection = None
    #             logger.info('Closed existing connection')
    #         else:
    #             logger.info('No existing connection found for closing')
    #         return True
    #     except Exception as ex:
    #         logger.error("Exception encoured during close",exc_info=True)
    #         return False

def getExpGroup(exp_prefix):    
    ap_dict = {
        'qP':'WQ_MP',
        'qE':'WQ_MP',
        'E':'WQ_NPD',
        'G':'NON_WQ'
    }
    print(exp_prefix)
    exp_group = ap_dict[exp_prefix] if exp_prefix in ap_dict.keys() else 'NON_WQ'
    return exp_group



def getAPVersion(cms_config):
    base_code = cms_config[7:8]
    
    ap_dict = {
        '1':'AP2.0.3',
        '2':'AP2.1.4',
        '3':'AP2.5.1',
        '4':'AP3.2',
        '5':'AP3.3',
        '6':'AP3.4',
        '7':'AP3.5',
        '8':'AP3.6'
    }
    param = ap_dict[base_code] if base_code in ap_dict.keys() else 'INVALID'
    return param

def getAPVersion2(cms_config):
    logger.info(cms_config)
    base_code = cms_config[7:8]
    
    ap_dict = {
        '1':'AP2.0.3',
        '2':'AP2.1.4',
        '3':'AP2.5.1',
        '4':'AP3.2',
        '5':'AP3.3',
        '6':'AP3.4',
        '7':'AP3.5',
        '8':'AP3.6'
    }
    script = cms_config[14:16]
    AP_Version = ap_dict[base_code]+'_'+script
    return AP_Version



def getAPVersion1(base_code):
    # base_code = cms_config[7:8]
    
    ap_dict = {
        '1':'AP2.0.3',
        '2':'AP2.1.4',
        '3':'AP2.5.1',
        '4':'AP3.2',
        '5':'AP3.3',
        '6':'AP3.4',
        '7':'AP3.5',
        '8':'AP3.6'
    }
    # param = ap_dict[base_code] if base_code in ap_dict.keys() else 'INVALID'
    return ap_dict[base_code]

def getCurrentData():
    return dt.datetime.now().strftime('%Y-%m-%d')

def getIOP25Spec(ap_rev):    
    spec_dict = {
        'AP2.0.3':0,
        'AP2.1.4':0,
        'AP2.5.1':13,
        'AP3.2':13,
        'AP3.3':13,
        'AP3.4':10,
        'AP3.5':10,
        'AP3.6':10
    }
    return spec_dict[ap_rev]

def getTBWIOP25Spec(ap_rev):    
    spec_dict = {
        'AP2.0.3':0,
        'AP2.1.4':0,
        'AP2.5.1':27,
        'AP3.2':27,
        'AP3.3':27,
        'AP3.4':13,
        'AP3.5':27,
        'AP3.6':13
    }
    return spec_dict[ap_rev]

def getTBWSpec(ap_rev):    
    spec_dict = {
        'AP2.0.3':0,
        'AP2.1.4':0,
        'AP2.5.1':10,
        'AP3.2':10,
        'AP3.3':10,
        'AP3.4':10,
        'AP3.5':10,
        'AP3.6':10
    }
    return spec_dict[ap_rev]

def getTBWTTFHCSpec(ap_rev):    
    spec_dict = {
        'AP2.0.3':0,
        'AP2.1.4':0,
        'AP2.5.1':15,
        'AP3.2':15,
        'AP3.3':15,
        'AP3.4':15,
        'AP3.5':15,
        'AP3.6':15
    }
    return spec_dict[ap_rev]

    # CFR_104_IOP25_TBW_ACTUAL_BETA

# def getParamSpec(comment_spec):
#     param_dict = {
#         'AP2.5.1_C5_ER12_HL' :'CFR_100_COMBINE_ACTUAL_BETA',
#         'AP3.4_B1_ER12_HL'   :'CFR_104_IOP25_TBW_ACTUAL_BETA',
#         'AP3.4_B1_ER12_HC'   :'CFR_104_IOP25_TBW_ACTUAL_BETA',
#         'AP3.4_B2_ER14_HL'   :'CFR_104_IOP25_TBW_ACTUAL_BETA', 
#         'AP3.4_B2_ER14_HC '  :'CFR_104_IOP25_TBW_ACTUAL_BETA'
#     }
#     param = param_dict[comment_spec] if comment_spec in param_dict.keys() else 'CFR_104_IOP25_TBW_ACTUAL_BETA'
#     return param

def getParamSpec(comment_spec,uncensor_cnt=0):
    param_dict = {
            'AP2.5.1_C5_ER12_HL' :'CFR_100_COMBINE_ACTUAL_BETA',
            'AP3.4_B1_ER12_HL'   :'CFR_104_IOP25_TBW_ACTUAL_BETA',
            'AP3.4_B1_ER12_HC'   :'CFR_104_IOP25_TBW_ACTUAL_BETA',
            'AP3.4_B2_ER14_HL'   :'CFR_104_IOP25_TBW_ACTUAL_BETA', 
            'AP3.4_B2_ER14_HC '  :'CFR_104_IOP25_TBW_ACTUAL_BETA'
    }
    if uncensor_cnt >=20:
        param = param_dict[comment_spec] if comment_spec in param_dict.keys() else 'CFR_104_TBW_ACTUAL_BETA'
    else:
        
        param = param_dict[comment_spec] if comment_spec in param_dict.keys() else 'CFR_104_TBW_FIXED_BETA'
    return param


def getOfficialSpec(comment_spec):
    ap_rev = comment_spec.split('_')[0]
    spec_dict = {
        'AP2.5.1_C5_ER12_HL' : getIOP25Spec('AP2.5.1'), 
        'AP3.4_B1_ER12_HL'   : getTBWIOP25Spec('AP3.4'),
        'AP3.4_B1_ER12_HC'   : getTBWTTFHCSpec('AP3.4'),
        'AP3.4_B2_ER14_HL'   : 27, #dummy
        'AP3.4_B2_ER14_HC '  : 27 #dummy
    }
    spec = spec_dict[comment_spec] if comment_spec in spec_dict.keys() else getIOP25Spec(ap_rev)
    return spec

def getHC(cms_config):
    hc_val = cms_config[12:14]
    return "HL" if hc_val=="H_" else "HC"

def sigfigs(num,precision):
    digit = num
    if not math.isnan(num):
        digit = round(num, -int(floor(log10(num))) + (precision - 1))
        digit = np.float64(digit)
    return digit

def getFixedBeta(hamr_design):
    fb_dict = {
        'BOB25A.2' : 1.5,
        'BOB25A.3' : 0.7,
        'BOB25B'   : 1.3,
        'BOB28A.1' : 1.3,
        'BOB28A.2' : 1.5,
        'BOB28A.3' : 3.0,
        'BOB28A.6' : 3.0,
        'FDR31.2'  : 1.0,
        'FDR31.4'  : 2.0,
        'PHX7.1'   : 0.7,
        'PHX7.2C'  : 0.7,
        'PHX9.1'   : 1.5,
        'PHX9.1C2' : 1.5,
        'PHX9.1C3' : 1.5,
        'PHX9.1.3' : 1.5,
        'BOB34B2'  : 3.0,
        'BOB34B.2' : 3.0
    }

    fixed_beta = fb_dict[hamr_design] if hamr_design in fb_dict.keys() else 1.5
    return fixed_beta

def insert_error_tracker(msg,app=cfg.APP_NAME,alert_flag=1,insert_time=dt.datetime.now()):
    conn = Oracle_DB_Connection(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
    sql_query = ('INSERT INTO HAMR_IDA_RW.IDA_PROCESS_ERROR_TRACKER VALUES (:app,:msg,:insert_time,:alert)')
    # .format(app=app,msg=msg,date=insert_time,alert=alert_flag)
    params = [app,msg,insert_time,alert_flag]
    conn.insert(sql_query,params)
    conn.close()


def upsert_to_edw_prep(df,schema,table):
    """1. Get all available heads from edw table"""
    temp_df = df.copy()
    temp_df['SQL_SBR_GROUP'] = "'"+temp_df['SBR_GROUP']+"'"
    temp_df['SQL_SN'] = "'"+temp_df['DRIVE_SERIAL_NUM']+"'"
    sbr_text = ",".join(map(str,temp_df['SQL_SBR_GROUP'].unique()))
    sn_text = ", 0), (".join(map(str,temp_df['SQL_SN'].unique()))
    
    edw_query = f"""select 
                        sbr_group,drive_serial_num,hd_lgc_psn 
                    from 
                        {schema}.{table} 
                    where 
                        sbr_group in ({sbr_text}) and (drive_serial_num,0) in (({sn_text}, 0))
                 """
    reli_df = None
    try:
        nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
        reli_df = nrm_ifc.read(edw_query)
        reli_df.columns = reli_df.columns.str.upper()
        logger.info(f"Reading {table} data into EDW database successfully completed")
    except Exception as ex:
        logger.error(f"Error occured while reading {table} table data in EDW",exc_info=True)
        insert_error_tracker(f"Error occured while reading {table} table data in EDW")
    
    
    if reli_df.shape[0]==0:
        df['STATUS'] = 'INSERT'
    else:
        reli_df['STATUS'] = 'UPDATE'
        df = df.merge(reli_df,how='left',left_on=['DRIVE_SERIAL_NUM','SBR_GROUP','HD_LGC_PSN'],right_on=['DRIVE_SERIAL_NUM','SBR_GROUP','HD_LGC_PSN'])
        df['STATUS'] = df['STATUS'].fillna('INSERT')
    cols_to_rename = {
        'INSERTED_DATE_TIME':'IDA_DATE'
    }
    df = df.rename(columns=cols_to_rename)


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
            print(len(params[0]),len(cols_list))
            print(edw_query)
            #edw_query = edw_query.replace(' ', '')
            nrm_ifc.batch_insert(edw_query,params)
            logger.info(f"Inserting {schema}.{table} data into EDW database successfully completed")
        except Exception as ex:
            logger.error("Error occured while inserting {table} table data in EDW".format(table=table),exc_info=True)
            insert_error_tracker("Error occured while inserting {table} table data in EDW".format(table=table))

    """3. Update to EDW"""
    if df_update.shape[0]>0:
        logger.info("Updating the existing heads")
        
        df_update = df_update.drop(columns = ['STATUS'], axis = 1)
        print(df_update.columns)
        try:
            df_update = df_update.drop(['LAST_UPDATE_DATE','IDA_DATE'],axis=1) #,'LAST_UPDATE_DATE'
        except:
            pass
        df_update = df_update.where(pd.notnull(df_update), None) #convert nan values to oracle null before insert
        cols_list = df_update.columns
        # update_list = []
        count=0
        for index,row in df_update.iterrows():
            sbr = df_update['SBR_GROUP'].iloc[count]
            sn = df_update['DRIVE_SERIAL_NUM'].iloc[count]
            hd = df_update['HD_LGC_PSN'].iloc[count]
            edw_query = f"""UPDATE {schema}.{table} SET """
            params = []
            count+=1
            for idx,cols in enumerate(cols_list):
                # params.append(row[cols])
                if not idx == len(cols_list)-1:
                    # print(type(row[cols]))
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"',"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '"+ str(row[cols])+"'"+","
                        else:
                            edw_query +=cols+'='+str(row[cols])+","
                    # else:
                    #     edw_query +=cols+'='+row[cols]+","
                else:
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"'"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '" + str(row[cols])+"'"
                        else:
                            edw_query +=cols+'='+str(row[cols])
                    # else:
                    #     edw_query +=cols+'='+row[cols]
                    # edw_query +=cols+'='+str(row[cols])
            if edw_query.endswith(","):
                edw_query = edw_query[:-1]
            edw_query +=f" WHERE SBR_GROUP = '{sbr}' AND DRIVE_SERIAL_NUM = '{sn}' AND HD_LGC_PSN = {hd}"
            try:
                nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
                # params = list(df_update.itertuples(index=False,name=None))
                params=list(tuple(params))
                # print(len(params[0]),len(cols_list))
                # print(edw_query)
                nrm_ifc.insert(edw_query,params)
                logger.info("Updating {table} data into EDW database successfully completed".format(table=table))
            except Exception as ex:
                logger.error("Error occured while Updating {table} table data in EDW".format(table=table),exc_info=True)
                insert_error_tracker("Error occured while Updating {table} table data in EDW".format(table=table))


def upsert_to_ods_prep(df,schema,table):
    """1. Get all available heads from ODS table"""
    temp_df = df.copy()
    temp_df['SQL_SBR_GROUP'] = "'"+temp_df['SBR_GROUP']+"'"
    temp_df['SQL_SN'] = "'"+temp_df['DRIVE_SERIAL_NUM']+"'"
    sbr_text = ",".join(map(str,temp_df['SQL_SBR_GROUP'].unique()))
    sn_text = ", 0), (".join(map(str,temp_df['SQL_SN'].unique()))
    
    edw_query = f"""select 
                        sbr_group,drive_serial_num,hd_lgc_psn 
                    from 
                        {schema}.{table} 
                    where 
                        sbr_group in ({sbr_text}) and (drive_serial_num,0) in (({sn_text},0))
                 """
    reli_df = None
    try:
        nrm_ifc = DAO_Interface(cfg.orc_ods_username,cfg.orc_ods_passwd,cfg.orc_ods_hostname,cfg.orc_ods_sid,cfg.orc_ods_port,cfg.orc_ods_encoding)
        reli_df = nrm_ifc.read(edw_query)
        reli_df.columns = reli_df.columns.str.upper()
        logger.info(f"Reading {table} data into ODS database successfully completed")
    except Exception as ex:
        logger.error(f"Error occured while reading {table} table data in ODS",exc_info=True)
        insert_error_tracker(f"Error occured while reading {table} table data in ODS")
    
    if reli_df.shape[0]==0:
        df['STATUS'] = 'INSERT'
    else:
        reli_df['STATUS'] = 'UPDATE'
        df = df.merge(reli_df,how='left',left_on=['DRIVE_SERIAL_NUM','SBR_GROUP','HD_LGC_PSN'],right_on=['DRIVE_SERIAL_NUM','SBR_GROUP','HD_LGC_PSN'])
        df['STATUS'] = df['STATUS'].fillna('INSERT')
    cols_to_rename = {
        'INSERTED_DATE_TIME':'IDA_DATE'
    }
    df = df.rename(columns=cols_to_rename)


    """2. Split into insert and update dataframes"""
    df_insert = df[df['STATUS']=="INSERT"]
    df_update = df[df['STATUS']=="UPDATE"]

    """3. Insert to ODS"""
    if df_insert.shape[0]>0:
        logger.info("Inserting the new heads")
        df_insert = df_insert.drop(['STATUS','LAST_UPDATE_DATE'],axis=1)
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
            nrm_ifc = DAO_Interface(cfg.orc_ods_username,cfg.orc_ods_passwd,cfg.orc_ods_hostname,cfg.orc_ods_sid,cfg.orc_ods_port,cfg.orc_ods_encoding)
            params = list(df_insert.itertuples(index=False,name=None))
            print(len(params[0]),len(cols_list))
            # print(edw_query)
            nrm_ifc.batch_insert(edw_query,params)
            logger.info("Inserting {table} data into ODS database successfully completed".format(table=table))
        except Exception as ex:
            logger.error("Error occured while inserting {table} table data in ODS".format(table=table),exc_info=True)
            insert_error_tracker("Error occured while inserting {table} table data in ODS".format(table=table))

    """4. Update to ODS"""
    if df_update.shape[0]>0:
        logger.info("Updating the existing heads")
        df_update = df_update.drop(['STATUS'], axis = 1)
        try:
            df_update = df_update.drop(['LAST_UPDATE_DATE','IDA_DATE'],axis=1) #,'LAST_UPDATE_DATE'
        except:
            pass
        df_update = df_update.where(pd.notnull(df_update), None) #convert nan values to oracle null before insert
        cols_list = df_update.columns
        # update_list = []
        count=0
        for index,row in df_update.iterrows():
            sbr = df_update['SBR_GROUP'].iloc[count]
            sn = df_update['DRIVE_SERIAL_NUM'].iloc[count]
            hd = df_update['HD_LGC_PSN'].iloc[count]
            edw_query = f"""UPDATE {schema}.{table} SET """
            params = []
            count+=1
            for idx,cols in enumerate(cols_list):
                # params.append(row[cols])
                if not idx == len(cols_list)-1:
                    # print(type(row[cols]))
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"',"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '"+ str(row[cols])+"'"+","
                        else:
                            edw_query +=cols+'='+str(row[cols])+","
                    # else:
                    #     edw_query +=cols+'='+row[cols]+","
                else:
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"'"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '" + str(row[cols])+"'"
                        else:
                            edw_query +=cols+'='+str(row[cols])
                    # else:
                    #     edw_query +=cols+'='+row[cols]
                    # edw_query +=cols+'='+str(row[cols])
            if edw_query.endswith(","):
                edw_query = edw_query[:-1]
            edw_query +=f" WHERE SBR_GROUP = '{sbr}' AND DRIVE_SERIAL_NUM = '{sn}' AND HD_LGC_PSN = {hd}"
            try:
                nrm_ifc = DAO_Interface(cfg.orc_ods_username,cfg.orc_ods_passwd,cfg.orc_ods_hostname,cfg.orc_ods_sid,cfg.orc_ods_port,cfg.orc_ods_encoding)
                # params = list(df_update.itertuples(index=False,name=None))
                params=list(tuple(params))
                # print(len(params[0]),len(cols_list))
                # print(edw_query)
                nrm_ifc.insert(edw_query,params)
                logger.info("Updating {table} data into ODS database successfully completed".format(table=table))
            except Exception as ex:
                logger.error("Error occured while Updating {table} table data in ODS".format(table=table),exc_info=True)
                insert_error_tracker("Error occured while Updating {table} table data in ODS".format(table=table))


def upsert_to_mysql_prep(df,schema,table):
    """1. Get all available heads from MySQL table"""
    df=df.astype(str)
    temp_df = df.copy()
    temp_df['SQL_SBR_GROUP'] = "'"+temp_df['SBR_GROUP']+"'"
    temp_df['SQL_SN'] = "'"+temp_df['DRIVE_SERIAL_NUM']+"'"
    sbr_text = ",".join(map(str,temp_df['SQL_SBR_GROUP'].unique()))
    sn_text = ",".join(map(str,temp_df['SQL_SN'].unique()))
    
    edw_query = f"""select 
                        sbr_group,drive_serial_num,hd_lgc_psn 
                    from 
                        {schema}.{table} 
                    where 
                        sbr_group in ({sbr_text}) and drive_serial_num in ({sn_text})
                 """
    reli_df = pd.DataFrame()
    try:
        nrm_ifc = MySQL_DB_Connection(cfg.mys_username,cfg.mys_passwd,cfg.mys_hostname,cfg.mys_db_name)
        reli_df = nrm_ifc.retrieveAsDF(edw_query)
        if reli_df == None:
            print('hhhhhhhhhhhhhhhheeeeeeeeeeeeeeeeeeeyyyyyyyyyyyyyyyy')
        reli_df.columns = reli_df.columns.str.upper()
        print(reli_df.columns)
        logger.info(f"Reading {table} data into MySQL database successfully completed")
    except Exception as ex:
        logger.error(f"Error occured while reading {table} table data in MySQL",exc_info=True)
        insert_error_tracker(f"Error occured while reading {table} table data in MySQL")
    #logging.info(f'The columns in reli_df are {reli_df.columns}')
    print('Reli DF is ', reli_df)
    #reli_df = reli_df.columns.str.upper()
    if reli_df.shape[0]==0:
        df['STATUS'] = 'INSERT'
    else:
        reli_df['STATUS'] = 'UPDATE'
        print(df.columns, reli_df.columns)
        df = df.merge(reli_df,how='left',left_on=['DRIVE_SERIAL_NUM','SBR_GROUP','HD_LGC_PSN'],right_on=['DRIVE_SERIAL_NUM','SBR_GROUP','HD_LGC_PSN'])
        df['STATUS'] = df['STATUS'].fillna('INSERT')
    cols_to_rename = {
        'INSERTED_DATE_TIME':'IDA_DATE'
    }
    df = df.rename(columns=cols_to_rename)


    """2. Split into insert and update dataframes"""
    df_insert = df[df['STATUS']=="INSERT"]
    df_update = df[df['STATUS']=="UPDATE"]
    print(df)
    """3. Insert to MySQL"""
    if df_insert.shape[0]>0:
        logger.info("Inserting the new heads")
        df_insert = df_insert.drop(['STATUS'],axis=1)
        # df_insert = df_insert.where(pd.notnull(df_insert), None) #convert nan values to oracle null before insert
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
                # edw_query +=':'+str(cols)+','
                # if cols in ['ACID_ITH_START_MAIN_MA','ACID_ITH_END_MA','ACID_MAX_MAG_START']:
                #     # df_insert.loc[df_insert.cols.isin(['None']), cols] = None
                #     # df_insert[cols] = df_insert[cols].str.replace('None',None)
                #     df_insert[cols] = df_insert[cols].astype(float)
                edw_query +='%s'+','
            else:
                # edw_query +=':'+str(cols)
                edw_query +='%s'
        edw_query +=  ')'
        logger.info(f'The edw query is {edw_query}')

        try:
            nrm_ifc = MySQL_DB_Connection(cfg.mys_username,cfg.mys_passwd,cfg.mys_hostname,cfg.mys_db_name)
            params = list(df_insert.itertuples(index=False,name=None))
            print(params)
            print(len(params[0]),len(cols_list))
            # print(params)
            # print(edw_query)
            nrm_ifc.batch_insert(edw_query,params)
            logger.info("Inserting {table} data into MYSQL database successfully completed".format(table=table))
        except Exception as ex:
            logger.error("Error occured while inserting {table} table data in MYSQL".format(table=table),exc_info=True)
            insert_error_tracker("Error occured while inserting {table} table data in MYSQL".format(table=table))

    """4. Update to MYSQL"""
    if df_update.shape[0]>0:
        logger.info("Updating the existing heads")
        df_update = df_update.drop(['STATUS'], axis = 1)
        try:
            df_update = df_update.drop(['SQL_SN', 'SQL_SBR_GROUP'], axis = 1)
        except:
            pass
        try:
            df_update = df_update.drop(['IDA_DATE'],axis=1) #,'LAST_UPDATE_DATE'
        except:
            pass
        df_update = df_update.where(pd.notnull(df_update), None) #convert nan values to oracle null before insert
        cols_list = df_update.columns
        # update_list = []
        count=0
        for index,row in df_update.iterrows():
            sbr = df_update['SBR_GROUP'].iloc[count]
            sn = df_update['DRIVE_SERIAL_NUM'].iloc[count]
            hd = df_update['HD_LGC_PSN'].iloc[count]
            edw_query = f"""UPDATE {schema}.{table} SET """
            params = []
            count+=1
            for idx,cols in enumerate(cols_list):
                # params.append(row[cols])
                if not idx == len(cols_list)-1:
                    # print(type(row[cols]))
                    if(row[cols]!=None):
                        edw_query +=cols+"='"+str(row[cols]).replace("'", "")+"',"
                        # elif 'Timestamp' in str(type(row[cols])):
                        #     edw_query +=cols+"="+"TIMESTAMP '"+ str(row[cols])+"'"+","
                        # else:
                        #     edw_query +=cols+'='+str(row[cols])+","
                    # else:
                    #     edw_query +=cols+'='+row[cols]+","
                else:
                    if(row[cols]!=None):
                        # if(isinstance(row[cols], str)):
                        edw_query +=cols+"='"+str(row[cols])+"'"
                        # elif 'Timestamp' in str(type(row[cols])):
                        #     edw_query +=cols+"="+"TIMESTAMP '" + str(row[cols])+"'"
                        # else:
                        #     edw_query +=cols+'='+str(row[cols])
                    # else:
                    #     edw_query +=cols+'='+row[cols]
                    # edw_query +=cols+'='+str(row[cols])
            if edw_query.endswith(","):
                edw_query = edw_query[:-1]
            edw_query +=f" WHERE SBR_GROUP = '{sbr}' AND DRIVE_SERIAL_NUM = '{sn}' AND HD_LGC_PSN = {hd}"
            logger.info(f'The edw query is {edw_query}')
            try:
                nrm_ifc = MySQL_DB_Connection(cfg.mys_username,cfg.mys_passwd,cfg.mys_hostname,cfg.mys_db_name)
                # params = list(df_update.itertuples(index=False,name=None))
                params=list(tuple(params))
                # print(len(params[0]),len(cols_list))
                # print(edw_query)
                nrm_ifc.insert(edw_query,params)
                logger.info("Updating {table} data into MySQL database successfully completed".format(table=table))
            except Exception as ex:
                logger.error("Error occured while Updating {table} table data in MySQL".format(table=table),exc_info=True)
                insert_error_tracker("Error occured while Updating {table} table data in MySQL".format(table=table))

def logExists(log_path):
    if os.path.exists(log_path):
        return True
    else:
        return False

def getLogRepo(repo_type):
    if not repo_type.upper() in cfg.repo_source_path.keys():
        return cfg.repo_source_path['DEFAULT']
    else:
        return cfg.repo_source_path[repo_type.upper()]


def getLogTable(sbr,sn,table, queue):
    '''
      Log Data Tables and their IDs:
        -------------------------------------------
        |         TABLE NAME        |   TABLE ID  |
        -------------------------------------------
        |    P_LIFE_TEST_CYCLE_D3   |      03     |
        |   P_LIFE_TEST_SUMMARY_D3  |      13     |
        |  P_DVL_812_LT_SQZ_REF_TUB |      07     |
        |  P_DVL_815_BER_VS_IOP_DAC |      10     |
        -------------------------------------------
        
      Log File Path Template based on repo
         1. Reli Path : /extrastg/mdfs14/hamr-analysis/log/ALL_DRIVE/sn/<SBR-SN>/output_combine/<SBR-SN-TableID>.csv
         2. One clicker(OC) : /mnt/tep/WDVL/TEST_SERVER/CLICKER_PROJECT/DATA_LIVE/<SBR>/<table_name>/DRIVE/<table_name.csv>
    '''
    repo_path = getLogRepo(cfg.repo_flag) + queue + '/'
    if 'RELI' in cfg.repo_flag.upper():
        sbr_sn = sbr+'-'+sn
        table_id = cfg.relia_log_table_ids[table]
        repo_path = repo_path+sbr_sn+'/output-combine/'+sbr_sn+'-'+table_id+'.csv'
        return repo_path
    elif 'OC' in cfg.repo_flag.upper():
        pass
    else:
        pass
