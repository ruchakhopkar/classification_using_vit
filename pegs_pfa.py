
from dbs import *
from peg_only_pfa import *
from post_processing import *
def job():
    
    try:    
        source_df = getInputData('enseapedia', 'hamr_pfa_ai', 'hamr_ida_rw', 'peg_deformation_pfa', ['jmp.head_serial_num', 'jmp.peg_from_core_url_bb', 'jmp.peg_sem_date', 'cdsem.peg_classification'], head_column = 'HEAD_SERIAL_NUM', \
                                  link_col = 'PEG_FROM_CORE_URL_BB', source_date_col =  'PEG_SEM_DATE')
        source_df = source_df[source_df['PEG_CLASSIFICATION'].isna()]
        if len(source_df)>0:
            
            images, hd_num, dates, wafercode, wafer_substrate_lot_id = cleanData(source_df)
            
            storeData(images, hd_num)
            
            runModels()
            
            df = storeResults(dates, hd_num, images, wafercode, wafer_substrate_lot_id)
            missing_data, df = dqa()
            updated_data(df)
            send_email(df = missing_data)
        print('Run Completed')
    except Exception as e:
        send_email(e, flag=1)
        
    

import time
import schedule

schedule.every(1).hours.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
    

                                                                                                                                                                                                       
