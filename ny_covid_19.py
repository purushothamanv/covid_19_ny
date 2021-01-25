##========================================================
# COVID-19 NY data load by counties 
##========================================================

import pandas as pd
import urllib3 , json
from fast_to_sql import fast_to_sql as fts
import os
import re
import pyodbc
import logging
import concurrent.futures
import time
import schedule
import certifi
http = urllib3.PoolManager(
       cert_reqs='CERT_REQUIRED',
       ca_certs=certifi.where())

os.chdir('E:\\python_files\\covid_ny_data')
#os.getcwd()

logging.basicConfig(filename="E:\\python_files\\covid19.log", 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    filemode='w',
                   level=logging.INFO)
#logger=logging.getLogger(__name__) 


def covid_ny_data():
    url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
    r = http.request('GET',url)
    if r.status != 200: #200 means Successful responses
        logger.error('API is not respond successfuly')
        print(r.status, 'API is not respond successfuly')
    else:
        try:
            # decode json data into a dict object
            data = json.loads(r.data.decode('utf-8'))
            #print(data.keys())

            #Normalize Dict to Pandas DataFrame
            df = pd.json_normalize(data,'data')
            df_select = df.iloc[:,8:]
            old_names = df_select.columns
            new_names = ['test_date', 'county','new_positives', 'cumulative_number_of_positives', 'total_number_of_tests', 'cumulative_number_of_tests']
            df_select.rename(columns=dict(zip(old_names,new_names)),inplace=True)
            #df_select.head(10)
            return df_select
        except Exception as err:
            logging.error("Error in covid_ny_data: " + str(err))
            print("Error in covid_ny_data: " + str(err))

def load_sql(df,county_name):
    try:
        #global conn
        table_name = re.sub('[^A-Za-z]+','_',county_name) + "_covid"
        # Create a pyodbc connection
        conn = pyodbc.connect(
            """
            Driver={ODBC Driver 13 for SQL Server};
            Server=SQL_server;
            Database=DB;
            Trusted_Connection=yes;
            """
        )
        df['load_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
        create_statement = fts.fast_to_sql(df
                                           , table_name
                                           , conn
                                           , if_exists="append"
                                           , custom={"test_date":"date", "new_positives":"int", "cumulative_number_of_positives" : "int", "total_number_of_tests":"int", "cumulative_number_of_tests":"int", "load_date":"date default getdate()"}
                                           , temp=False)
        #print(create_statement)
        # Commit upload actions and close connection
        conn.commit()
        conn.close()
    except Exception as err:
        logging.error("Error in load_sql: " + str(err))
        print("Error in load_sql: " + str(err))
    
def covid_ny_data_by_county(county_name):
    try:
        #print('module name:', __name__)
        print('parent process:', os.getppid())
        print('process id:', os.getpid())
        df_covid = covid_ny_data()
        df_by_county = df_covid[df_covid['county'] == county_name]
        #df_select.dtypes
        pd.set_option('mode.chained_assignment', None)
        df_by_county['test_date'] = df_by_county['test_date'].str[:10]
        #df_by_county['test_date'] = df_by_county.test_date.map(lambda td: td.str[:10])
        df_final = df_by_county[['test_date','new_positives','cumulative_number_of_positives','total_number_of_tests','cumulative_number_of_tests']]
        #df_final.head(10)
        
        load_sql(df_final,county_name)
        
        #df_final['load_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
        df_final.to_csv(county_name+'.csv',mode = 'a', header=True,index=False)
        
        
        d = "Covid Data load is completed for the county: " + county_name
        logging.info(d)
        print(d)
        
    except Exception as err:
        logging.error("Error in covid_ny_data_by_county: " + str(err))
        print("Error in covid_ny_data_by_county: " + str(err))

def main(): 
    try:
        if __name__ == '__main__':
            #county_names = ['Albany', 'Allegany','St. Lawrence','New York']
            county_names = list(covid_ny_data().county.unique())
            start = time.perf_counter()
            
            with concurrent.futures.ThreadPoolExecutor() as exe:
                exe.map(covid_ny_data_by_county,county_names)
            
            finish = time.perf_counter()
            value = f'finished in {round(finish-start,2)} second(s)'
            print(value)
            logging.info(value)
    except Exception as err:
        logging.error("Error in main : " + str(err))
        print("Error in main : " + str(err))

##========================================================        
#main()

print("schedule job started")
#schedule.every(10).seconds.do(main)
schedule.every().day.at("09:00").do(main)
while 1:
    schedule.run_pending()
    time.sleep(1)
##========================================================	