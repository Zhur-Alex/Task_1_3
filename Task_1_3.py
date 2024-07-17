from pyparsing import Char
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from datetime import datetime
from time import sleep
import pandas as pd 
from sqlalchemy import Table, Column, Integer, String, Date, VARCHAR, CHAR, Numeric, Float, MetaData, Text, Sequence

engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')



#---------------------Функции для логирования#---------------------

from functools import wraps

def task_log_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_date_launche = datetime.today()
        task_name = func.__name__
        run_id = 1
        time_start = datetime.now()
        more_info = 'all_good'
        result = 'SUCCESS!'
        
        try:
            func(*args, **kwargs)
        except Exception as ex:
            result = 'FAIL...'
            more_info = ex
            print(f"ERROR: {ex}")
        sleep(1)
        time_end = datetime.now()
        time_duration = (time_end - time_start).total_seconds()
        
        columns = ['row_timestamp', 'task_name', 'run_id', 'time_start', 'time_end', 'duration_sec', 'result', 'info']
    
        cort = (task_date_launche, task_name, run_id, time_start, time_end, time_duration, result, more_info)

        df_start = pd.DataFrame([cort], columns=columns)
        df_start.to_sql(name='tasks_log', con=engine, schema='logs', if_exists='append', index=False)
    return wrapper       
 
#------------------------Создание_таблиц-------------------------

metadataobj_v2 = MetaData(schema = 'dm')

dm_f101_round_f_table_v2 = Table(
    "dm_f101_round_f_v2",
    metadataobj_v2,
    Column("from_date", Date), 
    Column("to_date", Date), 
    Column("chapter", CHAR), 
    Column("ledger_account", VARCHAR(5), primary_key=True), 
    Column("characteristic", CHAR), 
    Column("balance_in_rub", Numeric(23,8)), 
    Column("r_balance_in_rub", Numeric(23,8)), 
    Column("balance_in_val", Numeric(23,8)), 
    Column("r_balance_in_val", Numeric(23,8)), 
    Column("balance_in_total", Numeric(23,8)), 
    Column("r_balance_in_total", Numeric(23,8)), 
    Column("turn_deb_rub", Numeric(23,8)), 
    Column("r_turn_deb_rub", Numeric(23,8)), 
    Column("turn_deb_val", Numeric(23,8)), 
    Column("r_turn_deb_val", Numeric(23,8)), 
    Column("turn_deb_total", Numeric(23,8)), 
    Column("r_turn_deb_total", Numeric(23,8)), 
    Column("turn_cre_rub", Numeric(23,8)), 
    Column("r_turn_cre_rub", Numeric(23,8)), 
    Column("turn_cre_val", Numeric(23,8)), 
    Column("r_turn_cre_val", Numeric(23,8)), 
    Column("turn_cre_total", Numeric(23,8)), 
    Column("r_turn_cre_total", Numeric(23,8)), 
    Column("balance_out_rub", Numeric(23,8)), 
    Column("r_balance_out_rub", Numeric(23,8)), 
    Column("balance_out_val", Numeric(23,8)), 
    Column("r_balance_out_val", Numeric(23,8)), 
    Column("balance_out_total", Numeric(23,8)), 
    Column("r_balance_out_total", Numeric(23,8))
)

@task_log_function
def create_dm_f101_round_f_table_v2():
    metadataobj_v2.create_all(engine)	
    
#---------------------Таблица в CSV и наоборот----------------------

@task_log_function
def table_to_csv():
    df = pd.read_sql_table('dm_f101_round_f', con=engine, schema='dm')
    print(df)
    df.to_csv('C:/Users/Александр/Desktop/output.csv', index=False)

@task_log_function
def csv_to_copy_table():
    df_1 = pd.read_csv('C:/Users/Александр/Desktop/output.csv', sep=",")
    print(df_1)
    dtype_date_col = {'from_date': Date,
                  'to_date': Date,
                  'chapter': CHAR,
                  'ledger_account': VARCHAR(5),
                  'characteristic': CHAR,
                  'balance_in_rub': Numeric(23,8),
                  'balance_in_rub': Numeric(23,8), 
                  'balance_in_val': Numeric(23,8), 
                  'r_balance_in_val': Numeric(23,8), 
                  'balance_in_total': Numeric(23,8), 
                  'r_balance_in_total': Numeric(23,8), 
                  'turn_deb_rub': Numeric(23,8), 
                  'r_turn_deb_rub': Numeric(23,8), 
                  'turn_deb_val': Numeric(23,8), 
                  'r_turn_deb_val': Numeric(23,8), 
                  'turn_deb_total': Numeric(23,8), 
                  'r_turn_deb_total': Numeric(23,8), 
                  'turn_cre_rub': Numeric(23,8), 
                  'r_turn_cre_rub': Numeric(23,8), 
                  'turn_cre_val': Numeric(23,8), 
                  'r_turn_cre_val': Numeric(23,8), 
                  'turn_cre_total': Numeric(23,8), 
                  'r_turn_cre_total': Numeric(23,8), 
                  'balance_out_rub': Numeric(23,8), 
                  'r_balance_out_rub': Numeric(23,8), 
                  'balance_out_val': Numeric(23,8), 
                  'r_balance_out_val': Numeric(23,8), 
                  'balance_out_total': Numeric(23,8),
                  'r_balance_out_total': Numeric(23,8)} 
    df_1.to_sql(name='dm_f101_round_f_v2', con=engine, schema='dm', if_exists='replace', index=False, dtype=dtype_date_col)
    
    
create_dm_f101_round_f_table_v2()
#table_to_csv()
csv_to_copy_table()