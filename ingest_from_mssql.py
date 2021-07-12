import pymssql
import pandas as pd
import numpy as np
import time
from decouple import config
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


######### -- Get MODIFIEDON of latest record from Snowflake SOURCE.CANDIDATE.LEAD
snowflake_conn = snowflake.connector.connect(
    user=config("SNOWFLAKE_USER"),
    password=config("SNOWFLAKE_PASS"),
    account=config("SNOWFLAKE_ACCOUNT"),
    role='loader',
    database='source',
    schema='candidate',
    warehouse='loading_wh'
    )
snowflake_cs = snowflake_conn.cursor()
try:
    snowflake_cs.execute("SELECT max(modifiedon) from source.candidate.lead")
    one_row = snowflake_cs.fetchone()
    print(one_row[0])
except:
    print("Snowflake error")
snowflake_cs.close()
##########



######### -- Get all records w/ MODIFIEDON > latest Snowflake record from MSSQL candidate3.dbo.Lead
sql_statement = "select * from Lead where convert(varchar,modifiedon,120) > \'" + str(one_row[0])+"\'"
print(sql_statement)

server = config('MSSQL_SERVER')
user = config('MSSQL_USER')
password = config('MSSQL_PASS')

sql_conn = pymssql.connect(server, user, password, "candidate3")

sql_cs = sql_conn.cursor()
sql_cs.execute(sql_statement)

#########

######### -- Insert records 10000 records at a time into Snowfalke SOURCE.CANDIDATE.LEAD_IMPORT_TEST
df_cols = [col[0] for col in sql_cs.description]

#Fetching from SQL cursor
start = time.time()
rows = sql_cs.fetchmany(10000)
end = time.time()

print("Fetching rows from SQL took " + str(end-start)+" seconds")

i = 0
while rows:
    print("batch", str(i))
    #Creating DataFrame
    start = time.time()
    df = pd.DataFrame(rows, columns = df_cols)

    df['ModifiedOn'] = pd.to_datetime(df['ModifiedOn'],utc=True)
    df['CreatedOn'] = pd.to_datetime(df['CreatedOn'],utc=True)

    end = time.time()

    print("DF creation took " + str(end-start) + " seconds")

    #Writing to Snowflake
    start = time.time()
    write_pandas(snowflake_conn, df, "LEAD_IMPORT_TEST")
    end = time.time()
    
    print("Writing to Snowflake took " + str(end-start)+" seconds")

    #Fetching from SQL cursor
    start = time.time()
    rows = sql_cs.fetchmany(10000)
    end = time.time()
    
    print("Fetching rows from SQL took " + str(end-start)+" seconds")

    i+=1
    print("done")


snowflake_conn.close()
sql_conn.close()

