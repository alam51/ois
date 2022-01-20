import mysql.connector
import pandas as pd
import openpyxl as xl  # used by pandas to write excel file
from calendar import monthrange

"""Input values. May be dynamically provided as per user request"""
year = 2021
month = 7
ss_id = 5

"""connector is to be configured in django"""
mydb = mysql.connector.connect(
    host='127.0.0.1',
    user="root",
    password="por1BABU",
    database="ois",
    port="3306"
)
"""***End of input***"""

day = monthrange(year, month)[1]  # last day of month
# day = 1  # first day fo month
date_time = f'{year}-{month}-{day}'
# query to find total kwh of main transformers' low side
query_string_kwh = f"""
-- USE pgcb_substation_logsheet;

-- SELECT * FROM substation_equipment se
SELECT tr.name, e.date_time, e.value, e.is_export, e.is_import, e.is_kwh, e.is_kvarh, tr.is_auxiliary, 
se.is_transformer_low
-- SELECT *
FROM substation_equipment se
JOIN transformer tr ON se.transformer_id = tr.id
-- 
JOIN energy e ON se.id = e.sub_equip_id 
WHERE e.date_time = '{date_time}' AND tr.substation_id = '{ss_id}' AND e.is_kwh=1 
AND tr.is_auxiliary = 0 AND se.is_transformer_low = 1;
"""

# query to find total kvarh of main transformers' low side
query_string_kvarh = f"""
-- USE pgcb_substation_logsheet;

-- SELECT * FROM substation_equipment se
SELECT tr.name, e.date_time, e.value, e.is_export, e.is_import, e.is_kwh, e.is_kvarh, tr.is_auxiliary, 
se.is_transformer_low
-- SELECT *
FROM substation_equipment se
JOIN transformer tr ON se.transformer_id = tr.id
-- 
JOIN energy e ON se.id = e.sub_equip_id 
WHERE e.date_time = '{date_time}' AND tr.substation_id = '{ss_id}' AND e.is_kvarh=1 
AND tr.is_auxiliary = 0 AND se.is_transformer_low = 1;
"""
# query_string1 = 'SELECT * FROM substation_equipment'
# query_path = r'G:\My Drive\my_works\SQL\pgcb_ss_logsheet_pf.sql'
# query = open(query_path, 'r').read()
kwh_df = pd.read_sql_query(query_string_kwh, mydb)
kvarh_df = pd.read_sql_query(query_string_kvarh, mydb)

""""Write pd dataframe in excel (for debugging purpose)"""
kwh_df.to_excel('kwh.xlsx')
kvarh_df.to_excel('kvarh.xlsx')

kwh = sum(kwh_df['value'])
kvarh = sum(kvarh_df['value'])
kvah = (kwh**2 + kvarh**2) ** .5
if kvah != 0:
    pf = kwh / kvah
else:
    pf = 0

""""pf of the S/S found. may be used for further proceedings"""
print(pf)
