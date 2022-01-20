from calendar import monthrange
import mysql.connector
import pandas as pd


class Mod:
    def __init__(self, ss_id, year, month):
        self.sql_db = mysql.connector.connect(
            host='127.0.0.1',
            user="root",
            password="por1BABU",
            database="ois_db"
        )
        self.ss_id, self.year, self.month = [ss_id, year, month]
        self.day = monthrange(year, month)[1]  # last day fo month
        self.date_time = f'{year}-{month}-{self.day}'

        query_gmd_ss = f"""
            SELECT gmd.name AS gmd, ss.name AS ss 
            FROM substation AS ss
            JOIN gmd ON ss.gmd = gmd.id
            WHERE ss.id = {self.ss_id}         
        """
        gmd_ss_df = pd.read_sql_query(query_gmd_ss, self.sql_db)
        self.gmd_name, self.ss_name = gmd_ss_df.iloc[0, :]
