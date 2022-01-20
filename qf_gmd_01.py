# from calendar import monthrange
"""
mysql.connector.connect may be replaced by djnago.connector (something like that)
"""
from mysql.connector import connect as connector
import pandas as pd


class DbConnector:
    def __init__(self):
        """
        sql connector class
        """
        self.sql_db = connector(
            host='127.0.0.1',
            user="root",
            password="por1BABU",
            database="ois"
        )


class Query(DbConnector):
    def __init__(self, ss_id, mw_mvar_kwh_kvarh_kv_table_name, time_start, time_end, equipment_type=None):
        super().__init__()
        query_str = f"""
        SELECT xy.date_time, se.id as equipment_id, xy.value
        FROM substation_equipment AS se
        JOIN {mw_mvar_kwh_kvarh_kv_table_name} AS xy ON xy.sub_equip_id = se.id
        WHERE se.substation_id = {ss_id} AND
        xy.date_time BETWEEN '{time_start}' AND '{time_end}'       
        """
        b = 5
        self.reading_df = pd.read_sql_query(query_str, self.sql_db)


class Equipment(DbConnector):
    def __init__(self, ss_id, time_start, time_end):
        super().__init__()
        self.voltage_db = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='voltage',
                                time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        self.mw_db = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='mega_watt',
                           time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        self.mvar_db = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='mega_var',
                             time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        self.kwh_db = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='energy',
                            time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        equipment_list_query_str = f"""
                                SELECT *
                                -- SELECT se.id as equipment_id, se.name
                                FROM substation_equipment AS se
                                WHERE se.substation_id = {ss_id}
                                """
        self.equipment_list_df = pd.read_sql_query(equipment_list_query_str, self.sql_db)

        def equipment_id_name_type(is_equipment_str):
            df1 = self.equipment_list_df[self.equipment_list_df[is_equipment_str] == 1]. \
                      loc[:, ['id', 'name']]
            type_list = [is_equipment_str[3:] for i in range(df1.shape[0])]
            df1.loc[:, 'equipment_type'] = type_list
            return df1

        # bus = self.equipment_list_df[self.equipment_list_df['is_bus'] == 1].loc[:, ['id', 'name']]
        # transformer = self.equipment_list_df[self.equipment_list_df['is_transformer'] == 1].loc[:, ['id', 'name']]
        # transformer_high = self.equipment_list_df[self.equipment_list_df['is_transformer_high'] == 1].loc[:,
        bus = equipment_id_name_type('is_bus')
        transformer_high = equipment_id_name_type('is_transformer_high')
        transformer_low = equipment_id_name_type('is_transformer_low')
        transformer = equipment_id_name_type('is_transformer')
        line = equipment_id_name_type('is_line')
        feeder = equipment_id_name_type('is_feeder')
        bank = equipment_id_name_type('is_bank')
        breaker = equipment_id_name_type('is_breaker')
        lvac = equipment_id_name_type('is_lvac')
        self.equipment_id_name_type_df = pd.concat([
            bus,
            transformer_high,
            transformer_low,
            transformer,
            line,
            feeder,
            bank,
            breaker,
            lvac
        ])


equipment = Equipment(21, '2021-8-11', '2021-8-12')
b = 4
