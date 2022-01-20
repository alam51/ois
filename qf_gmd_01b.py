# from calendar import monthrange
"""
mysql.connector.connect may be replaced by djnago.connector (something like that)
"""
import datetime
import re

from dateutil import relativedelta
from mysql.connector import connect as connector
import pandas as pd
from df_to_html_with_style import Df2HtmlWithStyle


def ht_by_lt_to_ht_or_lt_voltage(df, ht=True):
    """Extracts 230 from 230/132. Could be easy as bus etc if high, low values were stored as int"""
    for i in df.index:
        ht_by_lt_str = df.loc[i, 'voltage_level']
        if ht:
            df.loc[i, 'voltage_level'] = \
                ht_by_lt_str[: ht_by_lt_str.find('/')]  # extract 230 from 230/132
        else:
            df.loc[i, 'voltage_level'] = \
                ht_by_lt_str[ht_by_lt_str.find('/') + 1:]  # extract 132 from 230/132
    return df


class DbConnector:
    def __init__(self):
        """
        sql connector class
        """
        self.sql_db = connector(
            host='127.0.0.1',
            user="root",
            password="por1BABU",
            database="ois_db"
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

        p_q_i = ['mega_watt', 'mega_var', 'Ampere R', 'Ampere Y', 'Ampere B']
        #  equipment type wise reading
        self.reading_dict = {
            '0. Bus': ['voltage'],  # Frequency could be added
            '1. Transformer HT': p_q_i,
            '2. Transformer': ['tap'],  # WT, OT could be added
            '3. Transformer LT': p_q_i,
            '4. Line': p_q_i,
            '5. Feeder': p_q_i,
            '6. Bank': p_q_i,
            '7. Breaker': ['Gas Pressure', 'Air Pressure', 'Spring Status']
        }
        self.voltage_df = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='voltage',
                                time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        self.mw_df = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='mega_watt',
                           time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        self.mvar_df = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='mega_var',
                             time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        self.kwh_df = Query(ss_id=ss_id, mw_mvar_kwh_kvarh_kv_table_name='energy',
                            time_start=time_start, time_end=time_end).reading_df.set_index('date_time')

        """create Ampere df"""
        amp_str = f"""
                SELECT xy.date_time, se.id as equipment_id, xy.R_value, xy.Y_value, xy.B_value
                FROM substation_equipment AS se
                JOIN ampere AS xy ON xy.sub_equip_id = se.id
                WHERE se.substation_id = {ss_id} AND
                xy.date_time BETWEEN '{time_start}' AND '{time_end}'       
                """
        b = 5
        self.amp_df = pd.read_sql_query(amp_str, self.sql_db)

        """create tap df"""
        tap_str = f"""
                    SELECT xy.date_time, se.id as equipment_id, xy.value
                    FROM substation_equipment AS se
                    JOIN tap AS xy ON xy.sub_equip_id = se.id
                    WHERE se.substation_id = {ss_id} AND
                    xy.date_time BETWEEN '{time_start}' AND '{time_end}'       
                    """
        b = 5
        self.tap_df = pd.read_sql_query(tap_str, self.sql_db)

        """create breaker gas pressure, Air pressure, Spring status df"""
        pressure_str = f"""
                    SELECT p.date_time, se.id as equipment_id, p.gas_pressure, p.air_pressure, p.spring_status
                    FROM substation_equipment AS se
                    JOIN breakers AS xy ON xy.id = se.breaker_id
                    JOIN pressure AS p ON p.breaker_no_id = xy.id
                    WHERE se.substation_id = {ss_id} AND
                    p.date_time BETWEEN '{time_start}' AND '{time_end}'           
                    """
        b = 5
        self.pressure_df = pd.read_sql_query(pressure_str, self.sql_db)

        bus_str = f"""
        SELECT equipment_voltage.name AS voltage_level, se.id AS eq_id, se.name AS name,
        '{list(self.reading_dict.keys())[0]}' AS 'type'
        FROM bus
        JOIN substation_equipment AS se ON bus.id = se.bus_id
        JOIN equipment_voltage ON bus.bus_voltage = equipment_voltage.id
        WHERE se.substation_id = {ss_id} AND se.is_bus = 1
        """
        self.bus_df = pd.read_sql_query(bus_str, self.sql_db)

        transformer_high_str = f"""
        SELECT ty.voltage_level, se.id AS eq_id, se.name AS name, 
        '{list(self.reading_dict.keys())[1]}' AS 'type' 
        FROM substation_equipment AS se 
        JOIN transformer AS t ON t.id = se.transformer_id
        JOIN transformer_type AS ty ON t.type_id = ty.id
        -- JOIN equipment_voltage ON bus.bus_voltage = equipment_voltage.id
        WHERE se.substation_id = {ss_id} AND se.is_transformer_high = 1
        """
        self.transformer_high_df1 = pd.read_sql_query(transformer_high_str, self.sql_db)
        # """Could be easy as before(bus) if high, low values were stored as int"""
        # for i in self.transformer_high_df.index:
        #     ht_by_lt_str = self.transformer_high_df.loc[i, 'voltage_level']
        #     self.transformer_high_df.loc[i, 'voltage_level'] = \
        #         ht_by_lt_str[: ht_by_lt_str.find('/')]  # extract 230 from 230/132
        self.transformer_high_df = ht_by_lt_to_ht_or_lt_voltage(self.transformer_high_df1, ht=True)

        transformer_str = f"""
            SELECT ty.voltage_level, se.id AS eq_id, t.name,
            '{list(self.reading_dict.keys())[2]}' AS 'type'
            FROM transformer AS t
            JOIN transformer_type AS ty ON t.type_id = ty.id
            JOIN substation_equipment AS se ON se.transformer_id = t.id
            WHERE t.substation_id = {ss_id}
        """
        self.transformer_df1 = pd.read_sql_query(transformer_str, self.sql_db)
        # transformer params will be in HT side
        self.transformer_df = ht_by_lt_to_ht_or_lt_voltage(self.transformer_df1, ht=True)

        transformer_low_str = f"""
                SELECT ty.voltage_level, se.id AS eq_id, se.name AS name, 
                '{list(self.reading_dict.keys())[3]}' AS 'type' 
                FROM substation_equipment AS se 
                JOIN transformer AS t ON t.id = se.transformer_id
                JOIN transformer_type AS ty ON t.type_id = ty.id
                -- JOIN equipment_voltage ON bus.bus_voltage = equipment_voltage.id
                WHERE se.substation_id = {ss_id} AND se.is_transformer_high = 0
                """
        self.transformer_low_df1 = pd.read_sql_query(transformer_low_str, self.sql_db)
        self.transformer_low_df = ht_by_lt_to_ht_or_lt_voltage(self.transformer_low_df1, ht=False)

        line_str = f"""
        SELECT v.name AS 'voltage_level', se.id AS eq_id, l.name AS 'name', 
        '{list(self.reading_dict.keys())[4]}' AS 'type'
        FROM line AS l
        JOIN substation_equipment AS se ON l.id = se.line_id
        JOIN equipment_voltage AS v ON l.line_voltage = v.id
        WHERE l.src_sub_id = {ss_id} 
        """
        self.line_df = pd.read_sql_query(line_str, self.sql_db)

        feeder_str = f"""
        SELECT v.name AS 'voltage_level', se.id AS eq_id, f.name AS 'name', 
        '{list(self.reading_dict.keys())[5]}' AS 'type'
        FROM feeder AS f
        JOIN substation_equipment AS se ON f.id = se.feeder_id
        JOIN equipment_voltage AS v ON f.feeder_voltage = v.id
        WHERE f.substation_id = {ss_id}
        """
        self.feeder_df = pd.read_sql_query(feeder_str, self.sql_db)

        bank_str = f"""
        SELECT equipment_voltage.name AS 'voltage_level', se.id AS 'eq_id', bank.name AS name, 
        '{list(self.reading_dict.keys())[6]}' AS 'type'
        FROM bank
        JOIN substation_equipment AS se ON bank.id = se.id
        JOIN equipment_voltage ON bank.bank_voltage = equipment_voltage.id
        WHERE bank.substation_id = {ss_id}
        """
        self.bank_df = pd.read_sql_query(bank_str, self.sql_db)

        breaker_str = f"""
        SELECT equipment_voltage.name AS voltage_level, se.id AS eq_id, cb.breaker_name AS name, 
        '{list(self.reading_dict.keys())[7]}' AS 'type'
        FROM breakers AS cb
        JOIN substation_equipment AS se ON cb.sub_equip_id = se.id
        JOIN equipment_voltage ON cb.breaker_voltage = equipment_voltage.id
        WHERE se.substation_id = {ss_id} 
        """
        self.breaker_df = pd.read_sql_query(breaker_str, self.sql_db)
        self.equipment_voltage_id_name_type_df1 = pd.concat(
            [
                self.bus_df,
                self.transformer_high_df,
                self.transformer_df,
                self.transformer_low_df,
                self.line_df,
                self.feeder_df,
                self.bank_df,
                self.breaker_df,
            ],
            ignore_index=True,  # index newly
        )
        for i in self.equipment_voltage_id_name_type_df1.index:
            val = self.equipment_voltage_id_name_type_df1.loc[i, 'voltage_level']
            #  removing kV from voltages (only keeping digit, \D) to make sort applicable
            self.equipment_voltage_id_name_type_df1.loc[i, 'voltage_level'] = re.sub("\D", '', val)

        # sorting
        self.equipment_voltage_id_name_type_df2 = self.equipment_voltage_id_name_type_df1. \
            sort_values(by=['voltage_level', 'type', 'name'], ignore_index=True)
        # indexing by equipment_id (eq_id)
        self.equipment_voltage_id_name_type_df = self.equipment_voltage_id_name_type_df2.T

        eq_id = self.equipment_voltage_id_name_type_df.loc['eq_id', :].tolist()
        voltage_level = self.equipment_voltage_id_name_type_df.loc['voltage_level', :].tolist()
        eq_name = self.equipment_voltage_id_name_type_df.loc['name', :].tolist()
        eq_type = self.equipment_voltage_id_name_type_df.loc['type', :].tolist()

        """Final Multi-index of column creation start"""
        list_of_tuples = list(zip(voltage_level, eq_type, eq_name, eq_id))
        idx = []
        for t in list_of_tuples:
            for reading in self.reading_dict[t[1]]:
                new_val = [reading]
                n = 6
                idx.append(list(t) + new_val)

        self.column_m_index = pd.MultiIndex.from_arrays(list(zip(*idx)),
                                                        names=(
                                                            'Voltage Level [kV]', 'Equipment Type',
                                                            'Equipment Name', 'Equipment Id', 'Reading'))
        """Final Multi-index of column creation finished"""

        """Final index [time stamps] creation start"""
        allowed_time_start = pd.to_datetime(time_start).replace(minute=0, second=0)
        time_end = pd.to_datetime(time_end)
        time_now = datetime.datetime.now()
        if time_end > time_now:
            time_end = time_end

        times = pd.date_range(start=allowed_time_start, end=time_end, freq='1h')

        for pos, time in enumerate(times):
            if (18 <= time.hour <= 19) and time.replace(minute=30) <= time_end:
                times = times.insert(0, time.replace(minute=30))
        self.times = times.sort_values()
        """Final index creation end"""

        self.output_df = pd.DataFrame(index=self.times, columns=self.column_m_index)

        """Populating data from MySQL DB start"""
        for i in self.output_df.index:
            for j in self.output_df.columns:
                try:
                    if j[4] == 'voltage':
                        self.output_df.loc[i, j] = self.voltage_df[self.voltage_df['equipment_id'] == j[3]].loc[
                            i, 'value']
                    elif j[4] == 'mega_watt':
                        self.output_df.loc[i, j] = self.mw_df[self.mw_df['equipment_id'] == j[3]].loc[
                            i, 'value']
                    elif j[4] == 'mega_var':
                        self.output_df.loc[i, j] = self.mvar_df[self.mvar_df['equipment_id'] == j[3]].loc[
                            i, 'value']
                    elif j[4] == 'Ampere R':
                        self.output_df.loc[i, j] = self.amp_df[self.amp_df['equipment_id'] == j[3]].loc[
                            i, 'R_value']
                    elif j[4] == 'Ampere Y':
                        self.output_df.loc[i, j] = self.amp_df[self.amp_df['equipment_id'] == j[3]].loc[
                            i, 'Y_value']
                    elif j[4] == 'Ampere B':
                        self.output_df.loc[i, j] = self.amp_df[self.amp_df['equipment_id'] == j[3]].loc[
                            i, 'B_value']
                    elif j[4] == 'tap':
                        self.output_df.loc[i, j] = self.tap_df[self.tap_df['equipment_id'] == j[3]].loc[
                            i, 'value']
                    elif j[4] == 'Gas Pressure':
                        self.output_df.loc[i, j] = self.pressure_df[self.pressure_df['equipment_id'] == j[3]].loc[
                            i, 'gas_pressure']
                    elif j[4] == 'Air Pressure':
                        self.output_df.loc[i, j] = self.pressure_df[self.pressure_df['equipment_id'] == j[3]].loc[
                            i, 'air_pressure']
                    elif j[4] == 'Spring Status':
                        self.output_df.loc[i, j] = self.pressure_df[self.pressure_df['equipment_id'] == j[3]].loc[
                            i, 'spring_status']

                except KeyError:
                    self.output_df.loc[i, j] = ''

        b = 4
        """Populating data from MySQL DB end"""


time_end_actual = pd.Timestamp.now()

"""applicable for QF-GMD-01"""
time_end = time_end_actual.replace(second=0, microsecond=0)
time_start = time_end - pd.to_timedelta('8 hour')

"""calling Equipment class [backbone class]"""
equipment = Equipment(21, str(time_start), str(time_end))

input_str_in_html = '<input type="number" name>'
df_to_show = equipment.output_df
n_rows = len(df_to_show.iloc[:, 0])
n_col = len(df_to_show.iloc[0, :])

i = df_to_show.index[-1]  # last row
cols = df_to_show.columns
for j in cols:
    if df_to_show.loc[i, j] == '':
        input_str = input_str_in_html.replace('name', 'required id="{{%s}}" name="{{%s}}"' % (j[2], j[2]))
        df_to_show.loc[i, j] = input_str

b = 5
print(pd.Timestamp.now() - time_end_actual)

"""calling custom styler class"""
Df2HtmlWithStyle(equipment.output_df, 'op.html')
print(pd.Timestamp.now() - time_end_actual)
b = 4
