import sqlite3
from collections import defaultdict
from sqlite3 import Error
import pandas as pd
import glob
import os
import shutil
import requests
import json


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None


def execute_sql(cursor, sql_command):
    try:
        return cursor.execute(sql_command)
    except Error as e:
        print(e)
    return None


def init_database(cursor):
    """
    :param cursor:
    :return: an empty database with initialized tables
    """
    sql_create_orientation_table = """ 
                                    CREATE TABLE IF NOT EXISTS  orientation (
                                        site  INT,
                                        room  INT,
                                        orien TEXT
                                    );"""
    execute_sql(cursor, sql_create_orientation_table)

    sql_create_coordinates_table = """ CREATE TABLE IF NOT EXISTS  coordinates (
                                        site INT,
                                        lng  REAL,
                                        lat  REAL
                                    );"""
    execute_sql(cursor, sql_create_coordinates_table)

    sql_create_details_sensor_table = """ CREATE TABLE IF NOT EXISTS details_sensor (
                                        site     INT,
                                        resource INT,
                                        subsite  INT,
                                        property TEXT,
                                        uri      TEXT
                                    );"""
    execute_sql(cursor, sql_create_details_sensor_table)

    sql_create_details_site_table = """ CREATE TABLE IF NOT EXISTS details_site (
                                            site INT,
                                            name TEXT
                                        );"""
    execute_sql(cursor, sql_create_details_site_table)

    sql_create_API_Temperature_table = """
                                            CREATE TABLE IF NOT EXISTS API_Temperature  (
                                            id INT,
                                            time  DATETIME,
                                            value  REAL
                                            );
                                        """

    execute_sql(cursor, sql_create_API_Temperature_table)

    sql_create_API_CloudCoverage_table = """
                                                CREATE TABLE IF NOT EXISTS API_CloudCoverage  (
                                                id INT,
                                                time  DATETIME,
                                                value  REAL
                                                );
                                            """
    execute_sql(cursor, sql_create_API_CloudCoverage_table)


def WeatherAPI_to_sqlite(c, date_range, Y):
    """
    collecting temperature , cloud coverage , humidity
    :param c: cursor
    :param date_range: how many months do you like
                        example
                        [
                            '1-1', '1-31',
                            '2-1', '2-28',
                            '3-1,', '3-31',
                            '4-1', '4-30',
                            '5-1', '5-31',
                            '6-1', '6-30',
                            '7-1', '7-31',
                            '8-1', '8-31,',
                            '9-01', '9-30',
                            '10-1', '10-31',
                            '11-1', '11-30',
                            '12-1', '12-31',
                        ]
    :param Y: Year
    :return: print stdout sqlite commands for import API weather data into sqlite
    """
    coordinate_dict = query_site_lat_lng(c)
    site_list = query_site_list(c)

    df_peak_API_Cloud = pd.DataFrame(columns=site_list)
    df_peak_API_TempC = pd.DataFrame(columns=site_list)
    df_peak_API_Humidity = pd.DataFrame(columns=site_list)
    URL = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"

    for site_i in site_list:
        lng, lat = coordinate_dict[site_i][0], coordinate_dict[site_i][1]
        coordinate = str(lat) + "," + str(lng)

        result_listTempC = []
        result_listHumidity = []
        result_listCloud = []
        result_listTime = []

        Year = Y + "-"
        date_range = iter(date_range)
        for i in date_range:
            begin = Year + i
            end = Year + next(date_range)
            params = {
                'q': coordinate,
                'date': begin,
                'enddate': end,
                'tp': '1',
                'key': '612818efa9204368a1785431172610',  # TODO expired 2017 Dec
                'format': 'json',
                'includelocation': 'yes',
            }
            r = requests.get(URL, params).json()
            print(json.dumps(r, sort_keys=True, indent=4))  # human-readable response :)
            for i in r["data"]["weather"]:
                date = i["date"]
                for j in i["hourly"]:
                    result_listTempC.append(int(j["tempC"]))
                    result_listCloud.append(int(j["cloudcover"]))
                    result_listHumidity.append(int(j["humidity"]))
                    result_listTime.append(
                        str(date) + " " + '{:02d}'.format(int(int(j["time"]) / 100)) + ":00" + ":00")

        df_peak_API_Cloud[site_i] = result_listCloud
        df_peak_API_Humidity[site_i] = result_listHumidity
        df_peak_API_TempC[site_i] = result_listTempC

    df_peak_API_Cloud["timestamps"] = result_listTime
    df_peak_API_Cloud = df_peak_API_Cloud.reset_index(drop=True)
    df_peak_API_Cloud = df_peak_API_Cloud.set_index('timestamps')
    df_peak_API_Cloud.to_csv("API_Cloud_" + Y + ".csv", sep=";")
    csv_batch_to_one_table("./", "API_CloudCoverage", "API_Cloud_")

    df_peak_API_TempC["timestamps"] = result_listTime
    df_peak_API_TempC = df_peak_API_TempC.reset_index(drop=True)
    df_peak_API_TempC = df_peak_API_TempC.set_index('timestamps')
    df_peak_API_TempC.to_csv("API_tempC_" + Y + ".csv", sep=";")
    csv_batch_to_one_table("./", "API_Temperature", "API_tempC_")

    df_peak_API_Humidity["timestamps"] = result_listTime
    df_peak_API_Humidity = df_peak_API_Humidity.reset_index(drop=True)
    df_peak_API_Humidity = df_peak_API_Humidity.set_index('timestamps')
    df_peak_API_Humidity.to_csv("API_Humidity_" + Y + ".csv", sep=";")
    csv_batch_to_one_table("./", "df_peak_API_Humidity", "API_Humidity_")


# def pandas_to_sqlite(df):
#     """
#     :param df: df head : [ timestamps id1  id2 ...idn]
#     :return: sqlite execute command
#     """
#     directory = './DBimport/'
#     if not os.path.exists(directory):
#         os.makedirs(directory)
#
#     for value in list(df)[1::]:
#         print(value)
#         df_value = df[['timestamps', value]]
#         df_value["id"] = [value] * df.shape[0]
#         new = ["id", "timestamps", value]
#         df_value = df_value.reindex(columns=new)
#         # print(df_value)
#         df_value.to_csv(directory + value + "to_import_sqlite.csv", sep=";", index=False, header=False)
#     csv_batch_to_sqlite(directory, "resource_value")


def WeatherAPIcsv_to_sqlite(APIcsvfile, table):
    """

    :param APIcsvfile: head list ['timestamps, id1,id2,...]
    :param table for the input csv data
    :return: sqlite execute command
    """
    df = pd.read_csv(APIcsvfile, delimiter=";", parse_dates=True)  # ,index_col='timestamps')
    directory = './API_import/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        shutil.rmtree(directory)
        os.makedirs(directory)
    headers = list(df)
    for id in headers[1::]:
        df_value = df[['timestamps', id]]
        df_value["id"] = [id] * df.shape[0]
        new = ["id", "timestamps", id]
        df_value = df_value.reindex(columns=new)
        df_value.to_csv(directory + id + "to_import_sqlite.csv", sep=";", index=False, header=False)
    csv_batch_to_one_table(directory, table)


def csv_batch_to_one_table(folder, table, matcher=None):
    # print("Copy paste below dot command into your sqlite \n! Nothing execute in here\n")
    if matcher:
        file_list = glob.glob(folder + matcher + "*.csv")
        print
    else:
        file_list = glob.glob(folder + "*.csv")
    print(".separator \";\"")
    for i in file_list:
        print(".import " + i + " " + table)
    print(
        "delete from " + table + " where rowid not in (select  min(rowid) from " + table + " group by id,time,value);")


def csv_batch_to_tables(sensor_data_folder):
    """
    :param sensor_data_folder:  inside  filename = "/../../5min2017Jul-Oct/27827.csv <---> table site_27827"
    :return: sqlite commands for import data into tables for each site
    """
    file_list = glob.glob(sensor_data_folder + "*.csv")
    print(".separator \";\"")
    for file in file_list:
        table = "site_" + file.split("/")[-1].split(".")[0]
        print(".import " + file + " " + table)
        print()
        print(
            "delete from " + table + " where rowid not in (select  min(rowid) from " + table + " group by id,time,value);")
        print()


def sqlite_to_csv(query, csvfile):
    # sqlite > select time, value from resource_value where time > '2017-03-21' and time < '2017-03-22';
    # print("Copy paste below dot command into your sqlite \n! Nothing execute in here\n")
    print(".headers on")
    print(".mode csv")
    print(".separator \";\"")
    print(".output " + csvfile)
    print(query)


def query_site_list(c):
    site_list = c.execute("select site from details_sensor group by site;")
    site_list = [id[0] for id in site_list]
    print(site_list)
    return site_list


def query_temp_resource_value(cursor, temp_resource_list):
    for id in temp_resource_list:
        resource_value = cursor.execute("select id , time, value from resource_value "
                                        "where id = " + str(id)
                                        + " and time > '2017-03-21' and time < '2017-03-22';")
    return resource_value


def query_site_room_orientaion(cursor, site_id):
    # print("select room, orien from orientation where site =" + str(site_id) + ";")
    orient = cursor.execute("select room, orien from orientation where site =" + str(site_id))
    # print(orient.fetchall()) # tuple [id,'NE']
    return orient.fetchall()


def query_site_orientaion(cursor):
    orientaion_dict = defaultdict(list)
    resp = cursor.execute("select * from orientation")
    for site, room, orient in resp.fetchall():
        orientaion_dict[site].append([room, orient])
    return orientaion_dict


def query_site_lat_lng(cursor):
    coordinate_dict = {}
    resp = cursor.execute("select * from coordinates")
    for site, lng, lat in resp.fetchall():
        coordinate_dict[site] = [lng, lat]
    return coordinate_dict


def query_temperature_resource(cursor, site_id):
    resource_list = cursor.execute("select resource from details_sensor "
                                   "where site= " + site_id
                                   + " and property = 'Temperature'"
                                   + " and uri NOT LIKE '%site%' "
                                   + "group by resource").fetchall()
    return [id[0] for id in resource_list]


def query_same_device_other_sensor(cursor, subsite_id):
    resource = cursor.execute("select resource from details_sensor "
                              "where subsite = " + subsite_id
                              + " and property = 'Motion'"
                              ).fetchall()
    return resource


def query_resource_device(cursor, site_id, property):
    """
    :param cursor:
    :param site_id:
    :param property: "Luminosity","Motion","Relative Humidity", etc ...
    :return: sensors id list and related pysical device list
    """
    resp = cursor.execute("select resource,uri,subsite from details_sensor "
                          "where site= " + str(site_id)
                          + " and property = '" + property + "'"
                          + " and uri NOT LIKE '%site%' "
                          + " and subsite != 0 "
                          + " group by resource"
                          ).fetchall()
    sensors = [iter[0] for iter in resp]
    devices = [iter[1].split("/")[-2] for iter in resp]  # TODO depend on different type of uri naming rules
    print(devices)
    sensor_temp = []
    for iter in devices:
        subsite = cursor.execute("select subsite from details_sensor "
                              "where site= " + str(site_id)
                              + " and property = 'Temperature'"
                              + " and uri LIKE '%" + iter + "%' "
                              + " and subsite != 0 "
                              + " group by resource"
                              ).fetchall()[0][0]

        print(subsite)

    return sensors, devices


def select_single_sensor_to_pandas(cursor, query, id):
    resp = cursor.execute(query)
    df = pd.DataFrame(resp.fetchall(), columns=["timestamps", id], dtype=float)
    df = df.reset_index(drop=True)
    df = df.set_index('timestamps')
    df = df[~df.index.duplicated(keep='first')]

    return df


def select_time_range_to_dataframe(cursor, site_id, resource_list, begin, end, feq=None):
    df_final = pd.DataFrame()
    for id in resource_list:
        if feq:
            query = "select time, value from site_" + site_id + " where id = " + str(id) + \
                    " and time LIKE  '%" + feq + "%' and time > '" + begin + "' and time < '" + end + "';"
        else:
            query = "select time, value from site_" + site_id + " where id = " + str(id) + \
                    " and time > '" + begin + "' and time < '" + end + "';"
        resp = cursor.execute(query)
        df = pd.DataFrame(resp.fetchall(), columns=["timestamps", id], dtype=float)
        df = df.reset_index(drop=True)
        df = df.set_index('timestamps')
        df = df[~df.index.duplicated(keep='first')]
        df_final = pd.concat([df_final, df], axis=1)
    # df_final.to_csv("df_final.csv")
    return df_final


def create_resource_value_tables(cursor):
    site_list = c.execute("select site from details_sensor group by site;")
    site_list = [str(id[0]) for id in site_list]
    for site_id in site_list:
        sql_create_site_i_table = " CREATE TABLE IF NOT EXISTS site_" + site_id + " (id INT,time DATETIME ,value  REAL);"
        # print(sql_create_site_i_table)
        execute_sql(cursor, sql_create_site_i_table)


def create_resource_ETL_tables(cursor):
    site_list = c.execute("select site from details_sensor group by site;")
    site_list = [str(id[0]) for id in site_list]
    for site_id in site_list:
        sql_create_site_i_table = " CREATE TABLE IF NOT EXISTS site_" + site_id + "_ETL (id INT,time DATETIME ,value  REAL);"
        # print(sql_create_site_i_table)
        execute_sql(cursor, sql_create_site_i_table)


def create_index_resource_value_tables(cursor, site_i_table):
    """
    speed up the performance by creating index on the table on : id(aka resource id) and  id+time
    :param cursor:
    :param site_i_table: for site i contains [ id , time,value ]
    :return: None
    Ref: https://medium.com/@JasonWyatt/squeezing-performance-from-sqlite-indexes-indexes-c4e175f3c346
    """
    print("CREATE INDEX id ON " + site_i_table + " (id);")
    print("CREATE INDEX id_time ON " + site_i_table + " (id,time);")
    cursor.execute("CREATE INDEX id ON " + site_i_table + " (id);")
    cursor.execute("CREATE INDEX id_time ON " + site_i_table + " (id,time);")


if __name__ == "__main__":
    with create_connection('/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db') as conn:
        try:
            c = conn.cursor()
            print(query_resource_device(c, '155877', 'Motion'))
            # query_site_lat_lng(c)

            # ######### init table for database
            # # init_database(c)

            # ######### after you fill up the details_of_sensor with, use this to create value table for each site
            # # create_resource_value_tables(c)
            # # create_resource_ETL_tables(c)

            # ######### import from CSV demo , fastest way , execute in the sqlite not in the scripts
            # resource_value_folder = "/Users/nanazhu/Documents/Sapienza/temp/5min2017Apr-Jun/"
            # csv_batch_to_tables(resource_value_folder)
            # resource_value_folder = "/Users/nanazhu/Documents/Sapienza/temp/5min2017Jan-Mar/"
            # csv_batch_to_tables(resource_value_folder)

            # ######### export to CSV demo
            # query = """select time, value from resource_value where time > '2017-03-21' and time < '2017-03-22';"""
            # exportToCSV(query, 'JustATest.csv')

            # ######### a full demo story:find all the temperature sensor data
            # #######   input
            # #######   time range : whole March
            # ######### output: pandas dataframe for one site
            # site_list = c.execute("select site from details_sensor group by site;")
            # site_list = [str(id[0]) for id in site_list]
            # for site_id in site_list:
            #     print(site_id, ":")
            #     temperature_resource_list = query_temperature_resource(c, site_id)
            #     print(temperature_resource_list)
            #     df = select_time_range_to_dataframe(c, temperature_resource_list, '2017-03-01', '2017-04-01')

            # ######### convert from previous format into ready-to-import-to-sqlite csv file
            # df = pd.read_csv("API_tempC_Febhourly.csv", delimiter=";", parse_dates=True)
            # print(df)
            # pandas_to_sqlite(df)


            ##########  Demo on WeatherOnlineAPI data collecting
            # date_range = [
            #     # '1-1', '1-31',
            #     # '2-1', '2-28',
            #     # '3-1,', '3-31',
            #     # '4-1', '4-30',
            #     # '5-1', '5-31',
            #     # '6-1', '6-30',
            #     # '7-1', '7-31',
            #     # '8-1', '8-31,',
            #     # '9-01', '9-30',
            #     '10-1', '10-31',
            #     '11-1', '11-30',
            #     # '12-1', '12-31',
            # ]
            # Year = '2017'
            # WeatherAPI_to_sqlite(c, date_range, Year)

            # WeatherAPIcsv_to_sqlite("API_Cloud_2017.csv", "API_CloudCoverage")
            # WeatherAPIcsv_to_sqlite("API_tempC_2017.csv", "API_Temperature")

        except Error as e:
            print("SQL ERROR:", e)
