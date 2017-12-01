import sqlite3
from sqlite3 import Error
import pandas as pd
import glob
import os
import shutil


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


def pandas_to_sqlite(df):
    """
    :param df: df head : [ timestamps id1  id2 ...idn]
    :return: sqlite execute command
    """
    directory = './DBimport/'
    if not os.path.exists(directory):
        os.makedirs(directory)


    for value in list(df)[1::]:
        print(value)
        df_value = df[['timestamps', value]]
        df_value["id"] = [value] * df.shape[0]
        new = ["id", "timestamps", value]
        df_value = df_value.reindex(columns=new)
        # print(df_value)
        df_value.to_csv(directory + value + "to_import_sqlite.csv", sep=";", index=False, header=False)
    csv_batch_to_sqlite(directory, "resource_value")

def csv_to_sqlite(APIcsvfile,table):
    """

    :param APIcsvfile: head list ['timestamps, id1,id2,...]
    :param table for the input csv data
    :return: sqlite execute command
    """
    df = pd.read_csv(APIcsvfile, delimiter=";", parse_dates=True)#,index_col='timestamps')
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
    csv_batch_to_sqlite(directory, table)


def csv_batch_to_sqlite(folder, table):
    # print("Copy paste below dot command into your sqlite \n! Nothing execute in here\n")

    file_list = glob.glob(folder + "*.csv")
    print(".separator \";\"")
    for i in file_list:
        print(".import " + i + " " + table)
    print(
        "delete from " + table + " where rowid not in (select  min(rowid) from " + table + " group by id,time,value);")


def sqlite_to_csv(query, csvfile):
    # sqlite > select time, value from resource_value where time > '2017-03-21' and time < '2017-03-22';
    # print("Copy paste below dot command into your sqlite \n! Nothing execute in here\n")
    print(".headers on")
    print(".mode csv")
    print(".separator \";\"")
    print(".output " + csvfile)
    print(query)


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


def query_site_lat_lng(cursor):
    coordinate_dict={}
    resp = cursor.execute("select * from coordinates")
    for id,lng,lat in resp.fetchall():
        coordinate_dict[id]=[lng,lat]
    return coordinate_dict


def query_temperature_resource(cursor, site_id):
    resource_list = cursor.execute("select resource from details_sensor "
                                   "where site= " + site_id
                                   + " and property = 'Temperature'"
                                   + " and uri NOT LIKE '%site%' "
                                   + "group by resource")
    return [id[0] for id in resource_list]


def select_single_sensor_to_pandas(cursor, query,id):
    resp = cursor.execute(query)
    df = pd.DataFrame(resp.fetchall(), columns=["timestamps", id], dtype=float)
    df = df.reset_index(drop=True)
    df = df.set_index('timestamps')
    df = df[~df.index.duplicated(keep='first')]

    return df


def select_time_range_to_dataframe(cursor, site_id, resource_list, begin, end):
    df_final = pd.DataFrame()
    for id in resource_list:
        resp = cursor.execute("select time, value from site_" + site_id +
                              " where id = " + str(id)
                              + " and time > '" + begin + "' and time < '" + end + "';")
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


            # csv_to_sqlite("API_Cloud_2017.csv","API_CloudCoverage")
            

            #query_site_lat_lng(c)

            # ######### init table for database
            # # init_database(c)
            #
            # ######### after you fill up the details_of_sensor with, use this to create value table for each site
            # # create_resource_value_tables(c)
            # # create_resource_ETL_tables(c)
            #
            # ######### import from CSV demo , fastest way , execute in the sqlite not in the scripts
            # # resource_value_folder = "/Users/nanazhu/Documents/Sapienza/sqlite/5min2017Jan-Mar/"
            # # importFromCSV(resource_value_folder, "resource_value")
            #
            # ######### export to CSV demo
            # query = """select time, value from resource_value where time > '2017-03-21' and time < '2017-03-22';"""
            # exportToCSV(query, 'JustATest.csv')
            #
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
            #
            # ######### convert from previous format into ready-to-import-to-sqlite csv file
            # df = pd.read_csv("API_tempC_Febhourly.csv", delimiter=";", parse_dates=True)
            # print(df)
            # pandas_to_sqlite(df)

        except Error as e:
            print("SQL ERROR:", e)
