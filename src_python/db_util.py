import sqlite3
from sqlite3 import Error
import pandas as pd
import glob


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

    sql_create_resource_value_table = """ CREATE TABLE IF NOT EXISTS resource_value (
                                        id INT,
                                        time  DATETIME,
                                        value  REAL
                                    );"""
    execute_sql(cursor, sql_create_resource_value_table)

    sql_create_resource_no_outliers_table = """ CREATE TABLE IF NOT EXISTS resource_no_outliers  (
                                            id INT,
                                            time  DATETIME,
                                            value  REAL
                                        );"""
    execute_sql(cursor, sql_create_resource_no_outliers_table)

    sql_create_resource_ETL_table = """ CREATE TABLE IF NOT EXISTS resource_ETL  (
                                            id INT,
                                            time  DATETIME,
                                            value  REAL
                                        );"""
    execute_sql(cursor, sql_create_resource_ETL_table)


def importFromCSV(folder, table):
    file_list = glob.glob(folder + "*.csv")
    # print("Copy paste below dot command into your sqlite \n! Nothing execute in here\n")
    print(".separator \";\"")
    for i in file_list:
        print(".import " + i + " " + table)
    print("""delete from resource_value where rowid not in 
    (select  min(rowid) from resource_value group by id,time,value);""")


def exportToCSV(query, file):
    # print("Copy paste below dot command into your sqlite \n! Nothing execute in here\n")
    print(".headers on")
    print(".mode csv")
    print(".separator \";\"")
    print(".output " + file)
    print(query)
    # sqlite > select time, value from resource_value where time > '2017-03-21' and time < '2017-03-22';


def query_temp_resource_value(cursor, temp_resource_list):
    for id in temp_resource_list:
        resource_value = cursor.execute("select id , time, value from resource_value "
                                        "where id = " + str(id)
                                        + " and time > '2017-03-21' and time < '2017-03-22';")
    return resource_value


def query_site_room_orientaion(cursor, room_list):
    for id in room_list:
        orient = [i for i in cursor.execute("select room, orien from orientation where room =" + str(id))]
    return orient


def query_temperature_resource(cursor, site_id):
    resource_list = cursor.execute("select resource from details_sensor "
                                   "where site= " + site_id
                                   + " and property = 'Temperature'"
                                   + " and uri NOT LIKE '%site%' "
                                   + "group by resource")
    return [id[0] for id in resource_list]


def select_data_to_col(cursor, query):
    # resp = cursor.execute("select time, value from resource_value "
    #                  "where id = " + str(id)
    #                  + " and time > '2017-03-01' and time < '2017-04-01';")
    resp = cursor.execute(query)
    df = pd.DataFrame(resp.fetchall(), columns=["timestamps", id], dtype=float)
    df = df.reset_index(drop=True)
    df = df.set_index('timestamps')
    df = df[~df.index.duplicated(keep='first')]
    return df


def select_time_range_to_dataframe(cursor, resource_list, begin, end):
    df_final = pd.DataFrame()
    for id in resource_list:
        resp = cursor.execute("select time, value from resource_value "
                              "where id = " + str(id)
                              + " and time > '" + begin + "' and time < '" + end + "';")
        # + " and time > '2017-03-01' and time < '2017-04-01';")
        df = pd.DataFrame(resp.fetchall(), columns=["timestamps", id], dtype=float)
        df = df.reset_index(drop=True)
        df = df.set_index('timestamps')
        df = df[~df.index.duplicated(keep='first')]
        df_final = pd.concat([df_final, df], axis=1)
        print(df_final.shape)
    df_final.to_csv(df_final + ".csv")
    return df_final


if __name__ == "__main__":
    database = 'test.db'

    with create_connection(database) as conn:
        try:
            cursor = conn.cursor()
            #########  init table for database
            # init_database(conn)

            #########   import from CSV , fastest way
            # resource_value_folder = "/Users/nanazhu/Documents/Sapienza/sqlite/5min2017Jan-Mar/"
            # importFromCSV(resource_value_folder, "resource_value")


            #########  export to CSV
            # query = """select time, value from resource_value where time > '2017-03-21' and time < '2017-03-22';"""
            # exportToCSV(query, 'JustATest.csv')


            #########  demo story :find all the resources which are temperature
            # input
            # time range : whole March
            # output: one dataframe for one site
            #########

            c = conn.cursor()
            site_list = c.execute("select site from details_sensor group by site;")
            site_list = [str(id[0]) for id in site_list]
            for site_id in site_list:
                print(site_id, ":")
                temperature_resource_list = query_temperature_resource(c, site_id)
                print(temperature_resource_list)
                df = select_time_range_to_dataframe(c, temperature_resource_list, '2017-03-01', '2017-04-01')
                break


        except Error as e:
            print("SQL ERROR:", e)
