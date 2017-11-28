import sqlite3
from sqlite3 import Error


def execute_sql(connect, sql_command):
    try:
        c = connect.cursor()
        c.execute(sql_command)
    except Error as e:
        print(e)

if __name__ == "__main__":
    database='gaia.db'
    conn = sqlite3.connect(database)

    sql_create_orientation_table = """ 
                                    CREATE TABLE IF NOT EXISTS  orientation (
                                        site  INT,
                                        room  INT,
                                        orien TEXT
                                    );"""

    sql_create_coordinates_table = """ CREATE TABLE IF NOT EXISTS  coordinates (
                                        site INT,
                                        lng  REAL,
                                        lat  REAL
                                    );"""

    sql_create_details_sensor_table = """ CREATE TABLE IF NOT EXISTS details_sensor (
                                        site     INT,
                                        resource INT,
                                        subsite  INT,
                                        property TEXT,
                                        uri      TEXT
                                    );"""

    sql_create_details_sensor_table = """ CREATE TABLE IF NOT EXISTS details_site (
                                            site INT,
                                            name TEXT
                                        );"""


    sql_create_resource_value_table = """ CREATE TABLE IF NOT EXISTS resource_value (
                                        id INT,
                                        time  DATETIME,
                                        value  REAL
                                    );"""

    sql_create_resource_no_outliers_table = """ CREATE TABLE IF NOT EXISTS resource_no_outliers  (
                                            id INT,
                                            time  DATETIME,
                                            value  REAL
                                        );"""

    sql_create_resource_ETL_table = """ CREATE TABLE IF NOT EXISTS resource_ETL  (
                                            id INT,
                                            time  DATETIME,
                                            value  REAL
                                        );"""

    if conn is not None:
        execute_sql(conn, sql_create_orientation_table)
        execute_sql(conn, sql_create_coordinates_table)
        execute_sql(conn, sql_create_details_sensor_table)
        execute_sql(conn, sql_create_resource_value_table)
        execute_sql(conn, sql_create_resource_no_outliers_table)
        execute_sql(conn, sql_create_resource_ETL_table)
    else:
        print("Error! cannot create the database connection.")