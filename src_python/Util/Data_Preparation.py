from pylab import *
from Util.db_util import *

pd.options.mode.chained_assignment = None


def retrieve_data(database, Year, Months, feq=None):
    """
    retrieve data from database during date rage A-B
    :param feq: 5mins sample , None; if 1hour sample use "00:00" ; 1 day sample use "21:00"
    :param database: sqlite database file
    :param Year :
    :param Months: query months range
    :return: full site list ,
             dictionary of : sensor data, API data for cloud coverage , API data for Temperature
    """
    dict_df = {}
    dict_df_cloud = {}
    dict_df_tempc = {}
    date_A = str(Year) + "-" + str('{:02d}'.format(Months[0])) + "-01"
    if 12 == Months[-1]:
        date_B = str(Year + 1) + "-01-01"
    else:
        date_B = str(Year) + "-" + str('{:02d}'.format(Months[-1] + 1)) + "-01"

    with create_connection(database) as conn:
        print("Database Connecting for sensor data....")
        try:
            orientation = {}
            c = conn.cursor()
            # site_list = c.execute("select site from details_sensor group by site;") # TODO or fill the table :details_site
            # site_list = [str(id[0]) for id in site_list]
            site_list = [
                # "144024","28843", "144243", "28850",
                '157185'  # "155849","144242",  "19640", "27827", "155849",
                #     "155851", "155076", "155865", "155077",
                #     "155877", "157185", "159705"
            ]
            for site_id in site_list:

                temperature_resource_list = query_site_room_orientaion(c, site_id)
                orientation[site_id] = temperature_resource_list
                temperature_resource_list = [i[0] for i in temperature_resource_list]
                if feq:
                    dict_df[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list, date_A,
                                                                      date_B, feq)
                else:
                    dict_df[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list, date_A,
                                                                      date_B)

                query = " select time,value from API_CloudCoverage " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_cloud[site_id] = select_single_sensor_to_pandas(c, query, site_id)

                query = " select time,value from API_Temperature " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_tempc[site_id] = select_single_sensor_to_pandas(c, query, site_id)
        except Error as e:
            print("SQL ERROR:", e)
    return site_list, dict_df, dict_df_cloud, dict_df_tempc, orientation


def retrieve_coordinate(database):
    """
    retrieve the coordinate of all the sites
    :param database:  format like  '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    :return: dictionary of coordinate
    """
    with create_connection(database) as conn:
        print("Database Connecting for coordinate....")
        try:
            c = conn.cursor()
            coordinate_dict = query_site_lat_lng(c)
            return coordinate_dict
        except Error as e:
            print("SQL ERROR:", e)
            return None


def retrieve_orientation(database):
    """
    retrieve the orientation of all the sites
    :param database: format like  '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    :return:  dictionary of orientation
    """
    with create_connection(database) as conn:
        print("Database Connecting for orientation ....")
        try:
            c = conn.cursor()
            orientation_dict = query_site_orientaion(c)

        except Error as e:
            print("SQL ERROR:", e)
            return None
    return orientation_dict


def outliers_sliding_window(df, window_number):
    """
    remove the outlier
    :param df: dataframe
    :param window_number: checking only window_number size history data
    :return: updated dataframe
    """
    window = []
    outlier = 0
    max_range = 0
    for i in range(df.size):
        item = df[i]
        if len(window) == window_number:
            window.pop(0)
        window.append(item)
        previous = window[:-1]
        if len(previous) == 0:
            continue

        average = mean(previous)
        Q1 = np.percentile(previous, 25)
        Q3 = np.percentile(previous, 75)
        upper = Q3 + (Q3 - Q1) * 3  # 1.5
        lower = Q1 - (Q3 - Q1) * 3  # 1.5
        last_min = np.percentile(previous, 0)
        last_max = np.percentile(previous, 100)
        min = np.percentile(window, 0)
        max = np.percentile(window, 100)
        now_range = max - min
        if max_range < now_range:
            max_range = now_range

        if np.isnan(item) or item == 0:
            outlier += 1
            window[-1] = average  # or fill back with last_max
            df[i] = average  # or fill back with last_max

        if len(previous) < window_number - 1:
            continue

        # new peak comes out, OTHERWISE keep calm and carry on
        if now_range > max_range * 0.9:  # 0.8:
            outlier += 1
            if item < lower:
                window[-1] = average  # or fill back with last_min ; (last_min + min) / 2
                df[i] = average  # or fill back with last_min ;(last_min + min) / 2
            if item > upper:
                window[-1] = average  # or fill back with last_max; (last_max + max) / 2
                df[i] = average  # or fill back with last_max ; (last_max + max) / 2

    return df, outlier, average


def ETL(df):
    df = df[~df.index.duplicated(keep='first')]

    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            df = df.drop(head, 1)
            continue

    if df.shape[1] > 0:
        active_time = []
        dfT = df.T
        for head in list(dfT):
            df_col = dfT[head]
            try:
                if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
                    dfT[head] = dfT[head].replace(0, np.nan)
                else:
                    active_time.append(head)
            except ValueError:
                continue
        df = dfT.T
        begin = df.index.get_loc(active_time[0])
    else:
        return df, [], -1

    df_power = df.iloc[begin:]

    df_power[df_power == 0] = np.nan

    for head in list(df_power):
        df_col = df_power[head]
        df_col, outliers, average = outliers_sliding_window(df_col, window_number=10)
        df_col = df_col.rolling(window=6, center=False, min_periods=1).mean()
        df_col = df_col.fillna(average)
        df_power[head] = df_col

    df.iloc[begin:] = df_power
    # df.iloc[:begin] = 0
    return df, list(df), begin


def ETL_activity(df):
    """
    if any none zero data from any sensor, this device is active
    so we add all up from one device and more accuracy for activity statistic

    :param df: data from the same device, different sensors
    :return: the device on the timeline  active or not (1 or 0)
    """
    df = df[~df.index.duplicated(keep='first')]
    df[df != 0] = 1
    df_active = df.sum(axis=1)
    df_active[df_active > 0] = 1
    return df_active


def feedback_data():
    # TODO write ETL data into database
    pass


def reindex_df(day_index, df):
    """
    use new index timestamps to replace the default index
    must be the same size or it won't work
    :param day_index: timestamp index
    :param df: dataframe with default index
    :return: dataframe with timestamp index
    """
    try:
        df['timestamps'] = day_index
        df = df.reset_index(drop=True)
        df = df.set_index('timestamps')
        return df
    except ValueError:
        print(len(day_index), df.shape, "see they are different size to merge")
        return df