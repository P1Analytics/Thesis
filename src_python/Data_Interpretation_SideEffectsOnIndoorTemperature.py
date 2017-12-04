import time
import operator
from collections import Counter
from Data_Preparation import *

warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def sun_rise_set(lat, lng, timestamp):
    timezone_url = "https://maps.googleapis.com/maps/api/timezone/json?location=" + str(lat) + "," + str(lng) + \
                   "&timestamp=" + str(timestamp) + \
                   "&key=AIzaSyAI4--_x4AE2K5zZ6Z5tZafwwpVI9uYlYM"
    # print(timezone_url)
    rs = requests.get(timezone_url).json()
    # print(json.dumps(rs, sort_keys=True, indent=4))  # human-readable response :)
    GMT = int((int(rs["dstOffset"]) + int(rs["rawOffset"])) / 3600)

    rise_set_url = "https://api.sunrise-sunset.org/json?lat=" + str(lat) + "&lng=" + str(lng) + \
                   "&date=" + time.strftime("%D", time.localtime(timestamp))
    try:
        rise_set = requests.get(rise_set_url).json()
    except:
        return 8, 12, 18
    sunrise = int(rise_set["results"]["sunrise"].split(":")[0]) + GMT
    noon = int(rise_set["results"]["solar_noon"].split(":")[0]) + GMT
    sunset = int(rise_set["results"]["sunset"].split(":")[0]) + 12 + GMT
    return sunrise, noon, sunset


def predict_orientation(df_rooms, active_rooms, date_list):
    prediction = []
    peak_dict = defaultdict(list)
    peak_top3 = defaultdict(list)
    sunrise, noon, sunset = sun_rise_set(lat, lng, pd.Timestamp(date_list[0] + ' 01:00').timestamp())
    for date in date_list:
        if '-15' in date or '-30' in date:  # update the sunrise sunset every 15 days
            sunrise, noon, sunset = sun_rise_set(lat, lng, pd.Timestamp(date + ' 01:00').timestamp())
        begin = df_rooms.index.get_loc(date + " " + '{:02d}'.format(sunrise + 2) + ':00:00')
        end = df_rooms.index.get_loc(date + " " + '{:02d}'.format(sunset) + ':00:00')
        df = df_rooms.iloc[begin:end]
        for room in active_rooms:
            if pd.isnull(df[room]).sum() == len(df.index.values):
                continue
            df.index = pd.to_datetime(df.index)
            peak_top3[room].append(list(map(int, [pd.to_datetime(str(timestamps)).strftime('%H') for timestamps in
                                                  df[room].nlargest(3).index.values])))
            peak_dict[room].append(df[room].groupby(pd.TimeGrouper('D')).idxmax().dt.hour.values[0])

    for room in active_rooms:
        peak = peak_dict[room]
        sum = 0
        east = False
        morning = []
        morning_counter = 0
        for hour in peak:
            sum += hour / 24 * 360
            if sunrise <= hour <= noon - 1:
                east = True
                morning.append(hour)
                morning_counter += 1
        peak_in_morning_ratio = morning_counter / len(peak)
        morning_peak_ratio = 0
        if east:
            morning_peak_ratio = peak_in_morning_ratio
        degree_prediction = sum / len(peak)

        if 0 <= degree_prediction < sunrise / 24 * 360:
            result = "NE"
        if 0.5 < peak_in_morning_ratio or sunrise / 24 * 360 <= degree_prediction < noon / 24 * 360:
            result = "SE"
        if noon / 24 * 360 <= degree_prediction < sunset / 24 * 360:
            result = "SW"
        if sunset / 24 * 360 <= degree_prediction < 359:
            result = "NW"
        prediction.append([room, result])

        peak_top3[room] = [item for sublist in peak_top3[room] for item in sublist]

        # print(room, '{:05.1f}'.format(degree_prediction), '{:05.2f}%'.format(morning_peak_ratio * 100),
        #       "peak hour,frequency:", sorted(dict(Counter(peak)).items(), key=operator.itemgetter(1), reverse=True),
        #       "temperature range: [",
        #       '{:05.2f}'.format(np.percentile(df[room].values, 0)),
        #       '{:05.2f}'.format(np.percentile(df[room].values, 50)),
        #       '{:05.2f}]'.format(np.percentile(df[room].values, 100)))

    return prediction, peak_top3


def retrieve_human_active(database, Year, Months, feq=None):
    """
    retrieve data from database during date rage A-B
    :param feq: 5 mins sample use None; if 1hour sample use "00:00" ; 1 day sample use "21:00"
    :param database: sqlite database file
    :param Year :
    :param Months: query months range
    :return: full site list ,
             dictionary of : sensor data, API data for cloud coverage , API data for Temperature
    """
    dict_df_temp = {}
    dict_df_motion = {}
    dict_df_cloud = {}
    dict_df_tempC = {}
    date_A = str(Year) + "-" + str('{:02d}'.format(Months[0])) + "-01"
    if 12 == Months[-1]:
        date_B = str(Year + 1) + "-01-01"
    else:
        date_B = str(Year) + "-" + str('{:02d}'.format(Months[-1] + 1)) + "-01"
    with create_connection(database) as conn:
        print("Database Connecting for sensor data....")
        try:
            c = conn.cursor()
            site_list = c.execute(
                "select site from details_sensor group by site;")  # TODO or fill the table :details_site
            site_list = [str(id[0]) for id in site_list]
            site_list=[
                #"144024","28843", "144243", "28850",
                "144242",#"155849", #"144242",  "19640", "27827", "155849",
            #     "155851", "155076", "155865", "155077",
            #     "155877", "157185", "159705"
            ]
            for site_id in site_list:
                temperature_resource_list = [i[0] for i in query_site_room_orientaion(c, site_id)]
                motion_resource_list = []
                for i in temperature_resource_list:
                    subsite_id = c.execute("select subsite from details_sensor where resource= " + str(
                        i) + " and subsite !=0 ;").fetchall()[0][0]
                    resource = c.execute("select resource from details_sensor where subsite = " + str(
                        subsite_id) + " and property = 'Motion';").fetchall()
                    if resource:
                        resource = resource[0][0]
                        motion_resource_list.append(resource)
                if feq:
                    dict_df_temp[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list,
                                                                           date_A, date_B, feq)
                    dict_df_motion[site_id] = select_time_range_to_dataframe(c, site_id, motion_resource_list, date_A,
                                                                             date_B, feq)
                else:
                    dict_df_temp[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list,
                                                                           date_A, date_B)
                    dict_df_motion[site_id] = select_time_range_to_dataframe(c, site_id, motion_resource_list, date_A,
                                                                             date_B)

                query = " select time,value from API_CloudCoverage " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_cloud[site_id] = select_single_sensor_to_pandas(c, query, site_id)

                query = " select time,value from API_Temperature " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_tempC[site_id] = select_single_sensor_to_pandas(c, query, site_id)
        except Error as e:
            print("SQL ERROR:", e)
    return site_list, dict_df_temp, dict_df_motion, dict_df_cloud, dict_df_tempC


if __name__ == "__main__":
    database = '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    site_list, dict_df_temp, dict_df_motion, dict_df_cloud, dict_df_tempC = retrieve_human_active(database, Year=2017, Months=list(range(9, 10)), feq=None)
    orientation_dict = retrieve_orientation(database)

    for site_id in site_list:
        print("********* ", site_id, " *********")
        rooms_orientation = orientation_dict[int(site_id)]
        if rooms_orientation is None:
            print(site_id, " orientation empty")
            continue

        df_original = dict_df_temp[site_id]
        df_temp, room_list, begin = ETL(df_original)
        if -1 == begin:
            print(site_id, " empty")
            continue

        df_motion = dict_df_motion[site_id]
        df_cloud = dict_df_cloud[site_id]
        df_cloud = df_cloud.reindex(df_original.index, method='pad', limit=11)

        df_tempC = dict_df_tempC[site_id]
        df_tempC = df_tempC.reindex(df_original.index, method='pad', limit=11)

        day_index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_original.index.values]
        day_list = sorted(set(day_index))
        df_motion.index = day_index
        df_temp.index = day_index
        df_cloud.index = day_index
        df_tempC.index = day_index

        room_labels = []
        i = 1
        for room_id in room_list:
            for id_label in rooms_orientation:
                if room_id == id_label[0]:
                    room_labels.append("R" + str(i) + "_" + id_label[1] + "_" + str(room_id))
                    i += 1
                    break

        for day_i in day_list[20:30]:

            df_day_i = df_temp.loc[day_i, :]
            df_motion_i = df_motion.loc[day_i]
            df_cloud_i = df_cloud.loc[day_i, :]
            df_tempC_i = df_tempC.loc[day_i, :]

            xmax = df_day_i.shape[0]
            if xmax == 24:
                major_ticks = np.arange(0, 24)
            else:
                major_ticks = np.arange(0, xmax, 12)  # 5mins * 12 = 1hour

            f, axn = plt.subplots(4, 1, sharex=True, squeeze=False)
            axn[0,0].set_title(day_i + " at " + site_id)

            df_day_i.plot(ax=axn[0,0])
            axn[0, 0].legend(room_labels, loc=1)
            axn[0, 0].set_xticks([])

            df_motion_i.plot(ax=axn[1,0],marker=".")
            axn[1, 0].legend(room_labels, loc=1)
            axn[1, 0].set_xticks([])

            df_cloud_i.plot(ax=axn[2, 0], marker=",")
            axn[2, 0].legend(["cloud"], loc=1)
            axn[2, 0].set_xticks([])

            df_tempC_i.plot(ax=axn[3, 0], marker="o")
            axn[3, 0].legend(["TempOut"], loc=1)
            axn[3, 0].set_xticks([])

            plt.xlim(0, xmax)
            plt.xticks(major_ticks, list(range(24)))
            f.set_size_inches(18.5, 7.5)
            plt.savefig(day_i + "_" + site_id + '.png', dpi=400)
        plt.close('all')
    # plt.show()
