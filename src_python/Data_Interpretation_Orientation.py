import time
from collections import Counter
import operator
import seaborn as sns
sns.set(style="whitegrid")
from Util.Data_Preparation import *

warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def sun_rise_set(lat, lng, timestamp):
    """
    Retrieve the data from API
    :param lat: latitude
    :param lng: longitude
    :param timestamp: because the day time keep changing as time fly
    :return: exact hour for sunset sunrise and noon
    """
    # check the DST offset
    timezone_url = "https://maps.googleapis.com/maps/api/timezone/json?location=" + str(lat) + "," + str(lng) + \
                   "&timestamp=" + str(timestamp) + \
                   "&key=AIzaSyAI4--_x4AE2K5zZ6Z5tZafwwpVI9uYlYM"
    # print(timezone_url)
    rs = requests.get(timezone_url).json()
    # print(json.dumps(rs, sort_keys=True, indent=4))  # human-readable response :)
    GMT = int((int(rs["dstOffset"]) + int(rs["rawOffset"])) / 3600)

    # check the weather
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
    """
    making prediction based on the indoor temperature from the sensor
    :param df_rooms: all rooms temperature of indoor for one site
    :param active_rooms: active sensor list
    :param date_list: checking each day with
    :return: prediction and top 3 peak hour list for later histgram analysis plot
    """
    # find the hottest time per day and top3
    prediction = []
    peak_dict = defaultdict(list)
    peak_top3 = defaultdict(list)
    hottest3 = defaultdict(list)
    sunrise, noon, sunset = sun_rise_set(lat, lng, pd.Timestamp(date_list[0] + ' 01:00').timestamp())
    for date in date_list:
        if '-15' in date or '-30' in date:  # update the sunrise sunset every 15 days
            sunrise, noon, sunset = sun_rise_set(lat, lng, pd.Timestamp(date + ' 01:00').timestamp())
        begin = df_rooms.index.get_loc(date + " " + '{:02d}'.format(sunrise+2) + ':00:00')
        end = df_rooms.index.get_loc(date + " " + '{:02d}'.format(sunset) + ':00:00')
        df = df_rooms.iloc[begin:end]
        for room in active_rooms:
            if pd.isnull(df[room]).sum() == len(df.index.values):
                continue
            df.index = pd.to_datetime(df.index)
            peak_top3[room].append(list(map(int, [pd.to_datetime(str(timestamps)).strftime('%H') for timestamps in df[room].nlargest(3).index.values])))
            # print(df[room].nlargest(3).values)
            hottest3[room].append(list(map(int,df[room].nlargest(3).values)))
            peak_dict[room].append(df[room].groupby(pd.TimeGrouper('D')).idxmax().dt.hour.values[0])


    # check room by room for prediction the orientation
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

        peak_top3[room]=[item for sublist in peak_top3[room] for item in sublist]
        hottest3[room]=[item for sublist in hottest3[room] for item in sublist]
        print(room,
              '{:05.1f}'.format(degree_prediction),
              '{:05.2f}%'.format(morning_peak_ratio * 100),
              "peak hour,frequency:", sorted(dict(Counter(peak)).items(), key=operator.itemgetter(1), reverse=True),
              "temperature range: [",
              '{:05.2f}'.format(np.percentile(df[room].values, 0)),
              '{:05.2f}'.format(np.percentile(df[room].values, 25)),
              '{:05.2f}'.format(np.percentile(df[room].values, 50)),
              '{:05.2f}'.format(np.percentile(df[room].values, 75)),
              '{:05.2f}]'.format(np.percentile(df[room].values, 100)))

    return prediction, peak_top3,hottest3


if __name__ == "__main__":
    # retrieve data from database
    database = '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    site_list, dict_df, _, _, _ = retrieve_data(database, Year=2017, Months=list(range(9, 10)), feq="00:00")
    orientation_dict = retrieve_orientation(database)
    coordinate_dict = retrieve_coordinate(database)

    for site_id in site_list:
        print("********* ", site_id, " *********")
        # validate the data
        rooms_orientation = orientation_dict[int(site_id)]
        if rooms_orientation is None:
            print(site_id, " orientation empty")
            continue
        if coordinate_dict[int(site_id)] is None:
            print(site_id, " coordinate empty")
            continue
        lng, lat = coordinate_dict[int(site_id)]
        df_original = dict_df[site_id]
        df_rooms, active_rooms, begin = ETL(df_original)
        if -1 == begin:
            print(site_id, " empty")
            continue

        # remove the time part from datetime index
        date_list = sorted(
            set([pd.to_datetime(str(timestamps)).strftime('%Y-%m-%d') for timestamps in df_original.index.values]))

        # make prediction based on temperature
        prediction,peak_dict,hot3 = predict_orientation(df_rooms, active_rooms, date_list)
        print("Truth :", rooms_orientation)
        print("Prediction :", prediction)

        # plot the histgram of the distribution of the peak time and temperature
        f, ax_temp = plt.subplots(len(active_rooms), 1, sharex=True, squeeze=False)
        f, ax_peak = plt.subplots(len(active_rooms), 1, sharex=True, squeeze=False)
        f, ax_hot = plt.subplots(len(active_rooms), 1, sharex=True, squeeze=False)
        i = 0
        for room_i in active_rooms:
            print("here is the hottest top 3 temperatrue for Room",room_i, hot3[room_i])
            ax_peak[i, 0].hist(peak_dict[room_i], bins=50, alpha=0.5, color='g')
            ax_hot[i, 0].hist(hot3[room_i], bins=200, alpha=0.5)
            # sns.swarmplot(x=df_rooms.index.values, y=room_i, data=df_rooms)
            ax_temp[i, 0].hist(df_rooms[room_i].dropna().values, bins=50, alpha=0.5, color='b')

            for id,ori in rooms_orientation:
                if room_i == id:
                    orientation = ori
            ax_temp[i, 0].set_ylabel("R_" + orientation+str(room_i), rotation=90, labelpad=5)
            ax_peak[i, 0].set_ylabel("R_" + orientation+str(room_i), rotation=90, labelpad=5)
            ax_hot[i, 0].set_ylabel("R_" + orientation+str(room_i), rotation=90, labelpad=5)
            i += 1

        ax_temp[i - 1, -1].set_xlabel('Temperature')
        ax_temp[0, 0].set_title(site_id)
        ax_peak[i - 1, -1].set_xlabel('Peak time')
        ax_peak[0, 0].set_title(site_id)
        ax_hot[i - 1, -1].set_xlabel('Temperature')
        ax_temp[0, 0].set_title(site_id)
        df_rooms.plot()
        df_rooms.diff().plot()
        df_rooms.plot()
    plt.show()


    # Todo What if all the data at the same time are dots on the 2D ,
    # if the distance between each other suddenly grows too far
