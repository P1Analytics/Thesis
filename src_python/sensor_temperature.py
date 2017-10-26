import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import warnings
from collections import Counter
import operator
import requests
import time
from comfort_models import *
import json

warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def outliers_Q1_Q3(df, head):
    try:
        Q1 = df.quantile(0.25)
        Q3 = df.quantile(0.75)
        upper = Q3 + (Q3 - Q1) * 3
        lower = Q1 - (Q3 - Q1) * 3
        sub = df.mean()
    except TypeError:
        print("here is the problem of Type Error from: ", head)
    pd.options.mode.chained_assignment = None  # TODO is there a better solution ?

    if df[upper < df].size > 0:
        df[upper < df] = df.quantile(1)
    if df[lower > df].size > 0:
        df[lower > df] = df.quantile(0)
    return df, sub


def sun_rise_set(lat, lng, timestamp):
    timezone_url = "https://maps.googleapis.com/maps/api/timezone/json?location=" + lat + "," + lng + \
                   "&timestamp=" + str(timestamp) + \
                   "&key=AIzaSyAI4--_x4AE2K5zZ6Z5tZafwwpVI9uYlYM"
    rs = requests.get(timezone_url).json()
    GMT = int((int(rs["dstOffset"]) + int(rs["rawOffset"])) / 3600)

    rise_set_url = "https://api.sunrise-sunset.org/json?lat=" + str(lat) + "&lng=" + str(lng) + \
                   "&date=" + time.strftime("%D", time.localtime(timestamp))
    rise_set = requests.get(rise_set_url).json()
    sunrise = int(rise_set["results"]["sunrise"].split(":")[0]) + GMT
    noon = int(rise_set["results"]["solar_noon"].split(":")[0]) + GMT
    sunset = int(rise_set["results"]["sunset"].split(":")[0]) + 12 + 1 + GMT

    return sunrise, noon, sunset


def clean_data(filename, sensor_type="temperature"):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)
    header = list(df)
    working_sensors = []

    # clear the rows with all 0s due to power-off,also we might don't need it due to moving windows
    # df = df[(df.T != 0).any()]

    for head in header:
        df_col = df[head]
        if (df_col[df_col != 0]).size == 0:
            df = df.drop(head, 1)
            continue
        if "temperature" == sensor_type or "humidity" == sensor_type:
            df_col = df_col.replace(0, np.nan)
        df_col, mean = outliers_Q1_Q3(df_col, head)

        df_col = df_col.rolling(window=36, center=False, min_periods=0).mean()
        df_col = df_col.fillna(mean)
        window_col = head + "_window36"
        df[window_col] = df_col
        working_sensors.append(head)

    # df.to_csv(filename.split(".")[0] + "_output.csv", sep=";") # to save the after-ETL data
    return df, working_sensors


def coordinate_dicts():
    global coordinates
    with open("coordinates.txt") as f:
        lines = f.read().splitlines()
    coordinates = {}
    for line in lines:
        coordinates[line.split()[0]] = [line.split()[1], line.split()[2]]
    # print(coordinates)
    return coordinates


if __name__ == "__main__":

    df_indoor, working_indoor = clean_data("155877_Temperature_indoor.csv")
    df_outdoor, working_outdoor = clean_data("155877_Temperature_outdoor.csv")

    coef = []
    inter = []
    for outdoor in working_outdoor:
        X = df_outdoor[outdoor]
        X = X.values.reshape(np.shape(X)[0], 1)
        for indoor in working_indoor:
            Y = df_indoor[indoor]
            Y = Y.values.reshape(np.shape(Y)[0], 1)
            clf = LinearRegression().fit(X, Y)
            coef.append(clf.coef_[0][0])
            inter.append(clf.intercept_[0])
            # print(clf.coef_[0][0],clf.intercept_[0],indoor)

            label_trend = "trend_" + indoor
            df_indoor[label_trend] = clf.predict(X)
            label_detrend = "detrend_" + indoor
            df_indoor[label_detrend] = df_indoor[indoor] - df_indoor[label_trend] + df_indoor[label_trend].mean()
            rms = np.std(df_indoor[label_detrend])
            # plt.scatter(coef, inter, marker='.')
            # df_indoor.to_csv(indoor.split("_")[0] + "_trend.csv", sep=";")

    head = list(df_indoor)
    coordinate_dict = coordinate_dicts()
    site = working_outdoor[0].split("_")[0]
    lng, lat = coordinate_dict[site][0], coordinates[site][1]

    sunrise, noon, sunset = sun_rise_set(lat, lng, df_indoor[working_indoor[0]].index[0].timestamp())
    print(list(df_indoor), "\n", sunrise, noon, sunset, lat, lng)

    print("********* Orientation *************")
    start = datetime.time(sunrise, 0, 0)
    end = datetime.time(sunset, 0, 0)
    for id in [1, ]:
        for x in range(3, 6):
            begin = df_indoor.index.get_loc(pd.Timestamp('15/09/2017 07:00')) + 2
            end = df_indoor.index.get_loc(pd.Timestamp('15/09/2017 20:00')) + 2
            gap = 288  # one day for 5 mins 12*5*
            peak = []
            marker = []
            bx = 0
            for i in range(28):  # for 28days,4weeks
                delta = i * gap
                df = df_indoor.iloc[begin + delta:end + delta]
                peak.append(df[head[x]].groupby(pd.TimeGrouper('D')).idxmax().dt.hour.values[0])
                marker.append(np.datetime64(df[head[x]].groupby(pd.TimeGrouper('D')).idxmax().dt.to_pydatetime()[0]))
            # print(marker)
            print("new peak list", sorted(dict(Counter(peak)).items(), key=operator.itemgetter(1), reverse=True))
            print(head[x])
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
            if east:
                print("Might facing to east, we have warming mornings at", sorted(set(morning)), "ratio:",
                      peak_in_morning_ratio * 100, "%")
            Ori = sum / len(peak)
            print(Ori)
            if 0 <= Ori < sunrise / 24 * 360:
                print("North|North-East")
            if 0.5 < peak_in_morning_ratio or sunrise / 24 * 360 <= Ori < noon / 24 * 360:
                print("East|South-East")
            if noon / 24 * 360 <= Ori < sunset / 24 * 360:
                print("South|South-West")
            if sunset / 24 * 360 <= Ori < 359:
                print("West|North-West")
            print("**********************")

    print("********* Comfortable *************")
    for i in range(5, 50):
        print(i, comfAdaptiveComfortASH55(ta=29.4, tr=i, runningMean=25.6, vel=1, eightyOrNinety=True,
                                          levelOfConditioning=0)[4])

    print("********* similarity for rooms *************")
    # TODO find the similarity for rooms


    print("********* replace outdoor with API *************")

    # real-time
    r = requests.get('http://api.openweathermap.org/data/2.5/weather?',
                     params={'lat': lat, 'lon': lng, 'units': 'metric',
                             'APPID': 'bd859500535f9871a59b2fa52547516e'}).json()
    print("the real time query:\n", r)

    # history
    # TODO API KEY expired on Dec25 https://developer.worldweatheronline.com/api/docs/historical-weather-api.aspx
    URL = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"
    params = {'q': lat + "," + lng, 'date': '2017-07-20', 'enddate': '2017-10-21',
              'key': '612818efa9204368a1785431172610', 'format': 'json',
              'includelocation': 'yes', 'tp': '1'}
    r = requests.get(URL, params).json()
    feel_like_temp_list = []
    for i in r["data"]["weather"]:
        for j in i["hourly"]:
            feel_like_temp_list.append(j["FeelsLikeC"])
    print(feel_like_temp_list)
    # print(json.dumps(r, sort_keys=True, indent=4)) # human-readable response :)


    plt.show()
