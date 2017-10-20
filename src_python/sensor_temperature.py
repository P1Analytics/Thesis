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


def Orient(df, lat, lng):
    # Orientation level 1  only check the day time peak
    sunrise, noon, sunset = sun_rise_set(lng, lat, df.index[0].timestamp())
    start = datetime.time(sunrise, 0, 0)
    end = datetime.time(sunset, 0, 0)
    df_daytime = df.between_time(start, end)

    peak_list = []
    for index, value in df_daytime.nlargest(100).iteritems():
        peak_list.append(index.hour)

    sum = 0
    east = False
    morning = []
    morning_counter = 0
    for hour in peak_list:
        sum += hour / 24 * 360
        if sunrise <= hour < noon - 2:
            east = True
            morning.append(hour)
            morning_counter += 1

    peak_in_morning_ratio = morning_counter / len(peak_list)
    if east:
        print("Might facing to east, we have warming mornings at", sorted(set(morning)), "ratio:",
              peak_in_morning_ratio * 100, "%")
    Ori = sum / len(peak_list)
    #
    # print ("debug",
    #        sunrise, noon, sunset, "\n",
    #        peak_list, "\n",
    #        Ori, "\n",
    #        sorted(dict(Counter(peak_list)).items(), key=operator.itemgetter(1), reverse=True))

    if 0 <= Ori < sunrise / 24 * 360:
        return "North|North-East"
    if 0.5 < peak_in_morning_ratio or sunrise / 24 * 360 <= Ori < noon / 24 * 360:
        return "East|South-East"
    if noon / 24 * 360 <= Ori < sunset / 24 * 360:
        return "South|South-West"
    if sunset / 24 * 360 <= Ori < 359:
        return "West|North-West"
    return "NULL"


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
        df[head] = df_col
        working_sensors.append(head)

    # df.to_csv(filename.split(".")[0] + "_output.csv", sep=";")
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

    coordinate_dict = coordinate_dicts()

    df_indoor, working_indoor = clean_data("27827_Temperature_indoor.csv")
    df_outdoor, working_outdoor = clean_data("27827_Temperature_outdoor.csv")

    site = working_outdoor[0].split("_")[0]
    lat, lng = coordinate_dict[site][0], coordinates[site][1]

    coef = []
    inter = []
    check = True
    for outdoor in working_outdoor:
        X = df_outdoor[outdoor]
        print(Orient(X, lat, lng), outdoor)
        X = X.values.reshape(np.shape(X)[0], 1)
        # print("unbiased variance", outdoor, np.std(X))
        for indoor in working_indoor:
            Y = df_indoor[indoor]
            if check:
                print(Orient(Y, lat, lng), indoor)
            # print("unbiased variance", indoor, np.std(Y))
            Y = Y.values.reshape(np.shape(Y)[0], 1)
            clf = LinearRegression().fit(X, Y)
            coef.append(clf.coef_[0][0])
            inter.append(clf.intercept_[0])

            label_trend = "trend_" + indoor
            df_indoor[label_trend] = clf.predict(X)
            label_detrend = "detrend" + indoor
            df_indoor[label_detrend] = df_indoor[indoor] - df_indoor[label_trend] + df_indoor[label_trend].mean()
            rms = np.std(df_indoor[label_detrend])
        check = False
        # plt.scatter(coef, inter, marker='.')
        # df_indoor.to_csv(indoor.split("_")[0] + "_trend.csv", sep=";")
    # plt.show()


