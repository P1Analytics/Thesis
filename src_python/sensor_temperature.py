import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import warnings
from collections import Counter
import operator
import scipy.stats.stats as stats
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
    # we reset the ourliers as boundary
    if df[upper < df].size > 0:
        print("hmmm", df[upper < df])
        df[upper < df] = df.quantile(1)
    if df[lower > df].size > 0:
        print("hmmm", df[lower > df])
        df[lower > df] = df.quantile(0)
    return df, sub


def outliers_normal_distrib(df):
    return


def comportable(df):
    comfort_zone = df[df > 18 or df < 26]
    print(comfort_zone.size)
    return


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
    sunset = int(rise_set["results"]["sunset"].split(":")[0]) + 12 + GMT

    return sunrise, noon, sunset


def Orient(df, lat, lng):
    # Orientation level 1  only check the day time peak
    # todo in one day / one week /one month
    sunrise, noon, sunset = sun_rise_set(lng, lat, df.index[0].timestamp())
    df_daytime = df.between_time(str(sunrise) + ":00", str(sunset) + ":00")
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
    morning_ratio = morning_counter / len(peak_list)
    if east:
        print("Might facing to east, we have warming mornings at", sorted(set(morning)), "ratio:", morning_ratio * 100, "%")
    Ori = sum / len(peak_list)

    # print ("debug",
    # sunrise, noon, sunset,"\n",
    # peak_list,"\n",
    # Ori,"\n",
    # sorted(dict(Counter(peak_list)).items(), key=operator.itemgetter(1), reverse=True))

    if 0 <= Ori < sunrise / 24 * 360:
        return "North|North-East"
    if morning_ratio > 0.5 or sunrise / 24 * 360 <= Ori < noon / 24 * 360:
        return "East|South-East"
    if noon / 24 * 360 <= Ori < sunset / 24 * 360:
        return "South|South-West"
    if sunset / 24 * 360 <= Ori < 359:
        return "West|North-West"
    return "NULL"


def Slope(df):
    # Orientation level 2 TODO check the slope of the diff
    print(df.diff())
    # slopes = df.diff().div(df.timestamps.diff())
    return


def clean_data(filename, sensor_type="temperature"):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)
    header = list(df)
    working_sensors = []

    # clear the rows with all 0s due to power-off
    # but this is only for temperature|humidity|TBD sensor
    # if "temperature" == sensor_type or "humidity" == sensor_type:
    #     df = df[(df.T != 0).any()]

    for head in header:
        df_col = df[head]

        if (df_col[df_col != 0]).size == 0:
            # print("this is an broken sensor with all zeros: ", head)
            df = df.drop(head, 1)
            continue

        # clear the outliers, <Tukey's fences> aka <interquartile range>
        if "temperature" == sensor_type or "humidity" == sensor_type:
            df_col = df_col.replace(0, np.nan)  # barely the value would be 0
        df_col, sub = outliers_Q1_Q3(df_col, head)

        # moving window average,smooth out short-term fluctuations and highlight longer-term trends or cycles
        df_col = df_col.rolling(window=36, center=False, min_periods=0).mean()
        # df_col = df_col.fillna(sub) # if windows big enough we dont need to refill the NaN again
        df[head] = df_col
        working_sensors.append(head)

    df.to_csv(filename.split(".")[0] + "_output.csv", sep=";")
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

    # df_indoor, working_indoor = clean_data("19640_Temperature_indoor4.csv")

    # df_indoor, working_indoor = clean_data("19640_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("19640_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("19640_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("19640_Luminosity_outdoor.csv")

    # df_indoor, working_indoor = clean_data("144243_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144243_Temperature_outdoor.csv")

    # df_indoor, working_indoor = clean_data("27827_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("27827_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("27827_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("27827_Luminosity_outdoor.csv")


    df_indoor, working_indoor = clean_data("144242_Temperature_indoor.csv")  # we have some classrooms facing east
    df_outdoor, working_outdoor = clean_data("144242_Temperature_outdoor.csv")

    # df_indoor, working_indoor = clean_data("144242_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144242_Luminosity_outdoor.csv")

    # df_indoor, working_indoor = clean_data("155865_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155865_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155865_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155865_Luminosity_outdoor.csv")

    # df_indoor, working_indoor = clean_data("155877_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155877_Temperature_outdoor.csv")

    # df_indoor, working_indoor = clean_data("144024_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144024_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("144024_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("144024_Luminosity_outdoor.csv")

    # df_indoor, working_indoor = clean_data("155851_Temperature_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155851_Temperature_outdoor.csv")
    # df_indoor, working_indoor = clean_data("155851_Luminosity_indoor.csv")
    # df_outdoor, working_outdoor = clean_data("155851_Luminosity_outdoor.csv")



    # spearmanr_list = []
    site=working_outdoor[0].split("_")[0]
    lat, lng = coordinate_dict[site][0], coordinates[site][1]
    check_ori = True

    coef = []
    inter = []
    for outdoor in working_outdoor:
        X = df_outdoor[outdoor]
        print(Orient(X, lat, lng), outdoor)

        X = X.values.reshape(np.shape(X)[0], 1)
        for indoor in working_indoor:
            Y = df_indoor[indoor]
            if check_ori:
                print(Orient(Y, lat, lng), indoor )

            Y = Y.values.reshape(np.shape(Y)[0], 1)
            # spearmanr_list.append({outdoor + "/" + indoor: stats.spearmanr(X, Y)[0]})
            print( stats.spearmanr(X, Y)[0])#,indoor,outdoor,"spearmanr\n")
            clf = LinearRegression().fit(X, Y)
            print(clf.coef_[0][0], clf.intercept_[0],indoor)#, outdoor,"LinearRegression\n")
            #     coef.append(clf.coef_[0][0])
            #     inter.append(clf.intercept_[0])
            #     xfit = df_indoor[working_indoor[0]].to_dense().values.reshape(np.shape(X)[0], 1)
            #     yfit = clf.predict(xfit)
            #     plt.plot(xfit, yfit, label=indoor)
            #     plt.legend(bbox_to_anchor=(1, 1), loc=1, borderaxespad=0)
        check_ori = False

            # print(spearmanr_list)
            # plt.figure()
            # plt.scatter(coef, inter, marker='.')
            # plt.figure()
            # df_indoor.boxplot()
            # df_indoor.hist()
    plt.show()
