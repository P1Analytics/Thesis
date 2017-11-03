import matplotlib.pyplot as plt
import numpy as np
import operator
import pandas as pd
import requests
import seaborn as sns
import time
import warnings
from collections import Counter
from pylab import *
from sklearn.linear_model import LinearRegression
from comfort_models import *
import json

sns.set()
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def outliers_Q1_Q3(df, head):
    try:
        Q1 = df.quantile(0.25)
        Q3 = df.quantile(0.75)
        upper = Q3 + (Q3 - Q1) * 3
        lower = Q1 - (Q3 - Q1) * 3
        mean = df.mean()
    except TypeError:
        print("TypeError from: ", head)
    pd.options.mode.chained_assignment = None  # TODO is there a better solution ?

    outliers = 0
    if df[upper < df].size > 0:
        outliers += df[upper < df].size
        df[upper < df] = df.quantile(1)

    if df[lower > df].size > 0:
        outliers += df[lower > df].size
        df[lower > df] = df.quantile(0)

    return df, mean, outliers


def outliers_sliding_window(df, window_number):
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
        if len(previous) < window_number - 1:
            if np.isnan(item):
                outlier += 1
                window[-1] = average
                df[i] = average
                # print(item, "change to mean", average)
            continue

        Q1 = np.percentile(previous, 25)
        Q3 = np.percentile(previous, 75)
        upper = Q3 + (Q3 - Q1) * 3
        lower = Q1 - (Q3 - Q1) * 3
        last_min = np.percentile(previous, 0)
        last_max = np.percentile(previous, 100)
        min = np.percentile(window, 0)
        max = np.percentile(window, 100)
        now_range = max - min
        if max_range < now_range:
            max_range = now_range

        if np.isnan(item):
            outlier += 1
            window[-1] = average
            df[i] = average
            # print(item, "change to mean", average)
            continue
        if now_range > max_range * 0.85:  # new peak comes out, OTHERWISE keep calm and carry on
            if item < lower :
                # print(item, "change to min", min)
                window[-1] = last_min
                df[i] = last_min
                outlier += 1
                continue
            if item > upper:
                # print(item, "change to max", max)
                window[-1] = last_max
                df[i] = last_max
                outlier += 1
                continue
    return df, outlier, average


def sun_rise_set(lat, lng, timestamp):
    timezone_url = "https://maps.googleapis.com/maps/api/timezone/json?location=" + str(lat) + "," + str(lng) + \
                   "&timestamp=" + str(timestamp) + \
                   "&key=AIzaSyAI4--_x4AE2K5zZ6Z5tZafwwpVI9uYlYM"

    print(timezone_url)
    rs = requests.get(timezone_url).json()

    # print(json.dumps(rs, sort_keys=True, indent=4))  # human-readable response :)


    GMT = int((int(rs["dstOffset"]) + int(rs["rawOffset"])) / 3600)

    rise_set_url = "https://api.sunrise-sunset.org/json?lat=" + str(lat) + "&lng=" + str(lng) + \
                   "&date=" + time.strftime("%D", time.localtime(timestamp))
    rise_set = requests.get(rise_set_url).json()
    sunrise = int(rise_set["results"]["sunrise"].split(":")[0]) + GMT
    noon = int(rise_set["results"]["solar_noon"].split(":")[0]) + GMT
    sunset = int(rise_set["results"]["sunset"].split(":")[0]) + 12 + 1 + GMT

    return sunrise, noon, sunset


def ETL(filename, statistic=False):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)  #

    if statistic:
        df_norm = (df - df.min()) / (df.max() - df.min())
        index_month = [int(str(i).split("-")[1]) for i in df.index.date]
        df_norm.index = index_month

    active_time = []  # some sensors are 0
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
    # print("POWER OFF time", (1 - df[(df.T != 0).any()].shape[0] / df.shape[0]) * 100, "%") # could be easier ?

    active_sensor = []  # sometimes is 0
    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            df[head] = df[head].replace(0, np.nan)
            # print("dead sensor ", head)
        else:
            active_sensor.append(head)

    if statistic:
        begin = df.index.get_loc(active_time[0])
        print(filename, begin, active_time[0])
        sum_nan = sum(df.iloc[begin:].isnull().sum().values)
        power_on = df.iloc[begin:].shape
        result = sum_nan / power_on[0] / power_on[1] * 100
        miss_ratio = ("%.2f" % result)+ "%"
        return df_norm, miss_ratio

    # Here is the smooth out : delete outliers + rolling windows
    sum_outliers = 0
    for head in list(df):
        df_col = df[head]
        if df_col.isnull().sum() == df_col.size:
            df = df.drop(head, 1)

        df_col, outliers, average = outliers_sliding_window(df_col, window_number=20)
        sum_outliers += outliers


        df_col = df_col.rolling(window=6, center=False, min_periods=0).mean()
        df_col = df_col.fillna(average)
        df[head + "_rollingwindow"] = df_col
    result = sum_outliers / df.shape[0] / df.shape[1] * 100
    outlierS = ("%.2f" % result)+ "%"
    df.to_csv(filename.split(".")[0] + "_XXX.csv", sep=";")
    return df, active_sensor, outlierS


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

    # heatmap for STATISTIC of MISSING DATA  for 15 sites , active vs inactive for 2 years
    TwoYEARs_list = [
        # "Libelium.csv",
        # "Synfield.csv",
        # "Electrical.csv",
        "144024_2YEARS.csv",
        "144242_2YEARS.csv",
        "144243_2YEARS.csv",
        "155076_2YEARS.csv",
        "155077_2YEARS.csv",
        "155849_2YEARS.csv",
        "155851_2YEARS.csv",
        "155865_2YEARS.csv",
        "155877_2YEARS.csv",
        "157185_2YEARS.csv",
        "159705_2YEARS.csv",
        "19640_2YEARS.csv",
        "27827_2YEARS.csv",
        "28843_2YEARS.csv",
        "28850_2YEARS.csv"
    ]
    fig, axn = plt.subplots(len(TwoYEARs_list), 1, sharex=True)
    cbar_ax = fig.add_axes([.92, .3, .03, .4]) # [left, bottom, width, height]
    fig.suptitle("Sensor Activity 2015.Nov.01-2017.Oct.30", fontsize=14)
    file_i = 0
    for i, ax in enumerate(axn.flat):
        df_norm,miss_ratio = ETL(TwoYEARs_list[file_i],statistic=True)
        print(miss_ratio)
        label = ""+TwoYEARs_list[file_i].split("_")[0]
        sns.heatmap(df_norm.iloc[2:].T, ax=ax,
                    xticklabels=31,yticklabels=False,
                    cbar=i == 0,cbar_ax=None if i else cbar_ax,
                    cbar_kws={'label': 'inactive(0)------->active(1)'})
        ax.set_ylabel(label, rotation=0,labelpad=50)
        _,_,outliers = ETL(TwoYEARs_list[file_i])
        print(TwoYEARs_list[file_i],outliers)
        file_i += 1
    ax.set_xlabel('month')
    # heatmap for STATISTIC of MISSING DATA END




    """
    df_indoor, working_indoor, _ = ETL("19640_Temp.csv")
    df_outdoor, working_outdoor, _ = ETL("19640_Temp_outdoor.csv")
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
    # lng, lat = 17.05267,61.30486   # Sweden school do not have this data
    lng, lat = 23.8888985,37.9997385 # Ellinogermaniki Agogi S.A.  do not have this data
    sunrise, noon, sunset = sun_rise_set(lat, lng, df_indoor[working_indoor[0]].index[0].timestamp())
    print(list(df_indoor), "\n", sunrise, noon, sunset, lat, lng)
    
    
    print("********* Orientation *************")
    for x in range(26, 52):  # TODO change
        begin = df_indoor.index.get_loc(pd.Timestamp('01/09/2017 ' + str(sunrise) + ':00')) + 2
        end = df_indoor.index.get_loc(pd.Timestamp('01/09/2017 ' + str(sunset) + ':00')) + 2
        gap = 288  # one day for 5 mins 12*5*
        peak = []
        marker = []
        bx = 0
        for i in range(28):  # for 28days,4weeks # TODO change
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
    """



    print("********* Comfortable *************")
    df,_,_ = ETL("demo.csv")
    heads = list(df)
    index = df.index.values
    outdoor = df[heads[2]].values
    indoor = df[heads[3]].values
    comfort=[]
    for ind,ta,out in zip(index,indoor,outdoor):
        comfort.append(comfAdaptiveComfortASH55(ta,ta,out)[5])
    com_df=pd.DataFrame(comfort,index=index,columns=["comfort"])
    # com_df.to_csv("comfort.csv", sep=";")

  
    print("********* replace outdoor with API *************")
    lng, lat  = 29.589258,36.147923
    # real-time
    # r = requests.get('http://api.openweathermap.org/data/2.5/weather?',
    #                  params={'lat': lat, 'lon': lng, 'units': 'metric',
    #                          'APPID': 'bd859500535f9871a59b2fa52547516e'}).json()
    # print("the real time query:\n", r)

    # history
    # TODO API KEY expired on Dec25 https://developer.worldweatheronline.com/api/docs/historical-weather-api.aspx
    URL = "http://api.worldweatheronline.com/premium/v1/past-weather.ashx"
    params = {'q': "36.14792,29.5892",  # TODO need to change
            'date': '2017-10-1', 'enddate': '2017-10-30',
            'key': '612818efa9204368a1785431172610', 'format': 'json',
            'includelocation': 'yes', 'tp': '24'}
    r = requests.get(URL, params).json()
    print(json.dumps(r, sort_keys=True, indent=4)) # human-readable response :)

    list_weather = []
    for i in r["data"]["weather"]:
      for j in i["hourly"]:
          list_weather.append(int(j["humidity"]))
          print(i["date"],int(j["humidity"]))
    print(list_weather,len(list_weather))


    plt.show()
