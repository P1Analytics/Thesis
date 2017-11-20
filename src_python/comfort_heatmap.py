import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
import requests
import seaborn as sns
from math import *
import time, warnings
from comfort_models import *

sns.set()
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def sun_rise_set(lat, lng, timestamp):
    url = "https://maps.googleapis.com/maps/api/timezone/json?location=" + str(lat) + "," + str(lng) + \
                   "&timestamp=" + str(timestamp) + \
                   "&key=AIzaSyAI4--_x4AE2K5zZ6Z5tZafwwpVI9uYlYM"

    print(url)
    rs = requests.get(url).json()
    # print(json.dumps(rs, sort_keys=True, indent=4))  # human-readable response :)
    GMT = int((int(rs["dstOffset"]) + int(rs["rawOffset"])) / 3600)


    url = "https://api.sunrise-sunset.org/json?lat=" + str(lat) + "&lng=" + str(lng) + \
                   "&date=" + time.strftime("%D", time.localtime(timestamp))
    rs = requests.get(url).json()
    print(json.dumps(rs, sort_keys=True, indent=4))
    input()
    sunrise = int(rs["results"]["sunrise"].split(":")[0]) + GMT
    noon = int(rs["results"]["solar_noon"].split(":")[0]) + GMT
    sunset = int(rs["results"]["sunset"].split(":")[0]) + 12 + 1 + GMT

    return sunrise, noon, sunset


def coordinate_dicts():
    global coordinates
    with open("coordinates.txt") as f:
        lines = f.read().splitlines()
    coordinates = {}
    for line in lines:
        coordinates[line.split()[0]] = [line.split()[1], line.split()[2]]
    # print(coordinates)
    return coordinates


def outliers_sliding_window(df, window_number):
    window = []
    outlier = 0
    max_range = 0
    for i in range(df.size):  # df is df_col
        item = df[i]
        if len(window) == window_number:
            window.pop(0)
        window.append(item)
        previous = window[:-1]
        if len(previous) == 0:
            continue  # TODO what if the first value is NaN ?

        average = np.mean(previous)
        Q1 = np.percentile(previous, 25)
        Q3 = np.percentile(previous, 75)
        upper = Q3 + (Q3 - Q1) * 3 #1.5
        lower = Q1 - (Q3 - Q1) * 3 #1.5
        last_min = np.percentile(previous, 0)
        last_max = np.percentile(previous, 100)
        min = np.percentile(window, 0)
        max = np.percentile(window, 100)
        now_range = max - min
        if max_range < now_range:
            max_range = now_range

        if np.isnan(item) or item == 0: # if temperature always above 0
            outlier += 1
            window[-1] = average
            df[i] = average
            # print(item, "change to max", average)

        if len(previous) < window_number - 1:
            continue

        if now_range > max_range * 0.9:  # new peak comes out, OTHERWISE keep calm and carry on
            outlier += 1
            if item < lower:
                # print(item, "change to min", (last_min + min) / 2)
                window[-1] = (last_min + min) / 2
                df[i] = (last_min + min) / 2
            if item > upper:
                # print(item, "change to max", (last_max + max) / 2)
                window[-1] = (last_max + max) / 2
                df[i] = (last_max + max) / 2

    # print("******************************", df.name, df.size, outlier,outlier_NaN, outlier_NaN / outlier)
    return df, outlier


def ETL(filename, statistic=False):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)

    if statistic:
        df_norm = (df - df.min()) / (df.max() - df.min())  # Normalized different scale for different sensor
        index_month = [int(str(i).split("-")[1]) for i in df.index.date]
        df_norm.index = index_month

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
        pd.options.mode.chained_assignment = None
    df = dfT.T

    active_sensor = []
    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            # df[head] = df[head].replace(0, np.nan)
            df = df.drop(head, 1)
        else:
            active_sensor.append(head)

    if len(active_time) != 0:
        begin = df.index.get_loc(active_time[0])
        df_power = df.iloc[begin:]
        sum_nan = sum(df_power.isnull().sum().values)  # Define:  all the NaN data are "missing data"
        power_on = df_power.shape
        miss_ratio = sum_nan / power_on[0] / power_on[1] * 100
    else:
        df_power = df

    if statistic:
        print(begin, active_time[0])
        # print(filename)
        print(">>>>>>>statistic : active time", len(active_time) / df.shape[0] * 100, "%")
        print(">>>>>>>statistic : active sensor", len(head) / df.shape[1] * 100, "%")
        miss_ratio = ("%.2f" % miss_ratio) + "%"
        return df_norm, miss_ratio

    # Here is the smooth out : delete outliers + rolling windows
    sum_outliers = 0
    for head in list(df_power):
        df_col = df_power[head]
        nan = df_col.isnull().sum()
        df_col, outliers = outliers_sliding_window(df_col, window_number=4)
        sum_outliers += (outliers - nan)

        df_col = df_col.rolling(window=6, center=False, min_periods=0).mean()
        df_col = df_col.fillna(df_col.mean)
        df_power[head] = df_col
    outlierS = sum_outliers / df_power.shape[0] / df_power.shape[1] * 100

    df.iloc[begin:] = df_power

    # df.to_csv(filename.split(".")[0] + "_XXX.csv", sep=";")
    return df, active_sensor, outlierS


if __name__ == "__main__":

    df_api_temp = pd.read_csv("API_Temp.csv", delimiter="\t", index_col='timestamps', parse_dates=True)
    index = df_api_temp.index.tz_localize('CET', ambiguous='infer')

    school_list = [
        "144024_Temperature.csv",
        # "28843
        "144243_Temperature.csv",  # most Y
        # "28850
        "144242_Temperature.csv",
        "19640_Temperature.csv",
        "27827_Temperature.csv",
        "155849_Temperature.csv",  # most N
        "155851_Temperature.csv",
        "155076_Temperature.csv",
        "155865_Temperature.csv",
        # # "155077",
        "155877_Temperature.csv",
        "157185_Temperature.csv",
        "159705_Temperature.csv"
    ]
    site_newname = ['A',
                    # 'B',
                    'C',
                    # 'D',
                    'E', 'F', 'G', 'H', 'I', 'J', 'K',
                    # 'L',
                    'M', 'N', 'O']
    business_week = ['M','T','W','T','F']

    df_all_comfort = pd.DataFrame(index=index)
    for school in school_list:
        outdoor = df_api_temp[school.split("_")[0]].values
        df_rooms, _, _ = ETL(school)
        room_list = list(df_rooms)
        room_num = len(room_list)

        df_comfort_school_X = pd.DataFrame(index=index)
        for room_id in room_list:
            room = df_rooms[room_id].values
            comfort = []
            for ta, out in zip(room, outdoor):
                comfort.append(comfAdaptiveComfortASH55(ta, ta, out)[5])
            df_comfort_school_X["comfort_" + str(room_id)] = comfort
        if school == "144243_Temperature.csv":
            df_comfort_Y = df_comfort_school_X
        if school == "155851_Temperature.csv": #"19640_Temperature.csv":  # ""155849_Temperature.csv":
            df_comfort_N = df_comfort_school_X

        df_all_comfort[school.split("_")[0]] = df_comfort_school_X.sum(axis=1).divide(room_num)

    df_comfort_bday = pd.DataFrame(columns=list(df_all_comfort), dtype=float)
    df_comfort_Y_bday = pd.DataFrame(columns=list(df_comfort_Y), dtype=float)
    df_comfort_N_bday = pd.DataFrame(columns=list(df_comfort_N), dtype=float)

    index_date = sorted(set([str(time).split()[0] for time in df_all_comfort.index]))

    yticks_list = []
    busday_list = []
    for date in index_date:
        begin = df_all_comfort.index.get_loc(pd.Timestamp(date + ' 08:00:00'))
        if 0 <= df_all_comfort.index[begin].dayofweek < 5:  #  Monday-Friday
            busday_list.append(pd.to_datetime(str(date)).strftime('%b-%d'))
            yticks_list.append(business_week[df_all_comfort.index[begin].dayofweek])

            df_comfort_bday = pd.concat([df_comfort_bday, df_all_comfort.iloc[begin:begin + 9]], axis=0)
            df_comfort_Y_bday = pd.concat([df_comfort_Y_bday, df_comfort_Y.iloc[begin:begin + 9]], axis=0)
            df_comfort_N_bday = pd.concat([df_comfort_N_bday, df_comfort_N.iloc[begin:begin + 9]], axis=0)


    df_comfort_bday = df_comfort_bday.groupby(pd.TimeGrouper('D')).mean().dropna(axis=0)
    df_comfort_Y_bday = df_comfort_Y_bday.groupby(pd.TimeGrouper('D')).mean().dropna(axis=0)
    df_comfort_N_bday = df_comfort_N_bday.groupby(pd.TimeGrouper('D')).mean().dropna(axis=0)
    df_list = [df_comfort_Y_bday, df_comfort_N_bday, df_comfort_bday]

    fig, axn = plt.subplots(1, 3, gridspec_kw={'width_ratios': [1, 2, 3]}, sharey=True)
    cbar_ax = fig.add_axes([.91, .3, .03, .4])  # [left, bottom, width, height]

    xticks = [range(1,len(list(df_comfort_Y_bday))),range(1,len(list(df_comfort_N_bday))),site_newname]
    # xticks = [["R1", "R2", "R3", "R4"], ["R1", "R2", "R3", "R4", "R5"], site_newname]
    xlabels = ["SITE C", "SITE I", "Sites"]
    yticks = [pd.to_datetime(str(date)).strftime('%b-%d') for date in df_comfort_bday.index.values]

    i = 0
    for j, ax in enumerate(axn.flat):
        df = df_list[i]
        sns.heatmap(df, #[4:],
                    ax=ax,
                    xticklabels=xticks[i],
                    yticklabels=5,
                    cbar=j == 0,
                    cbar_ax=None if j else cbar_ax
                    )
        ax.set_xlabel(xlabels[i])
        i += 1
    ax.set_yticklabels(busday_list[::5],fontsize=5)#(yticks[0::5]) # ,rotation=90)

    # plot.tick_params(axis='both', which='major', labelsize=10)
    # plot.tick_params(axis='both', which='minor', labelsize=8)
    plt.rc('ytick',labelsize=7)
    # plt.savefig('comfort.png',dpi=4000)
    # plt.close('all')
    plt.show()
