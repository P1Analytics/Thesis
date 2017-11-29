import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pylab import *
import warnings
from sklearn.linear_model import LinearRegression
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")
pd.options.mode.chained_assignment = None


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
            window[-1] = average  # last_max
            df[i] = average  # last_max
            # print(item, "change to max", average)

        if len(previous) < window_number - 1:
            continue

        if now_range > max_range * 0.9:  # 0.8:  # new peak comes out, OTHERWISE keep calm and carry on
            outlier += 1
            if item < lower:
                # print(item, "change to min", (last_min + min) / 2)
                window[-1] = average  # last_min# (last_min + min) / 2
                df[i] = average  # last_min #(last_min + min) / 2
            if item > upper:
                # print(item, "change to max", (last_max + max) / 2)
                window[-1] = average  # last_max# (last_max + max) / 2
                df[i] = average  # last_max #(last_max + max) / 2
    return df, outlier, average


def ETL(filename, statistic=False):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True, dtype=float)

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
    df = dfT.T

    active_sensor = []
    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            df[head] = df[head].replace(0, np.nan)
            # print("dead sensor ", head)
        else:
            active_sensor.append(head)

    begin = df.index.get_loc(active_time[0])
    df_power = df.iloc[begin:]

    if statistic:
        print(begin, active_time[0])
        print(filename)
        print(">>>>>>>statistic : active time", len(active_time) / df.shape[0] * 100, "%")
        print(">>>>>>>statistic : active sensor", len(head) / df.shape[1] * 100, "%")
        miss_ratio = ("%.2f" % result) + "%"
        return df_norm, miss_ratio

    # Here is the smooth out : delete outliers + rolling windows
    sum_outliers = 0
    for head in list(df_power):
        df_col = df_power[head]
        if df_col.isnull().sum() == df_col.size:
            continue  # df = df.drop(head, 1)  # skip dead sensor

        nan = df_col.isnull().sum()
        df_col, outliers, average = outliers_sliding_window(df_col, window_number=4)
        sum_outliers += (outliers - nan)

        df_col = df_col.rolling(window=6, center=False, min_periods=0).mean()
        df_col = df_col.fillna(average)  # (df_col.mean)
        df_power[head] = df_col  # only save data after ETL
    result = sum_outliers / df_power.shape[0] / df_power.shape[1] * 100

    outlierS = ("%.2f" % result) + "%"
    df.iloc[begin:] = df_power

    # df.to_csv(filename.split(".")[0] + "_XXX.csv", sep=";")
    return df, active_sensor, outlierS


if __name__ == "__main__":
    df_API_Temp = pd.read_csv("API_Temp.csv", delimiter="\t", index_col='timestamps', parse_dates=True)
    index = df_API_Temp.index.tz_localize('CET', ambiguous='infer')
    date_list = sorted(set([str(time).split()[0] for time in index]))#[:-1]

    df_peak_API = pd.DataFrame(index=date_list, columns=list(df_API_Temp))
    for date_i in date_list:
        begin = df_API_Temp.index.get_loc(pd.Timestamp(date_i + " " + '6:00'))
        df = df_API_Temp.iloc[begin : begin + 15]
        daily_peak=[]
        for site_i in list(df_API_Temp):
            daily_peak.append(df[site_i].groupby(pd.TimeGrouper('D')).idxmax().dt.hour.values[0])
        df_peak_API.loc[date_i]=daily_peak

    df_API_Cloud = pd.read_csv("API_cloudcover_daily.csv", delimiter=";", index_col='timestamps', parse_dates=True)
    df_API_Cloud.index = sorted(set([str(time).split()[0] for time in index]))
    school_list = [
        # "144024_Temperature.csv",
        # "144243_Temperature.csv",
        # "144242_Temperature.csv",
        # "19640_Temperature.csv",
        # "27827_Temperature.csv",
        # "155849_Temperature.csv",
        # "155851_Temperature.csv",
        # "155076_Temperature.csv",
        "155865_Temperature.csv",
        # "155877_Temperature.csv",
        # "157185_Temperature.csv",
        # "159705_Temperature.csv"
    ]

    xticks = [pd.to_datetime(date).strftime('%b-%d') for date in date_list]
    for school_i in school_list:
        coef = []
        inter = []
        df_school_i = pd.read_csv(school_i, delimiter=";", index_col='timestamps', parse_dates=True)
        df_indoor, room_list, _ = ETL(school_i)
        school_i = school_i.split("_")[0]

        df_cloud = df_API_Cloud.loc[:,school_i]
        df_cloud = df_cloud.drop(df_cloud.index[len(df_cloud)-1]) # drop the last row Nov-4

        df_peak_i = pd.DataFrame(index=date_list, columns=list(room_list))
        for date_i in date_list:
            begin = df_indoor.index.get_loc(pd.Timestamp(date_i + " " + '9:00'))
            df = df_indoor.iloc[begin: begin + 12]
            daily_peak = []
            for room_i in room_list:
                daily_peak.append(df[room_i].groupby(pd.TimeGrouper('D')).idxmax().dt.hour.values[0])
            df_peak_i.loc[date_i]=daily_peak

        room = ['R1_SW', 'R2_SW', 'R3_NE','R4_NE']
        if len(room_list) > 1 :
            i = 0
            fig, axn = plt.subplots(len(room_list)+1, 1, sharex=True)
            df_cloud.plot(ax=axn[0])
            axn[0].set_ylabel("Cloudy(%)")
            for ax_i in axn[1:]:
                room_i = room_list[i]
                diff= (df_peak_API.loc[:,school_i]-df_peak_i.loc[:,room_i])#.abs()
                diff.plot(ax=ax_i)
                ax_i.set_ylabel(room[i])
                # ax_i.set_ylabel(room_i.split("_")[1])


                df_cloud = df_API_Cloud.loc[:,school_i]
                df_indoor_i = df_peak_i.loc[:,room_i]

                # clf = LinearRegression().fit(df_cloud.values.reshape(np.shape(df_cloud)[0], 1), diff.values.reshape(np.shape(diff)[0], 1))
                clf = LinearRegression().fit(df_cloud.values.reshape(np.shape(df_cloud)[0], 1), df_indoor_i.values.reshape(np.shape(df_indoor_i)[0], 1))
                print(room[i],clf.coef_[0][0],clf.intercept_[0])

                coef.append(clf.coef_[0][0])
                inter.append(clf.intercept_[0])

                i +=1
            plt.xticks(range(len(xticks)),xticks, rotation='vertical')
        else:
            diff.plot()
        # plt.suptitle(school_i)
        plt.figure()
        plt.scatter(coef,inter)
    plt.show()
