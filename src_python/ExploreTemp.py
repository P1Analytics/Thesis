import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
from sklearn.linear_model import LinearRegression
import warnings, json
from pylab import *

warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")
from collections import defaultdict


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

    # print("******************************", df.name, df.size, outlier,outlier_NaN, outlier_NaN / outlier)
    return df, outlier, average


def ETL(filename, statistic=False):
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)  #
    df = df[~df.index.duplicated(keep='first')]

    if statistic:
        df_norm = (df - df.min()) / (df.max() - df.min())  # Normalized different scale for different sensor
        index_month = [int(str(i).split("-")[1]) for i in df.index.date]
        df_norm.index = index_month

    active_time = []  # some sensors are 0, never active
    dfT = df.T
    check_earliest = True
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
    # print("POWER OFF time", (1 - df[(df.T != 0).any()].shape[0] / df.shape[0]) * 100, "%") # could be easier ?

    active_sensor = []  # sometimes is 0
    for head in list(df):
        df_col = df[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            # df[head] = df[head].replace(0, np.nan)
            df = df.drop(head, 1)
            # print("dead sensor ", head)
        else:
            active_sensor.append(head)
    if len(active_time)>0:
        begin = df.index.get_loc(active_time[0])
        df_power = df.iloc[begin:]
        sum_nan = sum(df_power.isnull().sum().values)  # Define:  all the NaN data are "missing data"
        power_on = df_power.shape
        result = sum_nan / power_on[0] / power_on[1] * 100

    if statistic:
        print(begin, active_time[0])
        print(filename)
        print(">>>>>>>statistic : active time", len(active_time) / df.shape[0] * 100, "%")
        print(">>>>>>>statistic : active sensor", len(head) / df.shape[1] * 100, "%")
        miss_ratio = ("%.2f" % result) + "%"
        return df_norm, miss_ratio

    # Here is the smooth out : delete outliers + rolling windows
    sum_outliers = 0
    outlierS = np.nan
    if len(active_time)>0:
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

    return df, active_sensor, outlierS


def Orientation():
    with open("orientation.txt", encoding="utf-8") as f:
        lines = f.read().splitlines()
    orientation = defaultdict(list)
    for line in lines:
        orientation[line.split()[0]].append([line.split()[1], line.split()[2]])
    return orientation


if __name__ == "__main__":

    orientation = Orientation()

    school_list = [
        "144024_Temperatur2months.csv",
        "144242_Temperatur2months.csv",
        "144243_Temperatur2months.csv",
        "155076_Temperatur2months.csv",
        "155849_Temperatur2months.csv",
        "155851_Temperatur2months.csv",
        "155865_Temperatur2months.csv",
        "155877_Temperatur2months.csv",
        "157185_Temperatur2months.csv",
        "159705_Temperatur2months.csv",
        "19640_Temperatur2months.csv",
        "27827_Temperatur2months.csv",
        "28843_Temperatur2months.csv",
        "28850_Temperatur2months.csv",
        # hourly
        # "144024_Temperature.csv",
        # "144243_Temperature.csv",
        # "144242_Temperature.csv",
        # "19640_Temperature.csv",
        # "27827_Temperature.csv",
        # "155849_Temperature.csv",
        # "155851_Temperature.csv",
        # "155076_Temperature.csv",
        # "155865_Temperature.csv",
        # "155877_Temperature.csv",
        # "157185_Temperature.csv",
        # "159705_Temperature.csv"
    ]

    # df_API_Cloud = pd.read_csv("API_cloudcover_hourly.csv", delimiter=";", index_col='timestamps', parse_dates=True)
    df_API_Cloud = pd.read_csv("API_cloudcover_hourlyFull.csv", delimiter=";", index_col='timestamps', parse_dates=True)

    df_API_Temp = pd.read_csv("API_tempC.csv", delimiter=";", index_col='timestamps', parse_dates=True)

    for site_i in school_list:
        print(site_i)
        site_id = site_i.split("_")[0]
        room_id_ori = orientation[site_id]

        df_cloud = df_API_Cloud.loc[:, site_id]
        df_tempC = df_API_Temp.loc[:, site_id]

        df_raw_i = pd.read_csv(site_i, delimiter=";", index_col='timestamps', parse_dates=True)
        df_raw_i = df_raw_i[~df_raw_i.index.duplicated(keep='first')]

        df_site_i, room_list, _ = ETL(site_i)

        df_cloud.index = pd.DatetimeIndex(df_cloud.index)
        df_cloud = df_cloud.reindex(df_site_i.index, method='pad', limit=11)

        df_tempC.index = pd.DatetimeIndex(df_tempC.index)
        df_tempC = df_tempC.reindex(df_site_i.index, method='pad', limit=11)

        xaxis = df_site_i.index.values
        xticks = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in xaxis]
        day_list = sorted(set(xticks))

        # # plot histgram for ETL Data
        # f, axn = plt.subplots(len(room_list),1, sharex=True,squeeze=False)
        # i = 0
        # for room_i in room_list:
        #     axn[i,0].hist(df_site_i[room_i].dropna().values,bins=50, alpha=0.5, color='b')
        #     axn[i,0].grid(color='grey', linestyle='-', linewidth=0.5)
        #     axn[i,0].set_ylabel("Room_" + str(i+1), rotation=90, labelpad=5)
        #     i = i + 1
        # axn[i-1,-1].set_xlabel('Temperature')
        # axn[0,0].set_title(site_id)
        #
        # correlation with Cloud coverage or outdoor temperature
        # coef = []
        # inter = []
        # X = df_cloud
        # X = X.values.reshape(np.shape(X)[0], 1)
        # fig = plt.figure()
        # ax = fig.add_subplot(1, 1, 1)
        # for room_i in room_list:
        #     Y = df_site_i[room_i]
        #     print(Y.isnull().sum())
        #     Y = Y.fillna(mean(Y.dropna().values))
        #     Y = Y.values.reshape(np.shape(Y)[0], 1)
        #     print(len(X),len(Y))
        #     clf = LinearRegression().fit(X, Y)
        #     coef.append(clf.coef_[0][0])
        #     inter.append(clf.intercept_[0])
        #     print(room_i,clf.coef_[0][0])
        # ax.scatter(coef, inter)
        # ax.set_yticks([])
        # ax.grid(color='grey', linestyle='-', linewidth=0.5)
        # ax.set_xlabel(room_i+"Indoor temperature correlation with "+"cloud coverage")
        #
        # coef = []
        # inter = []
        # X = df_tempC
        # X = X.values.reshape(np.shape(X)[0], 1)
        # fig = plt.figure()
        # ax = fig.add_subplot(1, 1, 1)
        # for room_i in room_list:
        #     Y = df_site_i[room_i]
        #     Y = Y.fillna(mean(Y.dropna().values))
        #     Y = Y.values.reshape(np.shape(Y)[0], 1)
        #     clf = LinearRegression().fit(X, Y)
        #     coef.append(clf.coef_[0][0])
        #     inter.append(clf.intercept_[0])
        #     print(room_i,clf.coef_[0][0])
        #
        # ax.scatter(coef, inter)
        # ax.set_yticks([])
        # ax.grid(color='grey', linestyle='-', linewidth=0.5)
        # ax.set_xlabel(room_i+" correlation with "+"outdoor temperature")
        #
        #
        # # timeline indoor temperature vs outdoor temperature & cloud cover(%)
        # fig = plt.figure()
        # ax = fig.add_subplot(1, 1, 1)
        # ax.grid(color='grey', linestyle='--', linewidth=0.5)
        # ax.plot(df_site_i)
        # ax.plot(xaxis,df_tempC.values,'b--',label="Outdoor_Temp")
        # another = ax.twinx()
        # another.plot(xaxis,df_cloud.values, 'c--',label="Cloudy")
        # ax.set_title(site_id)
        # plt.legend()


        # # one single day as one subplot for 30 days
        # df_cloud.index=xticks
        # df_site_i.index=xticks
        # fig, axn = plt.subplots(nrows=6, ncols=5,sharex=True,sharey=True)
        # for row in list(range(6)):
        #     for col in list(range(5)):
        #         if day_list:
        #             day_i = day_list[0]
        #             day_list.pop(0)
        #             df_day_i=df_site_i.loc[day_i,:]
        #             df_cloud_i=df_cloud.loc[day_i]
        #             df_day_i.plot(ax=axn[row,col],legend=False)
        #             axn[row,col].set_xticks([])
        #             axn[row,col].set_yticks([])
        #
        #             another = axn[row,col].twinx()
        #             another.plot(df_cloud_i.values,'b--')
        #             another.set_yticks([])
        #             another.set_title(day_i)
        # fig.tight_layout()
        # # plt.savefig('36days'+site_id+'.png',dpi=400)


        # one single day as one plot for 30 days

        if not room_id_ori:
            print("we dont know the real orientation for this site", site_id)
            continue

        df_cloud.index = xticks
        df_site_i.index = xticks
        df_raw_i.index = xticks
        df_tempC.index = xticks

        room_ids = [label.split("_")[-3] for label in room_list]
        room_labels = []
        i = 1
        for room_id in room_ids:
            for id_label in room_id_ori:
                if room_id in id_label:
                    room_labels.append("R" + str(i) + "_" + id_label[1] + "_" + room_id)
            i += 1

        for day_i in day_list[0:30]:
            df_day_i = df_site_i.loc[day_i, :]
            xmax = df_day_i.shape[0]
            df_day_i_raw = df_raw_i.loc[day_i, :]
            df_cloud_i = df_cloud.loc[day_i]
            df_temp_i = df_tempC.loc[day_i]

            fig = plt.figure()
            ax = fig.add_subplot(1, 1, 1)

            df_day_i.plot(ax=ax)
            ax.legend(room_labels, loc=1)

            raw = ax.twinx()
            raw.set_yticks([])
            df_day_i_raw.plot(ax=raw, legend=None, marker='.')

            cloud = ax.twinx()
            df_cloud_i.plot(ax=cloud, color='b', marker='o')
            cloud.legend(["Cloud"], loc=4)

            temp = ax.twinx()
            df_temp_i.plot(ax=temp, color='y', marker='o')
            temp.set_yticks([])
            temp.legend(["Outdoor"], loc=2)

            if xmax == 24:
                major_ticks = np.arange(0,24)
            else:
                major_ticks = np.arange(0, xmax, 12)
            plt.xlim(0, xmax)
            plt.xticks(major_ticks,list(range(24)))

            ax.set_title(day_i + " at " + site_id)

            fig.set_size_inches(18.5, 7.5)
            plt.savefig(day_i+"_"+site_id+'.png',dpi=400)
    plt.close('all')
    # plt.show()
