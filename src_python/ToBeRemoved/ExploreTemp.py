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

        if len(previous) < window_number - 1:
            continue

        if now_range > max_range * 0.9:  # 0.8:  # new peak comes out, OTHERWISE keep calm and carry on
            outlier += 1
            if item < lower:
                window[-1] = average # last_min# (last_min + min) / 2
                df[i] = average  # last_min #(last_min + min) / 2
            if item > upper:
                window[-1] = average # (last_max + max) / 2
                df[i] = average # last_max #(last_max + max) / 2

    # print("******************************", df.name, df.size, outlier,outlier_NaN, outlier_NaN / outlier)
    return df, outlier, average


def ETL_df(df):
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
        # df_col = df_col.fillna(average)
        df_power[head] = df_col

    df.iloc[begin:] = df_power
    return df, list(df), begin


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
        # 5mins
        # "144024_Temperatur2months.csv",
        # "144242_Temperatur2months.csv",
        # "144243_Temperatur2months.csv",
        # "155076_Temperatur2months.csv",
        # "155849_Temperatur2months.csv",
        # "155851_Temperatur2months.csv",
        "155865_Temperatur2months.csv",
        "155877_Temperatur2months.csv",
        "157185_Temperatur2months.csv",
        "159705_Temperatur2months.csv",
        "19640_Temperatur2months.csv",
        "27827_Temperatur2months.csv",
        "28843_Temperatur2months.csv",
        "28850_Temperatur2months.csv",

    ]

    df_API_Cloud = pd.read_csv("API_cloudcover_hourlyFull.csv", delimiter=";", index_col='timestamps', parse_dates=True)
    df_API_Temp = pd.read_csv("API_tempC.csv", delimiter=";", index_col='timestamps', parse_dates=True)

    for site_i in school_list:
        print(site_i)
        site_id = site_i.split("_")[0]
        room_id_ori = orientation[site_id]


        df_raw_i = pd.read_csv(site_i, delimiter=";", index_col='timestamps', parse_dates=True)
        df_raw_i = df_raw_i[~df_raw_i.index.duplicated(keep='first')]
        df_site_i, room_list, _ = ETL_df(df_raw_i)

        # double check the ETL filter effection
        for i in room_list:
            plt.figure()
            (df_raw_i[i] - df_site_i[i]).plot()
            df_raw_i[i].plot()
            df_site_i[i].plot()

        # Expanding the cloud or temperature from API sampled by 1 hour to 5mins interval
        df_cloud = df_API_Cloud.loc[:, site_id]
        df_cloud.index = pd.DatetimeIndex(df_cloud.index)
        df_cloud = df_cloud.reindex(df_site_i.index, method='pad', limit=11)

        df_tempC = df_API_Temp.loc[:, site_id]
        df_tempC.index = pd.DatetimeIndex(df_tempC.index)
        df_tempC = df_tempC.reindex(df_site_i.index, method='pad', limit=11)

        xaxis = df_site_i.index.values
        day_index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in xaxis]
        day_list = sorted(set(day_index))

        # plot histgram for ETL Data
        f, axn = plt.subplots(len(room_list),1, sharex=True,squeeze=False)
        i = 0
        for room_i in room_list:
            axn[i,0].hist(df_site_i[room_i].dropna().values,bins=50, alpha=0.5, color='b')
            axn[i,0].grid(color='grey', linestyle='-', linewidth=0.5)
            axn[i,0].set_ylabel("Room_" + str(i+1), rotation=90, labelpad=5)
            i = i + 1
        axn[i-1,-1].set_xlabel('Temperature')
        axn[0,0].set_title(site_id)

        # correlation with Cloud coverage or outdoor temperature
        coef = []
        inter = []
        X = df_cloud
        X = X.values.reshape(np.shape(X)[0], 1)
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        for room_i in room_list:
            Y = df_site_i[room_i]
            print(Y.isnull().sum())
            Y = Y.fillna(mean(Y.dropna().values))
            Y = Y.values.reshape(np.shape(Y)[0], 1)
            print(len(X),len(Y))
            clf = LinearRegression().fit(X, Y)
            coef.append(clf.coef_[0][0])
            inter.append(clf.intercept_[0])
            print(room_i,clf.coef_[0][0])
        ax.scatter(coef, inter)
        ax.set_yticks([])
        ax.grid(color='grey', linestyle='-', linewidth=0.5)
        ax.set_xlabel(room_i+"Indoor temperature correlation with "+"cloud coverage")

        coef = []
        inter = []
        X = df_tempC
        X = X.values.reshape(np.shape(X)[0], 1)
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        for room_i in room_list:
            Y = df_site_i[room_i]
            Y = Y.fillna(mean(Y.dropna().values))
            Y = Y.values.reshape(np.shape(Y)[0], 1)
            clf = LinearRegression().fit(X, Y)
            coef.append(clf.coef_[0][0])
            inter.append(clf.intercept_[0])
            print(room_i,clf.coef_[0][0])

        ax.scatter(coef, inter)
        ax.set_yticks([])
        ax.grid(color='grey', linestyle='-', linewidth=0.5)
        ax.set_xlabel(room_i+" correlation with "+"outdoor temperature")


        # timeline indoor temperature vs outdoor temperature & cloud cover(%)
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        ax.grid(color='grey', linestyle='--', linewidth=0.5)
        ax.plot(df_site_i)
        ax.plot(xaxis,df_tempC.values,'b--',label="Outdoor_Temp")
        another = ax.twinx()
        another.plot(xaxis,df_cloud.values, 'c--',label="Cloudy")
        ax.set_title(site_id)
        plt.legend()

        # one single day as one subplot for 30 days
        df_cloud.index=day_index
        df_site_i.index=day_index
        fig, axn = plt.subplots(nrows=6, ncols=5,sharex=True,sharey=True)
        for row in list(range(6)):
            for col in list(range(5)):
                if day_list:
                    day_i = day_list[0]
                    day_list.pop(0)
                    df_day_i=df_site_i.loc[day_i,:]
                    df_cloud_i=df_cloud.loc[day_i]
                    df_day_i.plot(ax=axn[row,col],legend=False)
                    axn[row,col].set_xticks([])
                    axn[row,col].set_yticks([])

                    another = axn[row,col].twinx()
                    another.plot(df_cloud_i.values,'b--')
                    another.set_yticks([])
                    another.set_title(day_i)
        fig.tight_layout()
        # plt.savefig('36days'+site_id+'.png',dpi=400)

        # one single day as one plot for 30 days
        if not room_id_ori:
            print("we dont know the real orientation for this site", site_id)
            continue

        df_site_i.index = day_index
        df_raw_i.index = day_index
        df_tempC.index = day_index
        df_cloud.index = day_index

        room_labels = []
        new_room_list = []
        i = 1
        for head in room_list:
            room_id = head.split("_")[-3]
            for id_label in room_id_ori:
                if room_id in id_label:
                    room_labels.append("R" + str(i) + "_" + id_label[1] + "_" + room_id)
                    new_room_list.append(head)
                    i += 1
                    break

        for day_i in day_list[0:30]:

            df_day_i_raw = df_raw_i.loc[day_i, :]
            df_day_i = df_site_i.loc[day_i, :]

            df_day_i_raw = df_day_i_raw[new_room_list]
            df_day_i = df_day_i[new_room_list]

            xmax = df_day_i.shape[0]
            if xmax == 24:
                major_ticks = np.arange(0, 24)
            else:
                major_ticks = np.arange(0, xmax, 12)  # 5mins * 12 = 1hour

            fig = plt.figure()
            plt.grid()
            ax = fig.add_subplot(1, 1, 1)
            ax.set_title(day_i + " at " + site_id)

            df_day_i.plot(ax=ax)
            ax.legend(room_labels,loc=1)
            ax.set_xticks([])

            # raw = ax.twiny()
            # df_day_i_raw.plot(ax=raw, marker='.', )
            # raw.legend(room_labels)
            # raw.set_xticks([])

            df_temp_i = df_tempC.loc[day_i]
            temp = ax.twinx().twiny()
            df_temp_i.plot(ax=temp, color='c')
            temp.set_yticks([])
            temp.set_xticks([])
            temp.legend(["Outdoor"], loc=3)

            df_cloud_i = df_cloud.loc[day_i]
            cloud = ax.twinx()
            df_cloud_i.plot(ax=cloud, color='tab:olive')
            cloud.legend(["Cloud"],loc=2)
            cloud.set_xticks([])

            plt.xlim(0, xmax)
            plt.xticks(major_ticks, list(range(24)))

            fig.set_size_inches(18.5, 7.5)
            plt.savefig(day_i+"_"+site_id+'.png',dpi=400)
        plt.close('all')
    # plt.show()
