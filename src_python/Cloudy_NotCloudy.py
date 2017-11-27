import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
from sklearn.linear_model import LinearRegression
import warnings, json
from pylab import *
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")
pd.options.mode.chained_assignment = None  # default='warn'

from ExploreTemp import *

if __name__ == "__main__":
    orientation = Orientation()

    Oct_list = [
        "144242_Temperatur2months.csv",
        "144024_Temperatur2months.csv",
    ]
    Feb_list = [
        "144242_TemperaturFeb.csv",
        "144024_TemperaturFeb.csv",
    ]
    for (site_feb,site_oct) in zip(Feb_list,Oct_list):
        site_id = site_oct.split("_")[0]
        room_id_ori = orientation[site_id]
        if not room_id_ori:
            print("we dont know the real orientation for this site", site_id)
            continue

        df_raw_feb = pd.read_csv(site_feb, delimiter=";", index_col='timestamps', parse_dates=True)
        df_raw_feb = df_raw_feb[~df_raw_feb.index.duplicated(keep='first')]
        df_raw_feb = df_raw_feb['2017-02-01':'2017-02-28']
        df_site_feb, room_feb,begin_feb = ETL_df(df_raw_feb)
        df_site_feb.index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_site_feb.index.values]

        df_raw_oct = pd.read_csv(site_oct, delimiter=";", index_col='timestamps', parse_dates=True)
        df_raw_oct = df_raw_oct[~df_raw_oct.index.duplicated(keep='first')]
        df_raw_oct = df_raw_oct.loc['2017-10-01':'2017-10-31']
        df_site_oct, room_list,begin_oct = ETL_df(df_raw_oct)
        df_site_oct.index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_site_oct.index.values]

        print(begin_feb,begin_oct)

        begin_feb_i = df_site_feb.index[begin_feb]
        begin_oct_i = df_site_oct.index[begin_oct]

        df_API_Cloud = pd.read_csv("API_cloudcover_Feb_Oct.csv", delimiter=";", index_col='timestamps', parse_dates=True)
        df_cloud = df_API_Cloud.loc[:,site_id]
        df_cloud.index=[pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_cloud.index.values]
        df_cloud.index.name = 'date'

        df_cloud_Feb = df_cloud.loc[begin_feb_i:'2017-02-28']
        mean_day = df_cloud_Feb.groupby('date').mean()
        cloudy_feb = mean_day.idxmax()
        sunny_feb = mean_day.idxmin()

        df_cloud_Oct = df_cloud.loc[begin_oct_i:'2017-10-31']
        mean_day = df_cloud_Oct.groupby('date').mean()
        cloudy_oct = mean_day.idxmax()
        sunny_oct = mean_day.idxmin()

        # df_API_Temp = pd.read_csv("API_tempC_Feb_Oct.csv", delimiter=";", index_col='timestamps', parse_dates=True)
        # df_tempC = df_API_Temp.loc[:,site_id]
        # df_tempC.index=[pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_tempC.index.values]

        date_list = [[cloudy_feb,sunny_feb],[cloudy_oct,sunny_oct]]

        fig, axn = plt.subplots(nrows=2, ncols=2)#,sharex=True)
        xmax = 288
        plt.xlim(0, xmax)
        [cloud,sunny] = date_list[0]
        df = df_site_feb.loc[cloud,:]
        print(df.shape,df)
        if df.shape[1] !=0 :
            df.plot(ax=axn[0,0],legend=False)
            axn[0,0].legend(room_labels, loc=1)
            axn[0,0].set_title(cloud + " at " + site_id+"cloudy")
            axn[0,0].set_xticks(np.arange(0, xmax, 12))
            axn[0,0].set_xticklabels(list(range(24)))

        df = df_site_feb.loc[sunny,:]
        if df.shape[1] !=0 :
            df.plot(ax=axn[0,1],legend=False)
            axn[0,1].legend(room_labels, loc=1)
            axn[0,1].set_title(sunny + " at " + site_id+"sunny")
            axn[0,1].set_xticks(np.arange(0, xmax, 12))
            axn[0,1].set_xticklabels(list(range(24)))

        room_ids = [label.split("_")[-3] for label in list(df_site_oct)]
        room_labels = []
        i = 1
        for room_id in room_ids:
            for id_label in room_id_ori:
                if room_id in id_label:
                    room_labels.append("R" + str(i) + "_" + id_label[1] + "_" + room_id)
                    i += 1


        [cloud,sunny] = date_list[1]
        df = df_site_oct.loc[cloud,:]
        if df.shape[1] !=0 :
            df.plot(ax=axn[1,0],legend=False)
            axn[1,0].legend(room_labels, loc=1)
            axn[1,0].set_title(cloud + " at " + site_id+"cloudy")
            axn[1,0].set_xticks(np.arange(0, xmax, 12))
            axn[1,0].set_xticklabels(list(range(24)))
        df = df_site_oct.loc[sunny,:]
        if df.shape[1] !=0 :
            df.plot(ax=axn[1,1],legend=False)
            axn[1,1].legend(room_labels, loc=1)
            axn[1,1].set_title(sunny + " at " + site_id+"sunny")
            axn[1,1].set_xticks(np.arange(0, xmax, 12))
            axn[1,1].set_xticklabels(list(range(24)))

        # cloud = axn[row,col].twinx()
        # df_cloud.loc[date_list[i]].plot(ax=cloud, color='b', marker='o')
        # cloud.legend(["Cloud"], loc=4)

        # temp = axn[row,col].twinx()
        # df_tempC.loc[date_list[i]].plot(ax=temp, color='y', marker='o')
        # temp.set_yticks([])
        # temp.legend(["Outdoor"], loc=2)

    plt.show()