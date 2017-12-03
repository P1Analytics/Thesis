from Data_Preparation import *

pd.options.mode.chained_assignment = None
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def plot_temp_indoor_outdoor(key_day, ax, weather, df_ETL, df_tempc, room_legends, xticks):
    df = df_ETL.loc[key_day, :]
    if 0 != df.shape[1]:
        df.plot(ax=ax)
        ax.grid(color='grey', linestyle='--', linewidth=0.1)
        ax.legend(room_legends, loc=2)
        ax.set_title(weather + " on " + df.index.values[0])
        ax.set_xticks(xticks)
        ax.set_xticklabels(list(range(24)))
        # outdoor = ax.twinx().twiny() # wrong but looks better on trend
        # outdoor = ax.twiny() # good but not good to show trend
        # df_tempc.loc[key_day].plot(ax=outdoor, color='c')
        # outdoor.set_xticks([])
        # outdoor.set_yticks([])
        # outdoor.legend(["Outdoor"], loc=3)

if __name__ == "__main__":

    database = '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    Year = 2017
    Months = list(range(8, 9))

    site_list, dict_df, dict_df_cloud, dict_df_tempc, orientation = retrieve_data(
        database='/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db', Year=2017, Months=list(range(1, 13)))
    for site_id in site_list:
        room_id_ori = orientation[site_id]
        if not room_id_ori:
            print("we dont know the true orientation for this site ", site_id)
            continue
        print("****************  ", site_id, room_id_ori, "****************  ")

        df_site_i = dict_df[site_id].sort_index()
        df_cloud_i = dict_df_cloud[site_id].sort_index()
        df_tempc_i = dict_df_tempc[site_id].sort_index()

        index = 0
        while index < len(Months):
            date_begin = str(Year) + "-" + str('{:02d}'.format(Months[index])) + "-01"
            try:
                date_end = str(Year) + "-" + str('{:02d}'.format(Months[index + 1])) + "-01"
            except IndexError:
                if 12 == Months[index]:
                    date_end = str(Year + 1) + "-01-01"
                else:
                    date_end = str(Year) + "-" + str('{:02d}'.format(Months[index] + 1)) + "-01"
            index += 1

            df_raw = df_site_i.loc[date_begin:date_end]
            df_ETL, rooms_active, begin = ETL(df_raw)
            if -1 == begin:
                print("No data in this time range [", date_begin, date_end, ")")
                continue
            print("Time range ", date_begin, date_end)

            df_ETL.index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_ETL.index.values]
            begin_i = df_ETL.index[begin]
            print("Power-on start from ", begin_i)

            df_cloud = df_cloud_i.loc[begin_i:date_end]
            df_cloud.index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_cloud.index.values]
            df_cloud.index.name = 'date'
            mean_day = df_cloud.groupby('date').mean()
            cloudy = mean_day.idxmax()
            sunny = mean_day.idxmin()

            df_tempc = df_tempc_i.loc[begin_i:date_end]
            df_tempc.index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_tempc.index.values]

            fig, axn = plt.subplots(1, 2, sharey=True)
            plt.suptitle(site_id)
            xmax = 288  # not magic number = 5(mins/ sampled data)*12(times/ hour)*24(hours)
            plt.xlim(0, xmax)
            legends = []
            i = 1
            for id_label in room_id_ori:
                if id_label[0] in rooms_active:
                    legends.append("R" + str(i) + "_" + id_label[1] + "_" + str(id_label[0]))
                    i += 1
            xticks = np.arange(0, xmax, 12)

            plot_temp_indoor_outdoor(cloudy, axn[0], "cloudy", df_ETL, df_tempc, legends, xticks)
            plot_temp_indoor_outdoor(sunny, axn[1], "sunny", df_ETL, df_tempc, legends, xticks)

            plt.tight_layout()
    plt.show()
