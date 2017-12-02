from pylab import *
from db_util import *
from Data_Preparation import *

pd.options.mode.chained_assignment = None
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def retrieve_data(database, Months):
    """
    retrieve data from database during date rage A-B
    :param database: sqlite database file
    :param Months: query months range
    :return: full site list ,
             dictionary of : sensor data, API data for cloud coverage , API data for Temperature
    """
    dict_df = {}
    dict_df_cloud = {}
    dict_df_tempc = {}
    date_A = str(Year) + "-" + str('{:02d}'.format(Months[0])) + "-01"
    if 12 == Months[-1]:
        date_B = str(Year + 1) + "-01-01"
    else:
        date_B = str(Year) + "-" + str('{:02d}'.format(Months[-1] + 1)) + "-01"

    with create_connection(database) as conn:
        print("Database Connecting....")
        try:
            orientation = {}
            c = conn.cursor()
            site_list = c.execute("select site from details_sensor group by site;")
            site_list = [str(id[0]) for id in site_list]
            for site_id in site_list:
                temperature_resource_list = query_site_room_orientaion(c, site_id)
                orientation[site_id] = temperature_resource_list
                temperature_resource_list = [i[0] for i in temperature_resource_list]

                dict_df[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list, date_A, date_B)

                query = " select time,value from API_CloudCoverage " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_cloud[site_id] = select_single_sensor_to_pandas(c, query, site_id)

                query = " select time,value from API_Temperature " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_tempc[site_id] = select_single_sensor_to_pandas(c, query, site_id)
        except Error as e:
            print("SQL ERROR:", e)
    return site_list, dict_df, dict_df_cloud, dict_df_tempc, orientation


def plot_temp_indoor_outdoor(key_day, ax, weather, df_ETL, df_tempc, room_legends, xticks):
    df = df_ETL.loc[key_day, :]
    if 0 != df.shape[1]:
        df.plot(ax=ax)
        ax.grid(color='grey', linestyle='--', linewidth=0.1)
        ax.legend(room_legends, loc=2)
        ax.set_title(weather + " on " + df.index.values[0])
        ax.set_xticks(xticks)
        ax.set_xticklabels(list(range(24)))
        outdoor = ax.twinx().twiny()
        df_tempc.loc[key_day].plot(ax=outdoor, color='c')
        outdoor.set_yticks([])
        outdoor.set_xticks([])
        outdoor.legend(["Outdoor"], loc=3)


if __name__ == "__main__":

    database = '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    Year = 2017
    Months = list(range(1, 13))

    site_list, dict_df, dict_df_cloud, dict_df_tempc, orientation = retrieve_data(database, Months)
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
            df_raw.plot()
            df_ETL, rooms_active, begin = ETL(df_raw)
            df_ETL.plot()
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
