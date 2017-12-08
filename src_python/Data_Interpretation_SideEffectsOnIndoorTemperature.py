from Util.Data_Preparation import *
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def retrieve_period_data(database, Year, Months, feq=None):
    """
    retrieve data from database during date rage A-B
    :param feq: 5 mins sample use None; 1hour sample use "00:00" ; 1 day sample use "21:00"
    :param database: sqlite database file
    :param Year :
    :param Months: query months range
    :return: full site list ,
             dictionary of : sensor data, API data for cloud coverage , API data for Temperature
    """
    dict_df_temp = {}
    dict_df_motion = {}
    dict_df_cloud = {}
    dict_df_tempC = {}
    date_A = str(Year) + "-" + str('{:02d}'.format(Months[0])) + "-01"
    if 12 == Months[-1]:
        date_B = str(Year + 1) + "-01-01"
    else:
        date_B = str(Year) + "-" + str('{:02d}'.format(Months[-1] + 1)) + "-01"
    with create_connection(database) as conn:
        print("Database Connecting for sensor data....")
        try:
            c = conn.cursor()
            site_list = c.execute(
                "select site from details_sensor group by site;")  # TODO or fill the table :details_site
            site_list = [str(id[0]) for id in site_list]
            # site_list=[
            #     #"144024","28843", "144243", "28850",
            #     '144243','155865',#'157185',#"155849", #"144242",  "19640", "27827", "155849",
            # #     "155851", "155076", "155865", "155077",
            # #     "155877", "157185", "159705"
            # ]
            for site_id in site_list:
                # retrieve the resource list for each type of sensor
                temperature_resource_list = [i[0] for i in query_site_room_orientaion(c, site_id)]
                motion_resource_list = []
                for i in temperature_resource_list:
                    subsite_id = c.execute("select subsite from details_sensor where resource= " +
                                           str(i) + " and subsite !=0 ;").fetchall()[0][0]
                    resource = c.execute("select resource from details_sensor where subsite = " +
                                         str(subsite_id) + " and property = 'Motion';").fetchall()
                    if resource:
                        resource = resource[0][0]
                        motion_resource_list.append(resource)

                # retrieve the sensor data
                if feq:
                    dict_df_temp[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list,
                                                                           date_A, date_B, feq)
                    dict_df_motion[site_id] = select_time_range_to_dataframe(c, site_id, motion_resource_list, date_A,
                                                                             date_B, feq)
                else:
                    dict_df_temp[site_id] = select_time_range_to_dataframe(c, site_id, temperature_resource_list,
                                                                           date_A, date_B)
                    dict_df_motion[site_id] = select_time_range_to_dataframe(c, site_id, motion_resource_list, date_A,
                                                                             date_B)
                # retrieve the API data
                query = " select time,value from API_CloudCoverage " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_cloud[site_id] = select_single_sensor_to_pandas(c, query, site_id)

                query = " select time,value from API_Temperature " \
                        "where id=" + site_id + " and (time> '" + date_A + "' and time < '" + date_B + "');"
                dict_df_tempC[site_id] = select_single_sensor_to_pandas(c, query, site_id)
        except Error as e:
            print("SQL ERROR:", e)
    return site_list, dict_df_temp, dict_df_motion, dict_df_cloud, dict_df_tempC


if __name__ == "__main__":

    # collect the data from database
    database = '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'
    site_list, dict_df_temp, dict_df_motion, dict_df_cloud, dict_df_tempC = \
        retrieve_period_data(database, Year=2017,Months=list(range(9, 10)),feq=None)
    orientation_dict = retrieve_orientation(database)

    # process each school
    for site_id in site_list:
        # if we don' know the orientation  , jump to next school
        rooms_orientation = orientation_dict[int(site_id)]
        if rooms_orientation is None:
            print(site_id, " orientation empty")
            continue

        # check the indoor temperature if it is empty , jump to next school
        df_original = dict_df_temp[site_id]
        df_temp, room_list, begin = ETL(df_original)
        if -1 == begin:
            print(site_id, " data empty")
            continue
        print(site_id, ".....")

        df_motion = dict_df_motion[site_id]
        df_cloud = dict_df_cloud[site_id]
        df_cloud = df_cloud.reindex(df_original.index, method='pad', limit=11) # expand the hour sample to 5mins sample

        df_tempC = dict_df_tempC[site_id]
        df_tempC = df_tempC.reindex(df_original.index, method='pad', limit=11)

        day_index = [pd.to_datetime(str(date)).strftime('%Y-%m-%d') for date in df_original.index.values]

        # replacing index for date for plot
        day_list = sorted(set(day_index))
        df_motion.index = day_index
        df_temp.index = day_index
        df_cloud.index = day_index
        df_tempC.index = day_index

        # prepare for the legend of each room
        room_legends = []
        i = 1
        for room_id in room_list:
            for id_label in rooms_orientation:
                if room_id == id_label[0]:
                    room_legends.append("R" + str(i) + "_" + id_label[1] + "_" + str(room_id))
                    i += 1
                    break


        # plot out the data per day
        for day_i in day_list:
            # only print weekend
            if pd.to_datetime(day_i).dayofweek < 5 :
                continue

            df_day_i = df_temp.loc[day_i, :]
            df_motion_i = df_motion.loc[day_i]
            df_cloud_i = df_cloud.loc[day_i, :]
            df_tempC_i = df_tempC.loc[day_i, :]


            f, axn = plt.subplots(4, 1, squeeze=False) #sharex=True,
            xmax = df_day_i.shape[0]
            if xmax == 24:
                major_ticks = np.arange(0, 24)
            else:
                major_ticks = np.arange(0, xmax, 12)  # 5mins * 12 = 1hour

            axn[0, 0].set_title(day_i + " at " + site_id)

            df_day_i.plot(ax=axn[0, 0])
            axn[0, 0].legend(room_legends, loc=2)
            axn[0, 0].set_xticks([])
            axn[0, 0].set_ylabel('Temp')
            axn[0, 0].set_xlim(0 + 8 * 12, xmax - 2 * 12)
            #
            df_day_i.diff().abs().plot(ax=axn[1, 0])
            axn[1, 0].legend(room_legends, loc=2)
            axn[1,0].set_ylabel('Diff')

            df_motion_i.plot(ax=axn[2, 0],legend=False)
            axn[2, 0].set_ylabel('Motion')

            df_cloud_i.plot(ax=axn[3, 0],legend=False)
            axn[3, 0].set_ylabel('Cloud')

            plt.xlim(0, xmax)

            # must set at the end of all the plots
            plt.setp(axn, xticks=major_ticks, xticklabels=list(range(24)))

            break
        break
    #         f.set_size_inches(18.5, 11.5)
    #         plt.savefig(day_i + "_" + site_id + '.png', dpi=400)
    #     plt.close('all')
    plt.show()
