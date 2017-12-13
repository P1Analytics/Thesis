import seaborn as sns
from thermal.cbe_comfort import *
from util.data_preparation import *
sns.set()
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


def business_day_comfort(site_list, dict_df, dict_df_tempc):
    """
    summary the comfort in workdays for all site and each site for all rooms
    :param site_list: site_id list
    :param dict_df: inddor temperature dictionary with site_id as key
    :param dict_df_tempc: outdoor temperature dictionary with site_id as key
    :return:
    """
    # put all the comfort KPI into one dataframe
    df_all_comfort = pd.DataFrame()
    for site_id in site_list:
        # calculate each site comfort with outdoor/indoor temperature
        outdoor = dict_df_tempc[site_id]
        outdoor = outdoor.sort_index()

        outdoor = outdoor[site_id].values

        df_i = dict_df[site_id]
        df_i = df_i.sort_index()
        df_rooms, room_list, begin = ETL(df_i)

        index = df_rooms.index
        if -1 == begin:
            continue
        room_num = len(room_list)
        df_comfort_school_X = pd.DataFrame(index=index)
        for room_id in room_list:
            room = df_rooms[room_id].values
            comfort = []
            for ta, out in zip(room, outdoor):
                try:
                    comfort.append(comfAdaptiveComfortASH55(int(ta), int(ta), int(out))[5])
                except ValueError:
                    comfort.append(comfAdaptiveComfortASH55(0, 0, int(out))[5])
            df_comfort_school_X[str(room_id)] = comfort

        df_sum = df_comfort_school_X.mean(axis=1)
        df_all_comfort[site_id] = df_sum.values

        index_date = [pd.to_datetime(str(time)).strftime('%b-%d') for time in df_comfort_school_X.index.values]
        df_comfort_school_X.index = index_date
        sns.heatmap(df_comfort_school_X, cbar=False)
        plt.title(site_id)
        plt.savefig(site_id + '_comfort_heatmap.png', dpi=400)
        plt.close('all')

    # set all comfort dataframe with the same index from database and remove the duplicated ones from DST changing
    df_all_comfort['date'] = df_sum.index.values
    df_all_comfort = df_all_comfort.reset_index(drop=True)
    df_all_comfort = df_all_comfort.set_index('date')
    df_all_comfort = df_all_comfort[~df_all_comfort.index.duplicated(keep='first')]
    # df_all_comfort.to_pickle("test.pkl")
    # df_all_comfort = pd.read_pickle("test.pkl")

    # only check the comfort during working-day
    df_comfort_business_day = pd.DataFrame(columns=list(df_all_comfort), dtype=float)
    index_date = sorted(set([pd.to_datetime(str(time)).strftime('%Y-%m-%d') for time in df_all_comfort.index.values]))
    yticks_list = []
    for date in index_date:
        begin = df_all_comfort.index.get_loc(date + ' 08:00:00')
        if 0 <= pd.to_datetime(df_all_comfort.index[begin]).dayofweek < 5:  # Monday-Friday
            df_comfort_business_day = pd.concat([df_comfort_business_day, df_all_comfort.iloc[begin:begin + 9]], axis=0)
            yticks_list.append(pd.to_datetime(str(date)).strftime('%b-%d'))
    df_comfort_business_day.index = pd.to_datetime(df_comfort_business_day.index)
    df_comfort_business_day = df_comfort_business_day.groupby(pd.TimeGrouper('D')).mean().dropna(axis=0)

    return df_comfort_business_day,yticks_list


if __name__ == "__main__":
    # retrieve the data from database , sample 1hour.  time range
    site_list, dict_df, _, dict_df_tempc = retrieve_data(
        database='/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db',
        Year=2017, Months=list(range(1, 10)), feq="00:00")

    df_comfort_business_day,yticks_list = business_day_comfort(site_list, dict_df, dict_df_tempc)

    # plot the whole heatmap for comfort of all site
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    cbar_ax = fig.add_axes([.902, .3, .03, .4])
    sns.heatmap(df_comfort_business_day,
                ax=ax,
                xticklabels=list(df_comfort_business_day),
                yticklabels=10,
                cbar=True,
                cbar_ax=cbar_ax,
                )
    ax.set_yticklabels(yticks_list[::10])
    plt.savefig('all_site_comfort_heatmap.png', dpi=400)
    plt.close('all')
