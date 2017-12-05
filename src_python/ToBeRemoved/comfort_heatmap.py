import seaborn as sns
from db_util import *

from Util.Data_Preparation import *
from Util.comfort_models import *

sns.set()
warnings.filterwarnings(action="ignore", module="scipy", message="^internal gelsd")


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

    site_name = [x.split("_")[0] for x in school_list]
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
        site_id = school.split("_")[0]
        df = pd.read_csv(school, delimiter=";", index_col='timestamps', parse_dates=True)

        df_rooms, _, _ = ETL(df)
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
            xtick_comfort_Y = school.split("_")[0]
        if school == "155851_Temperature.csv": #"19640_Temperature.csv":  # ""155849_Temperature.csv":
            df_comfort_N = df_comfort_school_X
            xtick_comfort_N = school.split("_")[0]

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

    fig, axn = plt.subplots(1,
                            3,
                            gridspec_kw={'width_ratios': [1, 1, 3]},
                            sharey=True
                            )
    cbar_ax = fig.add_axes([.902, .3, .03, .4])  # [left, bottom, width, height]

    # xticks = [range(1,len(list(df_comfort_Y_bday))+1),
    #           range(1,len(list(df_comfort_N_bday))+1),
    #           site_name #site_newname
    #           ]
    # xticks = [
    #             ["R1", "R2", "R3", "R4"],
    #             ["R1", "R2", "R3", "R4", "R5"],
    #             site_newname]

    xticks = [
                ["W", "SW", "SW", "NW"],
                ["NW", "SE", "SE", "SE", "SW"],
                site_name]
    # xlabels = ["SITE C", "SITE I", "Sites"]
    xlabels = ["site"+xtick_comfort_Y,"site"+xtick_comfort_N,"SITES"]
    yticks = [pd.to_datetime(str(date)).strftime('%b-%d') for date in df_comfort_bday.index.values]

    i = 0
    for j, ax in enumerate(axn.flat):
        df = df_list[i]
        sns.heatmap(df, #[4:],
                    ax=ax,
                    xticklabels=xticks[i],
                    yticklabels=5,
                    cbar=j == 0,
                    cbar_ax=None if j else cbar_ax,
                    cbar_kws={'label': 'uncomfortable(0)------->comfortable(1)'}
                    )
        pos1 = ax.get_position() # get the original position
        pos2 = [pos1.x0 - 0.02, pos1.y0,  pos1.width, pos1.height]
        ax.set_position(pos2)
        ax.set_xlabel(xlabels[i])#,fontsize=8)
        i += 1
    ax.set_yticklabels(busday_list[::5],fontsize=5) #(yticks[0::5]) # ,rotation=90)

    # plot.tick_params(axis='both', which='major', labelsize=10)
    # plot.tick_params(axis='both', which='minor', labelsize=8)

    # plt.savefig('comfort.png',dpi=4000)
    # plt.close('all')
    plt.show()
