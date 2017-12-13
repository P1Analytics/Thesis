import seaborn as sns
import os
from util.data_preparation import *
sns.set()


def select_csv_to_dataframe(site_id, resource_list):
    " interface for 2years data in csv, you need to put siteid_2years.csv file into the same folder "
    filename = str(site_id) + "_2YEARS.csv"
    if not os.path.isfile(filename):
        print("Put siteid_2years.csv file into this folder, no csv data no plot")
        exit(1)
    df = pd.read_csv(filename, delimiter=";", index_col='timestamps', parse_dates=True)
    new_heads = []
    for id in resource_list:
        for item in list(df):
            if str(id) in item:
                new_heads.append(item)
                break
    df_device = df[new_heads]
    return df_device


def select_all_to_dataframe(cursor, site_id, resource_list):
    """
    select device resource list for the whole avaliable database save into one dataframe for later process
    :param cursor:
    :param site_id: for site table
    :param resource_list: for one device , all the sensor data resource list
    :return: dataframe for one device
    """
    df_final = pd.DataFrame()
    feq = '21:00'
    for id in resource_list:
        query = "select time, value from site_" + site_id + " where id = " + str(id) + \
                " and time LIKE  '%" + feq + "%';"
        resp = cursor.execute(query)
        df = pd.DataFrame(resp.fetchall(), columns=["timestamps", id], dtype=float)
        df = df.reset_index(drop=True)
        df = df.set_index('timestamps')
        df = df[~df.index.duplicated(keep='first')]
        df_final = pd.concat([df_final, df], axis=1)
    df_final = df_final.sort_index()
    return df_final


def query_device(cursor, site_id):
    """
    query {device : resource list} from same site
    :param cursor: cursor of database
    :param site_id: site id
    :return: dictionary list
    """
    device_dict = defaultdict(list)
    query = "select site,resource,uri from details_sensor " \
            "where uri not like 'site%' and subsite=0 and site =" + str(site_id)
    resp = cursor.execute(query).fetchall()

    # find device
    for site, resource, uri in resp:
        if '0x' in uri:
            for i in uri.split('/'):
                if '0x' in i:
                    index = (uri.split('/').index(i))
                    device = uri.split("/")[index]
                    break
            device_dict[device].append(resource)
        elif 'soderhamn' in uri or 'gaia-ea' in uri:
            device = uri.split("/")[1]
            device_dict[device].append(resource)
        elif 'sapienza' in uri or 'gaia-prato' in uri:
            device = uri.split("/")[2]
            device_dict[device].append(resource)
        else:
            device = uri.split("/")[0]
            device_dict[device].append(resource)
    return device_dict


def reindex_df(new_index, df):
    # reset index of the dataframe
    df['timestamps'] = new_index
    df = df.reset_index(drop=True)
    df = df.set_index('timestamps')
    return df


def type_activity(c, site_list):
    df_device_env = pd.DataFrame()
    df_device_syn = pd.DataFrame()
    df_device_lib = pd.DataFrame()
    df_device_pow = pd.DataFrame()

    size_max = 0
    for site_id in site_list:
        print(site_id, ".......")

        # query database:  {device:[resources list],...}
        device_dict = query_device(c, site_id)
        # device : list of sensors , check activity
        for device in device_dict:

            # interface from database
            df_device = select_all_to_dataframe(c, site_id, device_dict[device])

            # interface from csv file
            # df_device = select_csv_to_dataframe(site_id, device_dict[device])

            df_static = device_activity(df_device)

            day_index = [pd.to_datetime(str(date)).strftime('%Y-%m') for date in df_static.index.values]
            if len(day_index) > size_max:  # TODO
                index_update = day_index
            try:
                # category different types into different dataframe
                if 'libelium' in device:
                    df_device_lib[str(site_id) + device] = df_static.values
                elif 'synfield' in device:
                    df_device_syn[str(site_id) + device] = df_static.values
                else:
                    query = "select property from details_sensor where resource = " + str(
                        device_dict[device][0])
                    if 'Power' in c.execute(query).fetchall()[0][0] or 'Current' in \
                            c.execute(query).fetchall()[0][
                                0]:
                        df_device_pow[str(site_id) + device] = df_static.values
                    else:
                        df_device_env[str(site_id) + device] = df_static.values
            except ValueError:
                print(df_static.shape)
                continue

    # print("outrage syn", outrages(df_device_syn))
    # print("outrage lib", outrages(df_device_lib))
    # print("outrage pow", outrages(df_device_pow))
    # print("outrage env", outrages(df_device_env))

    # reset index with plot Year - Month as label
    df_device_syn = reindex_df(index_update, df_device_syn)
    df_device_lib = reindex_df(index_update, df_device_lib)
    df_device_pow = reindex_df(index_update, df_device_pow)
    df_device_env = reindex_df(index_update, df_device_env)


    return df_device_syn, df_device_lib, df_device_pow, df_device_env


def single_site_activity(c,site_id):
    """
    put different devices activity into one site dataframe
    :param c: cursor
    :param site_id:
    :return: df_site_devices for all devices on this site ; day_index for heatmap plot
    """

    df_site_devices = pd.DataFrame()
    # query database:  {device:[resources list],...}
    device_dict = query_device(c, site_id)
    # device : list of sensors , check activity
    for device in device_dict:
        # interface from csv file
        # df_device = select_csv_to_dataframe(site_id, device_dict[device])

        # interface from database
        df_device = select_all_to_dataframe(c, site_id, device_dict[device])
        df_static = device_activity(df_device)
        df_site_devices[device] = df_static.values

    # print(site_id,outrages(df_site_devices))
    # reset index for plot "Year - Month" as xtick label
    day_index = [pd.to_datetime(str(date)).strftime('%Y-%m') for date in df_static.index.values]
    df_site_devices = reindex_df(day_index, df_site_devices)
    return df_site_devices,day_index

def outrages(df):
    # print(df)
    temp = df
    for head in list(temp):
        df_col = temp[head]
        if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
            # print(df_col)
            temp[head] = temp[head].replace(0, np.nan)
            continue

    if temp.shape[1] > 0:
        active_time = []
        dfT = temp.T

        for head in list(dfT):
            df_col = dfT[head]
            try:
                if (df_col[df_col == 0]).size + df_col.isnull().sum() == df_col.size:
                    dfT[head] = dfT[head].replace(0, np.nan)
                    # print(head)
                else:
                    active_time.append(head)
            except ValueError:
                continue
        temp = dfT.T


    if active_time:
        begin = temp.index.get_loc(active_time[0])
        df_power = temp.iloc[begin:]
        df_power[df_power == 0] = np.nan
        return df_power.isnull().sum().values.sum()/(temp.shape[0] * temp.shape[1])*100
    else:
        return active_time


if __name__ == "__main__":
    database = '/Users/nanazhu/Documents/Sapienza/Thesis/src_python/test.db'

    with create_connection(database) as conn:
        try:
            c = conn.cursor()
            site_list = [str(id[0]) for id in c.execute("select site from details_sensor group by site;")]
            # site_list = [ #'144243', '155865',
            #              "144024","28843", "144243", "28850",
            # #              # '157185',#"155849", #"144242",  "19640", "27827", "155849",
            # #              #     "155851", "155076", "155865", "155077",
            # #              #     "155877", "157185", "159705"
            #              ]
            # if len(site_list) <= 1:
            #     print("site list need more than 1 ", site_list)

            print("active by type")
            df_device_syn, df_device_lib, df_device_pow, df_device_env = type_activity(c, site_list)
            df_list = [df_device_env, df_device_syn, df_device_lib, df_device_pow]
            day_index = df_device_syn.index.values

            fig, axn = plt.subplots(4, 1, sharex=True)
            ylabel_list = ["Env", "Syn", "Lib", "Pow"]
            for j, ax in enumerate(axn.flat):
                # plot the heatmap for each type
                try:
                    sns.heatmap(df_list[j].T,
                                ax=ax,
                                xticklabels=31,
                                yticklabels=False,
                                cbar=False
                                )
                    ax.set_xlabel('')
                    ax.set_ylabel(ylabel_list[j], rotation=0, labelpad=20)
                    axn[-1].set_xticklabels(sorted(set(day_index)))
                except TypeError:
                    continue


            # ########################################################### #
            print("active by site")
            fig, axn = plt.subplots(len(site_list), 1, sharex=True)
            df_device_type = pd.DataFrame()
            for j, ax in enumerate(axn.flat):
                site_id = site_list[j]

                df_site_devices,day_index = single_site_activity(c,site_id)
                # plot the heatmap for each site
                sns.heatmap(df_site_devices.T,
                            ax=ax,
                            xticklabels=31,
                            yticklabels=False,
                            cbar=False
                            )
                ax.set_xlabel('')
                ax.set_ylabel(site_id, rotation=0, labelpad=20)
                axn[-1].set_xticklabels(sorted(set(day_index)))
            plt.show()
        except Error as e:
            print("SQL ERROR:", e)
