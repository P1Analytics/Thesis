from Util.db_util import *
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
import seaborn as sns
sns.set()

def select_time_range_to_dataframe(cursor, site_id, resource_list):
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
    return df_final


def ETL_activity(df):
    """
    if any none zero data from any sensor, this device is active
    so we add all up from one device and more accuracy for activity statistic

    :param df: data from the same device, different sensors
    :return: the device on the timeline  active or not (1 or 0)
    """
    df = df[~df.index.duplicated(keep='first')]
    df[df > 0] = 1
    df_active = df.sum(axis=1)
    df_active[df_active > 0] = 1
    return df_active


def query_device(c, site_id):
    """
    query {device : resource list} from same site
    :param c: cursor of database
    :param site_id: site id
    :return: dictionary list
    """
    device_dict = defaultdict(list)
    query = "select site,resource,uri from details_sensor " \
            "where uri not like 'site%' and subsite=0 and site =" + str(site_id)
    resp = c.execute(query).fetchall()

    # find device
    for site, resource,uri in resp:
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


def reindex_df(day_index, df):
    df['timestamps'] = day_index
    df = df.reset_index(drop=True)
    df = df.set_index('timestamps')
    return df

if __name__ == "__main__":
    # retrieve data from database
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
            if len(site_list) <= 1:
                print("site list need more than 1 ", site_list)
            fig, axn = plt.subplots(len(site_list), 1,sharex=True)
            df_device_type = pd.DataFrame()
            for j, ax in enumerate(axn.flat):
                site_id = site_list[j]
                print(site_id, "............")

                # query database:  {device:[resources list],...}
                device_dict = query_device(c,site_id)

                # device : list of sensors , check activity
                df_site_devices = pd.DataFrame()
                for device in device_dict:
                    # print("Checking data from :",device, device_dict[device])
                    df_device = select_time_range_to_dataframe(c, site_id, device_dict[device])
                    df_device = df_device.sort_index()
                    df_static = ETL_activity(df_device)
                    df_site_devices[device] = df_static.values


                # reset index with plot Year - Month as label
                day_index = [pd.to_datetime(str(date)).strftime('%Y-%m') for date in df_static.index.values]
                df_site_devices = reindex_df(day_index,df_site_devices)
                # plot the heatmap for each site
                sns.heatmap(df_site_devices.T,
                            ax=ax,
                            xticklabels=31,
                            yticklabels=False,
                            cbar=False
                            )
                ax.set_xlabel('')
                ax.set_ylabel(site_id)# , rotation=90, labelpad=5, fontsize=9)
            axn[-1].set_xticklabels(sorted(set(day_index)))
            plt.show()
        except Error as e:
            print("SQL ERROR:", e)
