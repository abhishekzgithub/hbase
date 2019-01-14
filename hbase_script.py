import happybase
import pandas as pd
import os,time
import datetime

timenow=datetime.datetime.now().strftime("%y-%m-%d-%H-%M")
hbaseip = input("Please provide the IP ADDRESS of the hbase==-->")
namespace = input("NAMESPACE of the hbase==-->")
encoding = 'utf-8'
filepath=os.getcwd()
filename=namespace+"_data.xlsx"



def convert_scan_data_to_list(h_data, is_col_family_included=False):
    """
    Converts hbase data to list of dictionaries

    :param h_data: Object returned from table.scan function
    :param is_col_family_included: Flag for adding column family in returned data
    :return: List
    """
    temp_list = []
    try:
        for row_key, vals in h_data:
            value_dict = {}
            key_name = (row_key.decode(encoding), vals)[0]
            key_vals = (row_key.decode(encoding), vals)[1]
            value_dict['row_key'] = key_name
            for keys, items in key_vals.items():
                if is_col_family_included:
                    names = keys
                else:
                    names = keys.decode(encoding).split(':')[1]
                value_dict[names] = items.decode(encoding)
            temp_list.append(value_dict)
    except Exception as e:
        print(e)
    return temp_list


def convert_scan_data_to_df(h_data, is_col_family_included=False):
    """
        Converts hbase data to DataFrame

        :param h_data: Object returned from table.scan function
        :param is_col_family_included: Flag for adding column family in returned data
        :return: DataFrame

        """
    temp_list = convert_scan_data_to_list(h_data, is_col_family_included)
    df = pd.DataFrame(temp_list)
    return df

def get_table_details(table_name=None, filter1=None, col=None, del_data=False, df_conv=False):
    conn = happybase.Connection(hbaseip, table_prefix=namespace, table_prefix_separator=":",
                                autoconnect=False)
    conn.open()
    table_data = conn.table(table_name)
    data = table_data.scan(columns=col, filter=filter1)
    if df_conv == True:
        df = convert_scan_data_to_df(data,is_col_family_included=True)
    if conn:
        conn.close()
    return df

def get_all_table_list(local_list=False):
    conn = happybase.Connection(hbaseip, table_prefix=namespace, table_prefix_separator=":",
                                autoconnect=False)
    conn.open()
    if local_list:
        table_list_all=tab_list_ccpa
    else:
        table_list_all = [i.decode('utf-8') for i in conn.tables()]
    if conn:
        conn.close()
    return table_list_all

def get_all_data(backup=False):
    try:
        print("saving data started")
        dfc = {}
        if backup:
            writer = pd.ExcelWriter(filepath+r"\\"+"bk_"+timenow+"_"+filename)
        else:
            writer = pd.ExcelWriter(filepath+r"\\"+filename,engine='openpyxl')
        table_list_all=get_all_table_list()
        for i in range(len(table_list_all)):
            dfc[i] = get_table_details(df_conv=True,
                                       table_name=table_list_all[i])
            dfc[i].to_excel(writer, sheet_name=table_list_all[i], index=False,encoding=encoding)
        writer.save()
    except Exception as e:
        print("exception in get all data",e)
    finally:
        print("excel file {0} is saved in below location\n {1} ".format(filename,filepath))


class HbaseOperation(object):
    def __init__(self,tablename=None):
        self.tab=tablename
        self.conn = happybase.Connection(hbaseip, table_prefix=namespace, table_prefix_separator=":",
                                    autoconnect=False)
        self.conn.open()
        get_all_data(backup=True)
    def _create_hbasetable(self):
        """
        # tablename='mytable1'
        # colfam={'cf1': dict()}
        # dict_name_family={tablename:colfam}
         #(create_hbasetable(dict_name_family))
        :param dict_name_family:
        :return:
        """
        print("creation of tables started")
        if self.tab == None:
            for i in range(len(tab_cf)):
                for table_name,colfam in tab_cf[i].items():
                    print("Table {0} is created".format(table_name))
                    try:
                        self.conn.create_table(name=table_name,families=colfam)
                    except Exception as e:
                        continue
        else:
            fam=[val for i in range(len(tab_cf)) for key, val in tab_cf[i].items() if key == self.tab][0]
            print("Table {0} is created".format(self.tab))
            self.conn.create_table(name=self.tab, families=fam)
        print("creation of tables ended")
        #self.conn.close()

    def _delete_hbasetable(self,local_list=False):
        print("deletion of tables started")
        tablename_list=get_all_table_list(local_list=local_list)
        #if True:#eval(input("Press '1234567' to delete all data\n"))==1234567:
        if self.tab == None:
            for i in tablename_list:
                print(("table {0} deleted").format(i))
                try:
                    self.conn.delete_table(name=i,disable=True)
                except Exception as e:
                    continue
        else:
            self.conn.delete_table(name=self.tab, disable=True)
            print(("table {0} deleted").format(self.tab))
        #self.conn.close()
        flag = False
        print("deletion of tables ended")


    def _delete_keydata_hbase(self):
        """safe delete"""
        if self.tab!=None:
            print("updating the {} table".format(self.tab))
            table_data=self.conn.table(self.tab)
            data=table_data.scan()
            for key, val in data:
                if key:
                    table_data.delete(key)
        else:
            raise SystemError("a table input is required for this operation")
    def insert_data(self):
        print("insertion of data in tables started")
        if not os.path.exists(filepath+r'\\'+filename):
            raise ValueError(("file  {0} doesnt exist in the path {1}").format(filename,filepath))
        else:
            file=filepath+r'\\'+filename
            if self.tab == None:
                sheet_list = pd.ExcelFile(file).sheet_names
                for i in range(len(sheet_list)):
                    print("Table {0} is being inserted ".format(sheet_list[i]))
                    table = self.conn.table(sheet_list[i])
                    df = pd.read_excel(file, sheet_name=sheet_list[i])
                    df_rowkey = df['row_key'].astype('str')
                    df_data = df.drop(['row_key'], axis=1).astype('str')
                    with table.batch(transaction=True) as b:
                        for x in range(df.shape[0]):
                            rk = df_rowkey.iloc[x]
                            data = df_data.iloc[x, :].to_dict()
                            b.put(rk, data)
            else:
                sheet_list=self.tab
                print("Table {0} is being inserted ".format(sheet_list))
                table = self.conn.table(sheet_list)
                df = pd.read_excel(file, sheet_name=sheet_list)
                df_rowkey = df['row_key'].astype('str')
                df_data = df.drop(['row_key'], axis=1).astype('str')
                with table.batch(transaction=True) as b:
                    for x in range(df.shape[0]):
                        rk = df_rowkey.iloc[x]
                        data = df_data.iloc[x, :].to_dict()
                        b.put(rk, data)
            #self.conn.close()
        time.sleep(2)
        get_all_data(backup=False)
        print("insertion of data in tables ended")

    def get_cf_table(self):
        sheet_list = pd.ExcelFile(filename).sheet_names
        dict_name_family = dict()
        for i in range(len(sheet_list)):
            table = self.conn.table(sheet_list[i])
            fam=table.families()
            cf=[k for k in fam.keys()][0].decode('utf-8')
            dict_name_family[i]={
                sheet_list[i]:{
                cf:dict()
                }
            }
        self.conn.close()
        #print(list(dict_name_family.values()))


if __name__ == "__main__":
    while True:
        var=int(input("Press 1 to create backup of data\n"
                      "Press 2 to download data which will be later used for upload after edit\n"
                  "Press 3 to upload the appended data. A default backup of previous data will be saved.\n"
                      "Press 4 to completely update the data from the excel\n"
                      "Press 5 to update specific table/sheet\n"
                      "Press 6 for safe update(time taking)\n"
                  "Press 10 to exit this loop\n--->"))
        if var == 10:
            raise SystemExit("You pressed 10 to exit")
        elif var == 1:
            get_all_data(backup=True)
        elif var == 2:
            get_all_data(backup=False)
        elif var == 3:
            hbo = HbaseOperation()
            hbo.insert_data()
            time.sleep(4)
            get_all_data()
        elif var == 4:
            hbo = HbaseOperation()
            hbo._delete_hbasetable()
            time.sleep(4)
            hbo1=HbaseOperation()
            hbo1._create_hbasetable()
            hbo2=HbaseOperation()
            hbo2.insert_data()
        elif var == 5:
            tabname=str(input("Please enter the sheet/table name for update\n-->"))
            hbo=HbaseOperation(tabname)
            hbo._delete_hbasetable()
            time.sleep(2)
            hbo._create_hbasetable()
            hbo.insert_data()
        elif var == 6:
            tabname = str(input("Please enter the sheet/table name for update\n-->"))
            hbo = HbaseOperation(tabname)
            hbo._delete_keydata_hbase()
            time.sleep(2)
            hbo.insert_data()
