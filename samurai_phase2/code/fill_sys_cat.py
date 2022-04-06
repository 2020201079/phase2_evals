from importlib.resources import path
from unicodedata import name



from pyparsing import col
from sqlalchemy import column


import common_utilities as cu


if __name__ == "__main__" :
    tunnel = cu.open_ssh_tunnel()
    connection = cu.mysql_connect(tunnel)
    app_tables_in_db = cu.get_app_tables_in_db(connection)
    col_names_in_db = cu.get_col_names_in_db(connection)
    table_names = cu.get_app_tables(cu.path_to_tables_folder)
    if not table_names: 
        print("CSV files for tables is not present")
        exit()
    # add entry in Application table,col_app_mapping and col_info
    for table_name in table_names:
        # case when the table is already present in db
        if table_name in app_tables_in_db:
            continue
        columns = cu.get_columns(table_name,cu.path_to_tables_folder)
        cu.add_table_name_in_db(connection,table_name,len(columns))
        app_tables_in_db.append(table_name)
        for col in columns:
            # col_id = len(col_names_in_db)+1
            col_id = cu.get_last_col_id_in_db(connection)+1
            cu.add_col_in_col_info_in_db(connection,str(col_id),col)
            
            #How to know from csv if the col is key or not
            key = False
            cu.add_col_app_mapping_in_db(connection,str(col_id),table_name,key)
            col_names_in_db.append(col)
    cu.mysql_disconnect(connection)
    cu.close_ssh_tunnel(tunnel)