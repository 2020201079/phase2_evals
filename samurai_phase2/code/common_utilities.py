## Printing the tree
import build_tree
from moz_sql_parser import parse
import json
import build_tree
import sys
import pymysql
import pandas as pd
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import os
import logging

def print_tree(root, val="val", left="left", right="right"):
    def display(root, val=val, left=left, right=right):
        """Returns list of strings, width, height, and horizontal coordinate of the root."""
        # No child.
        if getattr(root, right) is None and getattr(root, left) is None:
            # line = '%s' % getattr(root, val)
            line = root.display()
            width = len(line)
            height = 1
            middle = width // 2
            return [line], width, height, middle

        # Only left child.
        if getattr(root, right) is None:
            lines, n, p, x = display(getattr(root, left))
            # s = '%s' % getattr(root, val)
            s = root.display()
            u = len(s)
            first_line = (x + 1) * ' ' + (n - x - 1) * '_' + s
            second_line = x * ' ' + '/' + (n - x - 1 + u) * ' '
            shifted_lines = [line + u * ' ' for line in lines]
            return [first_line, second_line] + shifted_lines, n + u, p + 2, n + u // 2

        # Only right child.
        if getattr(root, left) is None:
            lines, n, p, x = display(getattr(root, right))
            # s = '%s' % getattr(root, val)
            s = root.display()
            u = len(s)
            first_line = s + x * '_' + (n - x) * ' '
            second_line = (u + x) * ' ' + '\\' + (n - x - 1) * ' '
            shifted_lines = [u * ' ' + line for line in lines]
            return [first_line, second_line] + shifted_lines, n + u, p + 2, u // 2

        # Two children.
        left, n, p, x = display(getattr(root, left))
        right, m, q, y = display(getattr(root, right))
        # s = '%s' % getattr(root, val)
        s = root.display()
        u = len(s)
        first_line = (x + 1) * ' ' + (n - x - 1) * '_' + s + y * '_' + (m - y) * ' '
        second_line = x * ' ' + '/' + (n - x - 1 + u + y) * ' ' + '\\' + (m - y - 1) * ' '
        if p < q:
            left += [n * ' '] * (q - p)
        elif q < p:
            right += [m * ' '] * (p - q)
        zipped_lines = zip(left, right)
        lines = [first_line, second_line] + [a + u * ' ' + b for a, b in zipped_lines]
        return lines, n + m + u, max(p, q) + 2, n + u // 2

    lines, *_ = display(root, val, left, right)
    for line in lines:
        print(line)

ssh_host = '10.3.5.211'
ssh_username = 'user'
ssh_password = 'iiit123'
database_username = 'user'
database_password = 'iiit123'
database_name = 'samurai'
localhost = '127.0.0.1'

path_to_tables_folder = 'sys_cat_details'


def mysql_connect(tunnel):
    
    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port
    )
    return connection

def mysql_disconnect(connection):
    """Closes the MySQL database connection.
    """
    
    connection.close()

def open_ssh_tunnel(verbose=False):    
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address = ('127.0.0.1', 3306)
    )
    
    tunnel.start()
    return tunnel

def get_app_tables_in_db(connection):
    df = run_query(connection,"select Table_Name from APPLICATION_TABLE")
    return df['Table_Name'].tolist()

def get_col_names_in_db(connection):
    df = run_query(connection,"select Col_Name from COL_INFO")
    return df['Col_Name'].tolist()

def add_table_name_in_db(connection,table_name,no_of_cols):
    # Execute the query
    sql = """insert into `APPLICATION_TABLE` (Table_Name,Number_Cols)
         values (%s, %s) 
    """
    cursor = connection.cursor()
    cursor.execute(sql,(table_name,no_of_cols))
    connection.commit()

def run_query(connection,sql):
    """Runs a given SQL query via the global database connection.
    
    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """
    return pd.read_sql_query(sql, connection)

def add_col_in_col_info_in_db(connection,col_id,col):
    # Execute the query
    sql = """insert into `COL_INFO` (Col_Id, Col_Name)
         values (%s, %s) 
    """
    cursor = connection.cursor()
    cursor.execute(sql,(col_id,col))
    connection.commit()

def add_col_app_mapping_in_db(connection,col_id,table_name,is_key):
    # Execute the query
    sql = """insert into `COL_APP_MAPPING` (Col_Id, Table_Name,Is_Key)
         values (%s, %s, %s) 
    """
    cursor = connection.cursor()
    cursor.execute(sql,(col_id,table_name,is_key))
    connection.commit()

def get_last_col_id_in_db(connection):
    # Execute the query
    sql = """select COALESCE(MAX(Col_Id), 0) as curr_max from COL_INFO """
    ans = run_query(connection,sql)
    return(ans['curr_max'].to_list()[0])

def get_app_tables(path):
    arr = os.listdir(path)
    table_names = []
    for name in arr:
        table_names.append(name[0:len(name)-4])
    return table_names

def get_columns(table_name,path_to_tables_folder):
    path_to_csv = path_to_tables_folder+'/'+table_name+'.csv'
    df = pd.read_csv (path_to_csv,usecols= ['ColumnNames'])
    columns = df['ColumnNames'].iloc[0]
    if not columns:
        print("colums of "+table_name+ " is missing " )
        exit()
    columns = columns.split()
    return columns

def close_ssh_tunnel(tunnel):
    tunnel.close


def format_predicate_helper(pred,key=None):
    if type(pred) is dict:
        for key in pred:
            variable = pred[key][0]
            literal = None
            rhs = pred[key][1]
            if type(rhs) is dict:
                literal = '\''+pred[key][1]['literal']+'\''
            else:
                literal = str(pred[key][1])
            result=""
            if key == 'eq':
                result = variable+" = "+literal
            elif key == 'neq':
                result = variable+" != "+literal
            elif key == 'gt':
                result = variable+" > "+literal
            elif key == 'lt':
                result = variable+" < "+literal
            elif key == 'gte':
                result = variable+" >= "+literal
            elif key == 'lte':
                result = variable+" <= "+literal
            return result
    else:
        s = pred
        # print("pred is : ",s)
        var = s[0]
        lit = s[1]
        lit = lit.strip()
        if ':' in lit:
            lit = lit.strip()
            lit = lit[1:len(lit)-1]
            lit_list = lit.split(':',1)
            lit = lit_list[1]
            lit = lit.strip()
        # else:

        result=""
        if key == 'eq':
            result = var+" = "+lit
        elif key == 'neq':
            result = var+" != "+lit
        elif key == 'gt':
            result = var+" > "+lit
        elif key == 'lt':
            result = var+" < "+lit
        elif key == 'gte':
            result = var+" >= "+lit
        elif key == 'lte':
            result = var+" <= "+lit
        return result

#Takes input as node. Returns list of predicates 
# present in this node 
# currently working for select. Extend to having by node too
def format_predicate(root):
    if not root:
        return
    if not root.node_type == "select":
        print("No predicates in this node")
        return []
    result = []
    if root.operation == 'or':
        for x in root.operands:
            result.append(format_predicate_helper(x))
    else:
        result.append(format_predicate_helper(root.operands,root.operation))
    return result


# given two preds check if it is valid
def valid_pred(p1,p2):
    var1,op1,lit1 = p1.split(' ',2)
    var2,op2,lit2 = p2.split(' ',2)
    # Given two cols are diff so these will be valid
    if var1 != var2: 
        return True
    
    #check if literals are strings or numerics
    if(lit1[0] == '\'' or lit1[0] == '\"'):
        lit1 = lit1[1:len(lit1)-1]
    else:
        lit1 = int(lit1)
    if(lit2[0] == '\'' or lit2[0] == '\"'):
        lit2 = lit2[1:len(lit2)-1]
    else:
        lit2 = int(lit2)

    if op1 == '=' and op2 == '=':
        return lit1 == lit2
    elif (op1 == '=' and op2 == '!=') or(op1 == '!=' and op2 == '='):
        return not(lit1 == lit2)
    elif (op1 == '=' and op2 == '<'):
        return lit2>lit1
    elif (op2 == '=' and op1 == '<'):
        return lit1>lit2
    elif (op1 == '=' and op2 == '>'):
        return lit2<lit1
    elif (op2 == '=' and op1 == '>'):
        return lit1<lit2
    elif (op1 == '=' and op2 == '<='):
        return lit2>=lit1
    elif (op2 == '=' and op1 == '<='):
        return lit1>=lit2
    elif (op1 == '=' and op2 == '>='):
        return lit2<=lit1
    elif (op2 == '=' and op1 == '>='):
        return lit1<=lit2
    elif op1 == '!=' and op2 == '!=':
        return True
    elif (op1 == '!=' and op2 == '<') or (op2 == '!=' and op1 == '<'):
        return True
    elif (op1 == '!=' and op2 == '>') or (op2 == '!=' and op1 == '>'):
        return True
    elif (op1 == '!=' and op2 == '<=') or (op2 == '!=' and op1 == '<='):
        return True
    elif (op1 == '!=' and op2 == '>=') or (op2 == '!=' and op1 == '>='):
        return True
    elif op1 == '<' and op2 == '<':
        return True
    elif op1 == '<' and op2 == '>':
        return lit2<lit1
    elif op1 == '>' and op2 == '<':
        return lit1<lit2
    elif (op1 == '<' and op2 == '<=') or (op2 == '<' and op1 == '<=') :
        return True
    elif op1 == '<' and op2 == '>=':
        return lit2<lit1
    elif op1 == '>=' and op2 == '<':
        return lit1<lit2
    elif op1 == '>' and op2 == '>':
        return True
    elif op1 == '>' and op2 == '<=':
        return lit1<lit2
    elif op2 == '>' and op1 == '<=':
        return lit2<lit1
    elif (op1 == '>' and op2 == '>=') or (op2 == '>' and op1 == '>='):
        return True
    elif op1 == '<=' and op2 == '<=':
        return True
    elif op1 == '<=' and op2 == '>=':
        return lit1>=lit2
    elif op2 == '<=' and op1 == '>=':
        return lit2>=lit1
    elif op1 == '>=' and op2 == '>=':
        return True
    return True

#---------helper functions still not used ----------

# takes table name and mysql connection returns 
# a tuple (fragment_type,[list of fragment ids])
def get_fargment_name_type_in_db(table_name,connection):
    tables_list_in_db = get_app_tables_in_db(connection)
    if table_name not in tables_list_in_db:
        print("Given table name does not exist in DB")
        exit()
    query = """select Frag_Name,Frag_Type 
            from FRAGMENTATION 
            where Table_Name = '{table_name}'""".format( table_name= table_name)
    df = run_query(connection,query)
    frag_id = df['Frag_Name'].to_list()
    frag_type = df['Frag_Type'].to_list()
    return (frag_id,frag_type[0])

def get_fragment_type_from_rel_name(frag_name,connection):
    query = """select Frag_Type 
            from FRAGMENTATION 
            where Frag_Name = '{frag_name}'""".format(frag_name= frag_name)
    df = run_query(connection,query)
    frag_type = df['Frag_Type'].to_list()
    return (frag_type[0])

def get_frag_parent_name_from_frag_name(frag_name,connection):
    query = """select Parent_Name 
        from FRAGMENTATION 
        where Frag_Name = '{frag_name}'""".format(frag_name= frag_name)
    df = run_query(connection,query)
    parent_name = df['Parent_Name'].to_list()
    return (parent_name[0])

def get_relation_name_from_frag_name(frag_name,connection):
    query = """select Table_Name 
        from FRAGMENTATION 
        where Frag_Name = '{frag_name}'""".format(frag_name= frag_name)
    df = run_query(connection,query)
    table_name = df['Table_Name'].to_list()
    return (table_name[0])



# get predicate logic list if fragment id is given as input
def get_predicate_logic_list_in_db(frag_name, connection):
    query = """
    select Pred_Logic from PREDICATE where Pred_Id in (select Pred_Id from FRAG_PRED_MAPPING where Fragment_Name = 
    '{frag_name}')
    """.format(frag_name=frag_name)

    df = run_query(connection,query)
    pred_logic=df['Pred_Logic'].to_list()
    return pred_logic

# get key attribute when relation name is given as input
def get_key_attribute_in_db(relation_name,connection):
    query="""
    select Col_Name from COL_INFO where Col_Id in (select Col_Id from COL_APP_MAPPING where Table_Name = '{relation_name}'
    and Is_Key=1)
    
    
    """.format(relation_name=relation_name)
    df = run_query(connection, query)
    key_attribute=df['Col_Name'].tolist()
    return key_attribute

# This function returns fragment type and its corresponding parent table
def get_frag_type_parent_in_db(fragment_name, connection):
    query="""
    select Frag_Type, Table_Name from FRAGMENTATION where Frag_Name = '{fragment_name}'
    """.format(fragment_name=fragment_name)
    df=run_query(connection,query)
    return df['Frag_Type'].to_list()[0], df['Table_Name'].to_list()[0]

# This function is used to get columns name when fragment_name is given as input
def get_col_fragment_name_in_db(fragment_name,connection):
    query="""
    select Col_Name from COL_INFO where Col_Id in (select Col_Id from FRAG_COL_INFO_MAPPING where Fragment_Name = '{fragment_name}') 
    """.format(fragment_name=fragment_name)
    df = run_query(connection,query)
    Col_Name_List = df['Col_Name'].to_list()
    return Col_Name_List

# This function is used to get all columns from relations when relations is given as input
def get_column_names(relation_name,connection):
    query=f"""
    select Col_Name from COL_INFO where Col_Id in (select Col_Id from COL_APP_MAPPING 
    where Table_Name ='{relation_name}')
    """.format(relation_name=relation_name)
    df = run_query(connection,query)
    Col_Name_List = df['Col_Name'].to_list()
    # format col_name_list
    for index in range(0,len(Col_Name_List)):
        Col_Name_List[index]=relation_name+"."+Col_Name_List[index]

    return Col_Name_List

if __name__ == '__main__':
    tunnel = open_ssh_tunnel()
    connection = mysql_connect(tunnel)
    key=get_key_attribute_in_db('CUSTOMER',connection)
    print(key)
    frag_type, table_name = get_frag_type_parent_in_db('VP1',connection)
    print(frag_type,':',table_name)
    col_names = get_col_fragment_name_in_db('VP1', connection)
    print(col_names)
    mysql_disconnect(connection)
    close_ssh_tunnel(tunnel)
