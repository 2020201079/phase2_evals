import sqlparse
from sqlparse.sql import TokenList, Identifier, Statement
def print_details(token:sqlparse.sql.Token):
    if isinstance(token,TokenList):
        type_name=type(token).__name__
        print("isInstance typename: " + type_name)
    else:
        type_name=str(token.ttype)
        print("type of token is: " + type_name)
    #getting details
    token_details={}
    if isinstance(token, TokenList):
        token_details['alias']=token.get_alias()
        token_details['name']=token.get_name()
        token_details['parent']=token.get_parent_name()
        token_details['real_name']=token.get_real_name()
    if isinstance(token, Identifier):
        token_details['ordering']=token.get_ordering()
        token_details['typecast']=token.get_typecast()
        token_details['widcard']=token.is_wildcard()

    if len(token_details) != 0:
        print(token_details)


def print_tree(token_list:sqlparse.sql.TokenList):
    for i, token in enumerate(token_list):
        if isinstance(token,TokenList):
            if(token.is_group):
                print("GROUP")
                print(token)
                print_tree(token.tokens)
        else:
            print_details(token)
sql_statement = """
select eno,dno from Employee, Department where Employee.eno = Department.dno and eno = 5 group by 
dept_id having count(dept_id)>10
"""

# formatting the sql query
formatted_statement = sqlparse.format(sql_statement,reindent=True,keyword_case='upper')
#print(formatted_statement)
# parsing the formatted statement
parsed_statement = sqlparse.parse(formatted_statement)[0]
print(parsed_statement.tokens)
token_list=sqlparse.sql.TokenList(parsed_statement)
print_tree(token_list)



