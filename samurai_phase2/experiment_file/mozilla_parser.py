from moz_sql_parser import parse
import json
sql_statement = "select ename,location from EMP"
def extract_join_attributes(where_list,relation_names):
    join_attributes=list()
    select_attributes=list()
    # traversing all predicates present after where
    for dict in where_list:
        left_side_join = False
        right_side_join = False
        for i in dict:
            if i == "or":
                select_attributes.append(dict)
            else:
                temp_list=dict[i]
                if "." in temp_list[0]:
                    split_list=temp_list[0].split(".")
                    if split_list[0] in relation_names:
                        left_side_join=True

                if "." in temp_list[1]:
                    split_list=temp_list[1].split(".")
                    if split_list[0] in relation_names:
                        right_side_join=True

                if left_side_join and right_side_join:
                    join_attributes.append(dict)
                else:
                    select_attributes.append(dict)


    return join_attributes, select_attributes
json_dump = json.dumps(parse(sql_statement))
json_dictionary=json.loads(json_dump)
print("dictionary" ,':', json_dictionary)
leaf_attributes=json_dictionary["from"]
project_attributes=list()

for d in json_dictionary["select"]:
        project_attributes.append(d['value'])
print(project_attributes)
if "where" in json_dictionary:
        # separate join predicates and select predicate
        where_list=json_dictionary["where"]["and"]
        join_attributes, select_attributes=extract_join_attributes(where_list,leaf_attributes)
        for attributes in join_attributes:
            for i in attributes:
                print(i,":",attributes[i])
