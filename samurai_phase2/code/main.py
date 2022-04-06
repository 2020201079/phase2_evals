from moz_sql_parser import parse
import json
import build_tree
import optimize_tree
import common_utilities as cu
import reduce_tree
import sys


predicate_list = []
# using this to just call the formatter remove later
def helper_func(root):
    if root.node_type == "select":
        formatted_pred = cu.format_predicate(root)
        # print("Original : ",root.operands)
        # print("formatted : ",formatted_pred)
        for p in formatted_pred:
            predicate_list.append(p)
    for c in root.children:
        helper_func(c)

def helper_check_pred():
    print("Printing all predicates")
    print(predicate_list)
    for i in range(len(predicate_list)):
        for j in range(i,len(predicate_list)):
            result = cu.valid_pred(predicate_list[i],predicate_list[j])
            # print(predicate_list[i])
            # print(predicate_list[j])
            # print("above predicates are : ",result)
            # print()
            # print()
    


def main() -> int:
    while True:
        # reading sql statement
        # sql_statement = """
        # SELECT ACCOUNT.account_status
        # FROM ACCOUNT,BRANCH
        # WHERE ACCOUNT.branch_id = BRANCH.branch_id
        # AND (BRANCH.branch_city='Hyderabad'
        #  OR BRANCH.branch_city='Mumbai')
        # """

        # sql_statement = """
        # select CUSTOMER.first_name from CUSTOMER,ACCOUNT,TRAN_DETAILS
        # WHERE CUSTOMER.cust_id = ACCOUNT.cust_id
        # AND TRAN_DETAILS.account_number = ACCOUNT.account_number
        # AND ACCOUNT.account_status = 'active'
        # AND TRAN_DETAILS.amount > 5000
        # AND CUSTOMER.age < 50
        #
        # """
        # sql_statement = """
        # select faculty.fname from faculty,labs
        # where  faculty.labId = labs.lab_id
        # and labs.lab_location = 'KCIS'
        # """

        sql_statement = """
        select students.branch from students,faculty
        where  faculty.faculty_id = students.facId
        """

        #sql_statement = input()
        # print("sql recieved : ",sql_statement)
        # parsing sql statement
        json_dump=json.dumps(parse(sql_statement))
        # loading json object into dictionary
        json_dictionary=json.loads(json_dump)
        # print(json_dictionary)
        # build tree
        print("initial tree : \n")
        o_t_root, leaf_address=build_tree.build_tree(json_dictionary)
        optimize_tree.print_tree(o_t_root)
    
        # optimize tree
        print("trees after optimization : \n")
        optimize_tree.optimize_tree(o_t_root,leaf_address)
        optimize_tree.print_tree(o_t_root)
        # perform localization
        
        print("tree after localization")
        #print calls are present inside this localize_tree call
        reduce_tree.localize_tree(o_t_root,leaf_address)
        # After performing reduction format the tree by removing the union or join nodes with single children
        # reduce_tree.format_tree(o_t_root)
        # localization(hit system catalog(fragmentation scheme))

        return 0

if __name__ == '__main__':
    sys.exit(main())

