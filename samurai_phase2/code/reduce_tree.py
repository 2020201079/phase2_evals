# This file is used to perform reduction of optimized tree
import common_utilities as cu
import build_tree as bt
import optimize_tree
import copy
from collections import defaultdict
tunnel = cu.open_ssh_tunnel()
connection = cu.mysql_connect(tunnel)

# This is a helper function used to insert node between
def insert_node_between(parent_node,new_node,child_address):
    for index in range(0,len(parent_node.children)):
        if parent_node.children[index] == child_address:
            parent_node.children[index]=new_node
            new_node.parent=parent_node
            child_address.parent=new_node
            new_node.children.append(child_address)
            break
# This function is used to push select down
def push_select_down(relation_node, child_address):
    temp_node = relation_node.parent
    while temp_node.node_type == "select" or temp_node.node_type == "Project":
        new_node = bt.Node(temp_node.node_type,temp_node.operation,temp_node.operands)
        parent_node=child_address.parent
        insert_node_between(parent_node,new_node,child_address)
        child_address=child_address.parent
        temp_node=temp_node.parent

# This function is used to delete top select node
def delete_top_select(node_address):
    temp_node=node_address
    while temp_node.parent.node_type == "select" or temp_node.parent.node_type == "Project":
        temp_node=temp_node.parent

    parent_node=temp_node.parent
    for index in range(0,len(parent_node.children)):
        if parent_node.children[index] == temp_node:
            parent_node.children[index]=node_address
            node_address.parent=temp_node
            break
# This function is used to get project node from relation node
def get_project_node(relation_node):
    temp_node=relation_node
    while temp_node.node_type != "Project":
        temp_node=temp_node.parent

    return temp_node

# This function is used to get all the attributes that need to be projected
def get_project_attributes(relation_node,attribute_list):
    node=relation_node.parent
    if node is None:
        return
    if  node.node_type == "Project":
        for x in node.operands:
            x = x.split(".")
            relation_name = x[0]
            attribute_name = x[1]
            attribute_list.append(attribute_name)

        get_project_attributes(node,attribute_list)

    elif node.node_type == "select":
        if node.operation == 'or':
            for dict in node.operands:
                for i in dict:
                    temp_list = dict[i]
                    attribute = temp_list[0]
                    # print(attribute)
                    temp_list = attribute.split(".")
                    relation_name = temp_list[0]
                    attribute_name = temp_list[1]
                    attribute_list.append(attribute_name)

        else:
            temp_list=node.operands
            attribute=temp_list[0]
            temp_list=attribute.split(".")
            attribute_list.append(temp_list[1])

        get_project_attributes(node,attribute_list)

    else:
        return

def is_there_any_intersection(sel_pred,leaf_list):
    res = True
    if len(sel_pred) == 1:
        for leaf_pred in leaf_list:
            curr_res = cu.valid_pred(leaf_pred,sel_pred[0])
            res = res and curr_res
        return res
    else:
        # In or clause
        # leaf pred should match with any one atleast
        curr_result = False
        for leaf_pred in leaf_list:
            curr_res = False
            for sp in sel_pred:
                curr_res = curr_res or cu.valid_pred(sp,leaf_pred)
            if curr_res == False:
                return False
        return True

# This function reduces horizontal fragmentation
def reduce_hf(leaf_addr):
    # print("curr reduce on : ",leaf_addr.relation_name)
    # get predicate list
    pred_list = cu.get_predicate_logic_list_in_db(leaf_addr.relation_name, connection)
    should_delete = False
    parent_node = leaf_addr.parent
    while parent_node.node_type == "select":
        curr_pred = cu.format_predicate(parent_node)
        # print("curr_pred is : ",curr_pred)
        # print("frag pred is : ",pred_list)
        curr_res = is_there_any_intersection(curr_pred,pred_list)
        if(curr_res == False):
            return True
        parent_node = parent_node.parent
        # if len(curr_pred) > 1:
        #     # handling or condition
        #     for pred in pred_list:
        #         res = False
        #         for pred1 in curr_pred:
        #             curr_result = cu.valid_pred(pred,pred1)
        #             if curr_result == True:
        #                 res = True
        #                 break
        #         if res == False:
        #             should_delete = True
        #             return True

        # else:
        #     # handling single predicate
        #     for pred in pred_list:
        #         res = cu.valid_pred(pred,curr_pred[0])
        #         if res == False:
        #             # should_delete = True
        #             return True

        # parent_node = parent_node.parent

    return False

# This function is used to perform vertical fragmentation reduction
def ends_with(root,leaf):
    if leaf is None:
        return False
    if root == leaf :
        return True
    return ends_with(root,leaf.parent)

#this func should del the child which has given leaf as ending
def delete_child_with_given_leaf(root,leaf):
    new_children  = []
    for c in root.children:
        if not ends_with(c,leaf):
            new_children.append(c)
    root.children = new_children

def replace_node(root,node,new_node):
    if root is None:
        return
    for i in range(len(root.children)):
        if root.children[i] is not None and root.children[i] == node:
            root.children[i] = None
            root.children.append(new_node)
            new_node.parent = root
        else:
            replace_node(root.children[i],node,new_node)

# given root and node to del
def delete_node(root ,node):
    if root is None:
        return
    for i in range(len(root.children)):
        if root.children[i] is not None and root.children[i] == node:
            root.children[i] = None
        else:
            delete_node(root.children[i],node)

def delete_node_vf(root,leaf_addr):
    temp_node = leaf_addr
    while temp_node.node_type != "Join":
        # delete the node
        parent_node=temp_node.parent
        for index in range(0,len(parent_node.children)):
            if parent_node.children[index] == temp_node:
                parent_node.children[index]=None
                break
        temp_node=parent_node


# This function is used to perform vertical fragmentation
# returns should delete or not with the list of attributes to be projected
def reduce_vf(leaf_addr,table_name,key_atrr):
    col_name_list = cu.get_col_fragment_name_in_db(leaf_addr.relation_name,connection)
    # get the parent node operands
    parent_node = leaf_addr.parent
    project_atrr = parent_node.operands
    # print(project_atrr)
    temp_len = len(key_atrr)
    # traverse col_name_list and find whether col names present in project_attr
    need_to_project=list()
    for k in key_atrr:
        need_to_project.append(k)
    for col_name in col_name_list:
        if col_name in project_atrr and col_name not in key_atrr:
            need_to_project.append(col_name)
    if len(need_to_project) == temp_len:
        return True, need_to_project

    return False, need_to_project

# helper function to extract join attributese
def extract_join_attributes(operands):
    join_attribute=list()
    for op in operands:
        join_attribute.append(op.split(".")[1])
    return join_attribute

def is_valid_for_join_over_union(root):
    #figure out later why is a None type present in child 
    if root is None:
        return False
    if root.node_type != 'Join':
        return False
    else:
        #node is join node
        # atleast one of the children should be union
        for c in root.children:
            if c is not None and c.node_type == "union":
                return True
        return False

def get_lr_child(node):
    ans = []
    for c in node.children:
        if c is not None:
            ans.append(c)
    return ans

#returns a new node
def dist_join_over_union(node):
    # print("enter the case of dist join over union \n")
    # print("node is : "+node.node_type+" "+str(node.operands)+"\n")
    ## Two cases both union children or one of them is union
    first_two_children = get_lr_child(node)
    # print("curr child are : ",first_two_children)

    left_child = first_two_children[0]
    right_child = first_two_children[1]
    new_union_node = bt.Node("union", "","")
    if left_child.node_type == 'union' \
        and right_child.node_type =='union':
        # both the child are having union as children
        u1 = left_child
        u2 = right_child
        for c1 in u1.children:
            for c2 in u2.children:
                new_join_node = bt.Node("Join", node.operation,node.operands)
                new_join_node.children.append(c1)
                new_join_node.children.append(c2)
                new_join_node.parent = new_union_node
                new_union_node.children.append(new_join_node)

    elif left_child.node_type == 'union' \
        and right_child.node_type !='union':
        # left child is having union as child
        u1 = left_child
        c2 = right_child
        for c1 in u1.children:
            new_join_node = bt.Node("Join", node.operation,node.operands)
            new_join_node.children.append(c1)
            new_join_node.children.append(c2)
            new_join_node.parent = new_union_node
            new_union_node.children.append(new_join_node)

    elif left_child.node_type != 'union' \
        and right_child.node_type =='union':
        # righ child is union
        c1 = left_child
        u2 = right_child
        for c2 in u2.children:
            new_join_node = bt.Node("Join", node.operation,node.operands)
            new_join_node.children.append(c1)
            new_join_node.children.append(c2)
            new_join_node.parent = new_union_node
            new_union_node.children.append(new_join_node)
    else:
        return
    # print("Check new union node")
    # optimize_tree.print_tree(new_union_node)
    return new_union_node
    # replace_node(orig_root,node,new_union_node)
    #
    # par = node.parent
    # for i in range(len(par.children)):
    #     if par.children[i]==node:
    #         par.children[i] = new_union_node
    #         break
    # new_union_node.parent = par

def reduce_join_over_union(root):
    if root is None:
        return
    for i in range(len(root.children)):
        if root.children[i] is not None:
            reduce_join_over_union(root.children[i])
    if is_valid_for_join_over_union(root):
        #here need to perform union over joins
        new_union_node = dist_join_over_union(root)
        for i in range(len(root.parent.children)):
            if root.parent.children[i] == root:
                root.parent.children[i] = new_union_node
        new_union_node.parent = root.parent
        # new_union_node.parent = curr_parent
        # curr_parent.children.append(new_union_node)
        # delete_node(orig_root,root)
        # replace_node(orig_root,root,new_union_node)

def get_leaf_relations(root):
    if root is None: 
        return []
    if root.node_type == 'leaf':
        return [root]
    res = []
    for c in root.children:
        if c is not None:
            curr_res = get_leaf_relations(c)
            for temp in curr_res:
                res.append(temp)
    return res

def get_frag_parent(frag_name):
    parent_name = cu.get_frag_parent_name_from_frag_name(frag_name,connection)
    if parent_name == frag_name :
        return parent_name
    else:
        return get_frag_parent(parent_name)

def intersection(frag1,frag2,join_attr):
    # if there is no intersection between frag1 and frag2 return true
    # check the pred of frag1 and frag2 and see for any intersections
    # many cases here frag1 - dhf frag2 - dhf etc
    frag_type1 = cu.get_fragment_type_from_rel_name(frag1.relation_name,connection)
    frag_type2 = cu.get_fragment_type_from_rel_name(frag2.relation_name,connection)
    # print("f1 is : "+frag1.relation_name+" type is : "+frag_type1)
    # print("f2 is : "+frag2.relation_name+" type is : "+frag_type2)
    if (frag_type1 == "DHF" and frag_type2 == "DHF") \
        or (frag_type1 == "DHF" and frag_type2 == "HF") \
        or (frag_type1 == "HF" and frag_type2 == "DHF" ):
        frag_parent_name1 = get_frag_parent(frag1.relation_name)
        frag_parent_name2 = get_frag_parent(frag2.relation_name)
        rel_name1 = cu.get_relation_name_from_frag_name(frag_parent_name1,connection)
        rel_name2 = cu.get_relation_name_from_frag_name(frag_parent_name2,connection)
        if rel_name1 != rel_name2:
            return True
        if frag_parent_name1 == frag_parent_name2:
            return True
        return False

    elif frag_type1 == "HF" and frag_type2 == "HF":
        # get simple predicates of frag_type 1
        s_p_hf1 = cu.get_predicate_logic_list_in_db(frag1.relation_name)
        # get simple predicates of frag_type 2
        s_p_hf2 = cu.get_predicate_logic_list_in_db(frag2.relation_name)
        # get predicate columns of hf1
        hf1_predicate = ""
        hf2_predicate = ""
        for ele in s_p_hf1:
            temp_list = ele.split(" ")
            column = temp_list[0].split(".")[1]
            if column == join_attr[0]:
                hf1_predicate = ele

        # get predicate columns of hf2
        for ele in s_p_hf2:
            temp_list = ele.split(" ")
            column = temp_list[0].split(".")[1]
            if column == join_attr[1]:
                hf2_predicate = ele

        if(not hf1_predicate):
            return True
        if(not hf2_predicate):
            return True
        # check validity of hf1 and hf2
        return cu.valid_pred(hf1_predicate,hf2_predicate)


    else:
        return True

def get_non_none_children(root):
    ans = []
    for c in root.children:
        if c is not None:
            ans.append(c)
    return ans 

def reduce_dhf_joins(root):
    reduce_dhf_joins_helper(root,root)

def reduce_dhf_joins_helper(orig_root,root):
    if root is None:
        return
    for c in root.children:
        if c is not None:
            reduce_dhf_joins_helper(orig_root,c)
    # can reduce only join nodes
    # check if children fragments are compatible
    if root.node_type == 'Join':
        non_none_children = get_non_none_children(root)
        if len(non_none_children) == 1:
            # prob a single child join or many child in vf join
            return
        if root.operation == "vf":
            return

        left_children = get_leaf_relations(non_none_children[0])
        right_children = get_leaf_relations(non_none_children[1])
        # getting join attribute
        join_attribute = extract_join_attributes(root.operands)
        # Need to see if the leaves have any dhf relation or not
        # two for loops check every child with other and see if valid
        should_del = False
        for i in range(len(left_children)):
            for j in range(len(right_children)):
                if intersection(left_children[i],right_children[j],join_attribute):
                    continue
                else:
                    should_del = True
                    break
        if should_del:
            delete_node(orig_root,root)

    # print("Printing inside the func ")
    # optimize_tree.print_tree(root)
        
# This is the base function to reduce the tree
def reduce_tree(root, orginal_relation_address,relation_children):
    for key in relation_children.keys():
        leaf_nodes_list=relation_children[key]
        frag_type, table_name = cu.get_frag_type_parent_in_db(leaf_nodes_list[0].relation_name,connection)
        key_attribute = cu.get_key_attribute_in_db(table_name,connection)
        if frag_type == "HF":
            for leaf_addr in leaf_nodes_list:
                should_del = reduce_hf(leaf_addr)
                if should_del:
                     print("------enter del ---------")
                     print(leaf_addr.relation_name)
                     delete_child_with_given_leaf(orginal_relation_address[key],leaf_addr)
        elif frag_type == "VF":
            for leaf_addr in leaf_nodes_list:
                should_del,project_list = reduce_vf(leaf_addr, table_name,key_attribute)
                if should_del:
                        print("------enter del --------")
                        print(leaf_addr.relation_name)
                        delete_node_vf(orginal_relation_address[key],leaf_addr)

                else:
                    # project only necessary attributes
                    parent_node = leaf_addr.parent
                    parent_node.operands = project_list
    
    #traverse from top down. If you find a join node with union as child
    # distribute Union over join
    print("tree before distribution")
    print("-------------------------- \n\n\n\n")
    optimize_tree.print_tree(root)

    format_tree(root)
    print("Remove single joins here which comes in vf ")
    print("-------------------------- \n\n\n\n")
    optimize_tree.print_tree(root)


    reduce_join_over_union(root)
    print("tree after distribution")
    print("-------------------------- \n\n\n\n")
    optimize_tree.print_tree(root)


    reduce_dhf_joins(root)
    print("tree after reduction 1 ")
    print("-------------------------- \n\n\n\n")
    optimize_tree.print_tree(root)

    del_single_join_nodes(root)
    print("tree after reduction 2")
    print("-------------------------- \n\n\n\n")
    optimize_tree.print_tree(root)



# This is a helper function which is used to format tree
def format_tree(root):
    if root is None:
        return
    if root.node_type == "Join" or root.node_type == "union":
        if len(get_non_none_children(root)) == 1:
            # there is only single child so delete root
            parent_node = root.parent
            child_node = root.children[0]
            for c in root.children:
                if c is not None:
                    child_node = c
            for index in range(0,len(parent_node.children)):
                if parent_node.children[index] == root:
                    parent_node.children[index]=child_node
                    break
            child_node.parent=parent_node

    # traverse the children of root
    for child in root.children:
        format_tree(child)

def del_single_join_nodes(root):
    if root is None:
        return
    for i in range(len(root.children)):
        del_single_join_nodes(root.children[i])
    if root.node_type == "Join":
        if len(get_non_none_children(root)) <= 1:
            parent_node = root.parent
            for index in range(0,len(parent_node.children)):
                if parent_node.children[index] == root:
                    parent_node.children[index]= None
                    break

# This function used to perform data localization of optimized tree
def localize_tree(root,leaf_address):
    # traverse keys
    orginal_relation_address={}
    relation_children=defaultdict(list)
    for relation_name in leaf_address.keys():
        leaf_node_address = leaf_address[relation_name]
        fragment_name, frag_type = cu.get_fargment_name_type_in_db(relation_name,connection)
        # now add frag_name based on frag_type
        if frag_type == "HF" or frag_type == "DHF":
            parent_node = leaf_node_address.parent
            # create union node
            new_node = bt.Node("union", "","")
            orginal_relation_address[relation_name] = new_node
            # insert new_node as children to parent node
            for index in range(0,len(parent_node.children)):
                if parent_node.children[index] == leaf_node_address:
                    parent_node.children[index]=new_node
                    break
            new_node.parent=parent_node
            # add children of new_node by getting list
            for leaf_names in fragment_name:
                temp_node=bt.LeafNode(leaf_names)
                relation_children[relation_name].append(temp_node)
                temp_node.parent=new_node
                new_node.children.append(temp_node)
        else:
            parent_node = leaf_node_address.parent
            key_attribute = cu.get_key_attribute_in_db(relation_name, connection)
            # create Join node
            new_node = bt.Node("Join","vf",key_attribute)
            orginal_relation_address[relation_name] = new_node
            # insert new_node as children to parent node
            for index in range(0,len(parent_node.children)):
                if parent_node.children[index] == leaf_node_address:
                    parent_node.children[index]=new_node
                    break
            new_node.parent=parent_node
            # add children of new_node by getting list
            for leaf_names in fragment_name:
                temp_node=bt.LeafNode(leaf_names)
                relation_children[relation_name].append(temp_node)
                temp_node.parent=new_node
                new_node.children.append(temp_node)

    print("After Localization : \n")
    optimize_tree.print_tree(root)
    

    # push attributes down based on fragmentation type of each relation
    # traverse each relation
    for relation_name in orginal_relation_address.keys():
        relation_node=orginal_relation_address[relation_name]
        fragment_type = relation_node.node_type
        if fragment_type == "union":
            # traverse each children of relation
             for child_address in relation_children[relation_name]:
                 push_select_down(relation_node,child_address)
             # now delete the top select
             delete_top_select(relation_node)
        else:
            # Now fragment type is vertical fragmentation
            # get project node
            attribute_list=list()
            get_project_attributes(relation_node,attribute_list)
            # traverse each children of relation
            for child_address in relation_children[relation_name]:
                new_node=bt.Node('Project','val',attribute_list)
                parent_node=child_address.parent
                insert_node_between(parent_node,new_node,child_address)

    print("Tree after pushing select,project  down : \n")
    optimize_tree.print_tree(root)
    
    # Calling reduce tree after localisation is done
    reduce_tree(root,orginal_relation_address,relation_children)
    print("\n\n\nTree after all reductions : \n")
    optimize_tree.print_tree(root)


    # format_tree(root)
    # print("\n\n\nTree after formatting : \n")
    # optimize_tree.print_tree(root)

