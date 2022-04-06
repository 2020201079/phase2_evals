# This file contains the code to optimize the build tree

import build_tree
import common_utilities as cu
from collections import defaultdict
tunnel = cu.open_ssh_tunnel()
connection = cu.mysql_connect(tunnel)

# This relation is used to check if relation names are same or not in case of or
def same_relation(operands):
    relation_list=list()
    for dict in operands:
        for i in dict:
            temp_list=dict[i]
            attribute=temp_list[0]
            relation=attribute.split(".")[0]
            relation_list.append(relation)

    for i in range(1,len(relation_list)):
        if relation_list[i] != relation_list[i-1]:
            return False


    return True, relation_list[0]

# This relation is used to get relation name given operands
def get_relation_name(operands):
    relation_name=operands[0].split(".")[0]
    return relation_name

# This relation is used to get select attributes
def get_select_attributes(node, select_node_list):
    if node is None:
        return
    if node.node_type == "select":
        # check whether operation is or or not
        if node.operation == "or":
            result, relation_name=same_relation(node.operands)
            if result:
                # insert node into dictionary
                select_node_list[relation_name].append(node)
                # delete node from tree
                # getting parent node
                parent_node = node.parent
                # initializing new child of parent node
                parent_node.children[0] = node.children[0]
                # initializing parent of node.left
                node.children[0].parent = parent_node

        else:
            relation_name = get_relation_name(node.operands)
            # insert node into dictionary
            select_node_list[relation_name].append(node)
            # delete node from tree
            # getting parent node
            parent_node = node.parent
            # initializing new child of parent node
            parent_node.children[0] = node.children[0]
            # initializing parent of node.left
            node.children[0].parent = parent_node

    for c in node.children:
        get_select_attributes(c,select_node_list)

def get_project_attributes_from_node(node):
    if node is None:
        return
    list_of_attributes = []
    for x in node.operands:
        list_of_attributes.append(x.split(".")[1])
    return list_of_attributes
# function used to make a dict --> set
# saving the attributes for each relation
def build_relation_attribute(node,relation_attribute_dict):
    if node is None:
        return
    if node.node_type == "Project":
        for x in node.operands:
            x = x.split(".")
            relation_name = x[0]
            attribute_name = x[1]
            relation_attribute_dict[relation_name].add(attribute_name)
    elif node.node_type == "select":
        if(node.operation == 'or'):
           relation_names=list()
           for dict in node.operands:
               for i in dict:
                   temp_list=dict[i]
                   attribute=temp_list[0]
                   relation_names.append(attribute.split(".")[0])


           # check whether relation same or not
           all_equal=True
           for i in range(1,len(relation_names)):
               if relation_names[i] != relation_names[i-1]:
                   all_equal=False
                   break

           if all_equal:
               return

           for dict in node.operands:
                for i in dict:
                    temp_list = dict[i]
                    attribute = temp_list[0]
                    print(attribute)
                    temp_list = attribute.split(".")
                    relation_name = temp_list[0]
                    attribute_name = temp_list[1]
                    relation_attribute_dict[relation_name].add(attribute_name)

        else:
            return
    elif node.node_type == "groupby":
        for x in node.operands:
            x = x.split(".")
            relation_name = x[0]
            attribute_name = x[1]
            relation_attribute_dict[relation_name].add(attribute_name)
    elif node.node_type == "Join":
        for x in node.operands:
            x = x.split(".")
            relation_name = x[0]
            attribute_name = x[1]
            relation_attribute_dict[relation_name].add(attribute_name)

    for c in node.children:
        build_relation_attribute(c,relation_attribute_dict)
    # build_relation_attribute(node.left,relation_attribute_dict)
    # build_relation_attribute(node.right,relation_attribute_dict)

# This function is used to push down select nodes to down
def push_attributes(bottom_root, node_content,relation_name):
    while bottom_root.parent.node_type == "select":
        bottom_root=bottom_root.parent

    parent_node=bottom_root.parent
    if parent_node.node_type == "Join":
        join_list=parent_node.operands
        if relation_name == join_list[0].split(".")[0]:
            # relation present on left side
            node_content.children.clear()
            node_content.children.append(bottom_root)
            bottom_root.parent=node_content
            parent_node.children[0] = node_content
            node_content.parent = parent_node

        else:
            # relation present on right side of join attribute
            node_content.children.clear()
            node_content.children.append(bottom_root)
            bottom_root.parent=node_content
            parent_node.children[1] = node_content
            node_content.parent = parent_node



    else:
        node_content.children.clear()
        node_content.children.append(node_content)
        bottom_root.parent=node_content
        parent_node.children[0]=node_content
        node_content.parent=parent_node

def print_tree(root):
        if root is not None:
            print_inorder(root)
        print("\n\n")
        print("----------------------------------------------")

def print_inorder(node):
    if node is not None:
        if node.node_type == "leaf":
            print(node.node_type, ":", node.relation_name)
        else:
            # if node.node_type != "select" and node.node_type != "Project":
            print(node.node_type , ":" , node.operation + ":" , node.operands)

        for c in node.children:
           print_inorder(c)
    else:
        return

def make_project_node(relation_name,attr):
    operands = []
    for a in attr:
        operands.append(relation_name+'.'+a)
    curr_node = build_tree.Node('Project','value',operands)
    return curr_node

# This function pushes all the attributes for each relation up
def push_node_up(curr_node,attributes_list,relation_name):
    while curr_node.parent.node_type == "select":
        curr_node=curr_node.parent

    new_node = make_project_node(relation_name,attributes_list)
    temp_node = curr_node.parent
    if temp_node.children[0] == curr_node:
        temp_node.children[0]=new_node
        new_node.parent=temp_node
        new_node.children.append(curr_node)
        curr_node.parent=new_node
    else:
        temp_node.children[1]=new_node
        new_node.parent=temp_node
        new_node.children.append(curr_node)
        curr_node.parent=new_node

def optimize_tree(root, leaf_address):
    # get select attributes from top down approach
    # handle the case of select *
    # checking whether root operand is *
    project_list=root.operands
    if project_list[0] == '*':
        #replace root operands
        final_col_list=list()
        for relation_name in leaf_address.keys():
            temp_list = cu.get_column_names(relation_name,connection)
            for e in temp_list:
                final_col_list.append(e)

        root.operands = final_col_list
    if len(leaf_address) == 1:
        print("Tree after pushing select attributes down : ")
        print()
        temp_node = root
        print_tree(temp_node)
        return

    temp_node = root
    # dictionary used to store select nodes for every relation
    select_node_list = defaultdict(list)
    get_select_attributes(temp_node, select_node_list)
    # print(select_node_list)
    # push select node list contents near to leaf nodes
    
    for relation_name in select_node_list.keys():
        for node_content in select_node_list[relation_name]:
            # print(relation_name,":",node_content.node_type,":",node_content.operands,":",node_content.operation)
            temp_node=leaf_address[relation_name]
            push_attributes(temp_node,node_content,relation_name)

    #make a dict relation --> attributes used
    relation_attribute_dict = defaultdict(set)
    build_relation_attribute(root,relation_attribute_dict)
    for relation_name in relation_attribute_dict.keys():
        curr_node = leaf_address[relation_name]
        push_node_up(curr_node,relation_attribute_dict[relation_name],relation_name)

    # print preorder traversal of tree
    # print("Tree after pushing select attributes down")
    # temp_node = root
    # print_tree(temp_node)

