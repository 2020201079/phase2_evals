import json
class Node:
    def __init__(self, node_type, operation, operands):
        # children of node
        self.children = list()
        # parent node
        self.parent = None
        # data initialization
        self.node_type = node_type
        self.operation = operation
        self.operands = operands
    def display(self):
        if self.node_type == 'Project':
            data = self.node_type+" "+str(self.operands)
            return (data[:10] + '..') if len(data) > 10 else data
        else:
            data = self.node_type + " "+str(self.operation)
            return (data[:10] + '..') if len(data) > 10 else data


class LeafNode:
    def __init__(self, relation_name):
        # children of leaf node
        self.children = list()
        # parent node
        self.parent = None
        # node type
        self.node_type = "leaf"
        # data initialization
        self.relation_name = relation_name
    def display(self):
        data = str(self.relation_name)
        return (data[:10] + '..') if len(data) > 10 else data


class Tree:
    def __init__(self):
        self.root = None

    def getroot(self):
        return self.root

    def insert_intermediate_node(self, node_type, operation, operands):
        if self.root is None:
            self.root = Node(node_type,operation,operands)
        else:
            self.insert_intermediate_node1(node_type, operation, operands, self.root)
    def insert_intermediate_node1(self, node_type, operation, operands, node):
        if len(node.children) == 0:
            # insert the new node
            new_node = Node(node_type,operation,operands)
            node.children.append(new_node)
            new_node.parent = node
        else:
            self.insert_intermediate_node1(node_type,operation,operands,node.children[0])

    # This function is used to get the root node when its children is given
    def getParent(self, node):
        while node.parent is not None:
            node = node.parent

        return node
    # This function is used to create join attribute node and return the root node(bottom up manner)
    def join_two_nodes(self,operation,operands,leaf_nodes_address):
        left_relation = operands[0].split(".")[0]
        right_relation = operands[1].split(".")[0]
        root1 = self.getParent(leaf_nodes_address[left_relation])
        root2 = self.getParent(leaf_nodes_address[right_relation])
        new_node = Node("Join", operation, operands)
        new_node.children.append(root1)
        new_node.children.append(root2)
        root1.parent = new_node
        root2.parent = new_node
        return new_node

    # This function is used to join previously build tree and new join tree
    def join_two_trees(self,join_root):
        temp_node=self.getroot()
        while len(temp_node.children):
            temp_node = temp_node.children[0]

        temp_node.children.append(join_root)
        join_root.parent=temp_node

    # This function is used to display tree contents in preorder fashion
    def print_tree(self):
        if self.root is not None:
            self.print_inorder(self.root)

    def print_inorder(self,node):
        if node is not None:
            if node.node_type == "leaf":
                print(node.node_type, ":", node.relation_name)
            else:
                print(node.node_type , ":" , node.operation + ":" , node.operands)

            for c in node.children:
                self.print_inorder(c)
        else:
            return
    def print_bottom_up(self,node):
        while node is not None:
            if node.node_type == "leaf":
                print(node.node_type, ":", node.relation_name)
            else:
                print(node.node_type , ":" , node.operation + ":" , node.operands)

            node = node.parent

# This method extracts join attributes from where list and return dictionary
def extract_join_attributes(where_list,relation_names):
    join_attributes = list()
    select_attributes = list()
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

                temp_list[1]=str(temp_list[1])
                if "." in temp_list[1]:
                    split_list=temp_list[1].split(".")
                    if split_list[0] in relation_names:
                        right_side_join=True

                if left_side_join and right_side_join:
                    join_attributes.append(dict)
                else:
                    select_attributes.append(dict)


    return join_attributes, select_attributes

# This method is used to build tree given json dictionary as input
def build_tree(json_dictionary):
    # parse json_dictionary
    # extract project attributes
    project_attributes = list()
    group_by_attributes = list()
    leaf_attributes = list()
    select_attributes = list()
    join_attributes = list()
    # extract project attributes
    select_dict = json_dictionary["select"]
    if type(select_dict) is str:
        project_attributes.append('*')
    if type(select_dict) is dict:
        project_attributes.append(select_dict['value'])
    elif type(select_dict) is list:
        for d in select_dict:
            project_attributes.append(d['value'])

    # print(project_attributes)
    # add project attributes to operator tree
    operator_tree=Tree()
    operator_tree.insert_intermediate_node("Project","value",project_attributes)
    # extract having attributes and add to operator tree
    if "having" in json_dictionary:
        having_dict = json_dictionary["having"]
        if "and" in having_dict.keys():
            operator_tree.insert_intermediate_node("Having","",having_dict["and"])
        else:
            operator_tree.insert_intermediate_node("Having","", having_dict)
    # extract group by attributes and add to operator tree
    if "groupby" in json_dictionary:
        group_by_dict=json_dictionary["groupby"]
        if type(group_by_dict) is dict:
            group_by_attributes.append(group_by_dict['value'])
        elif type(group_by_dict) is list:
            for d in group_by_dict:
                group_by_attributes.append(d['value'])

        operator_tree.insert_intermediate_node("groupby","value",group_by_attributes)
    # extract relation names from dictionary
    if "from" in json_dictionary:
        leaf_attributes_dict=json_dictionary["from"]
        if type(leaf_attributes_dict) is str:
            leaf_attributes.append(leaf_attributes_dict)
        elif type(leaf_attributes_dict) is list:
            leaf_attributes = leaf_attributes_dict

    # add where clause attributes to operator tree
    if "where" in json_dictionary:
        # separate join predicates and select predicate
        where_dict=json_dictionary["where"]
        if "and" in where_dict.keys():
            where_list=json_dictionary["where"]["and"]
            join_attributes, select_attributes = extract_join_attributes(where_list, leaf_attributes)
        else:
            left_side_join = False
            right_side_join = False
            for i in where_dict:
                if i == "or":
                    select_attributes.append(where_dict)
                else:
                    temp_list = where_dict[i]
                    if "." in temp_list[0]:
                        split_list=temp_list[0].split(".")
                        if split_list[0] in leaf_attributes:
                            left_side_join=True

                    temp_list[1]=str(temp_list[1])
                    if "." in temp_list[1]:
                        split_list=temp_list[1].split(".")
                        if split_list[0] in leaf_attributes:
                            right_side_join=True

                    if left_side_join and right_side_join:
                        join_attributes.append(where_dict)
                    else:
                        select_attributes.append(where_dict)



        # adding select attributes to operator tree
        for attributes in select_attributes:
            for i in attributes:
                operator_tree.insert_intermediate_node("select", i, attributes[i])


    # adding leaf and join attributes to operator tree
    leaf_nodes_address={}
    # traverse leaf nodes and store address in leaf nodes address
    for leaf_value in leaf_attributes:
        leaf_nodes_address[leaf_value] = LeafNode(leaf_value)

    # traverse join attributes and build a tree from in bottom up manner
    # this root will be updated upon building the join tree
    # Assuming there is atleast one table (so initializing....)
    initial_root=leaf_nodes_address[leaf_attributes[0]]
    for attributes in join_attributes:
        for attr in attributes:
            initial_root = operator_tree.join_two_nodes(attr,attributes[attr],leaf_nodes_address)

    # now join the previously build tree to initial root
    operator_tree.join_two_trees(initial_root)
    # displaying the tree for checking purpose
    # operator_tree.print_tree()
    return operator_tree.getroot(), leaf_nodes_address





















