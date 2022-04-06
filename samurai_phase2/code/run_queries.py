import os

query_file_name = "queries.txt"
f = open(query_file_name, "r")
s = f.read()
s = s.split('\n\n')

queries = []
for query in s:
    query_list = query.split('\n')
    final_query = query_list[0] 
    for i in range(1,len(query_list)):
        final_query = final_query +' '+query_list[i]
    queries.append(final_query)

for query in queries:
    print("Query is : ")
    print(query)
    print("Tree is : ")
    command = "echo \"" + query + "\" | python3 main.py"  
    os.system(command)
