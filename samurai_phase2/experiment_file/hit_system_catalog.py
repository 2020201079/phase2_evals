import pymysql
import random
from pymysql.constants import CLIENT
import pymysql.cursors
ip_address = ['10.3.5.211', '10.3.5.208', '10.3.5.204', '10.3.5.205']
# This is a test function
def test_function(ip_address):
    conn = pymysql.connect(
        host=ip_address,
        user='ryuga',
        password="vishwak@1999",
        db='samurai',
        client_flag=CLIENT.MULTI_STATEMENTS,
    )

    cur = conn.cursor()
    cur.execute("select @@version")




    





# Driver Code
if __name__ == "__main__":
    # to hit system catalog get random number from 0 to 3(4 sites)
    hit_server_index = random.randint(0, 3)
    hit_server_ip=ip_address[hit_server_index]
    test_function(hit_server_ip)





