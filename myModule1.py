#make connection
def server_connection(host_name, user_name, password):
    connection = None
    try:
        connection = mysql.connector.connect(host = host_name, user = user_name, passwd = password)
        print("connection seccessful")
    except Error as err:
            print(f"Error: '{err}'")
    return connection
pw = "batman"

connection = server_connection("localhost","root",pw)


#create database
def creat_db(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Db created")
    except Error as err:
        print(f"Error: '{err}'")
create_db_query = "create database mysql_python"
creat_db(connection, create_db_query)

#connect to database
def create_db_connection(host_name, user_name, password,db_name):
    connection = None
    try:
        connection = mysql.connector.connect(host = host_name, user = user_name, passwd = password, database = db_name)
        print("connection seccessful to db")
    except Error as err:
            print(f"Error: '{err}'")
    return connection
	
	#execute query

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query was successful")
    except Error as err:
        print(f"Error: '{err}'")
		
		
#create table
create_test_table = """
create table test_python1(
name varchar(10),
salary int,
id int primary key);
"""
connection = create_db_connection("localhost","root",pw,'mysql_python')
execute_query(connection, create_test_table)

#insert data in table
insert_query_in_test_python = """
insert into test_python values 
("Ashish", 300000),
("Advit", 400000),
("Vivek", 500000),
("Monika", 600000),
("Aman", 700000);
"""
connection = create_db_connection("localhost","root",pw,'mysql_python')
execute_query(connection, insert_query_in_test_python)


#read data from table
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
		
#Select statement query
select_query = """
select * from test_python;
"""
connection = create_db_connection("localhost","root",pw,'mysql_python')
results = read_query(connection, select_query)
for result in results:
    print(result)


#select in Data frames of pandas

import pandas as pd
select_query = """
SELECT * FROM test_python;
"""
connection = create_db_connection("localhost","root",pw,'mysql_python')
results = read_query(connection, select_query)
db_list = []
for result in results: 
    db_list.append(result)
columns = ["name","salary"]
df = pd.DataFrame(db_list,columns = columns)
print(db_list)
display(df)


#update query
update_query = """
update test_python
set salary = 900000
where name = 'Advit'
"""
connection = create_db_connection("localhost","root",pw,'mysql_python')
execute_query(connection, update_query)

#delete query
delete_query = """
delete from test_python
where name = 'Ashish'
"""
connection = create_db_connection("localhost","root",pw,'mysql_python')
execute_query(connection, delete_query)