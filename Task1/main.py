import itertools
from itertools import groupby
import pandas as pd
import time
import csv
import mysql.connector
import sqlite3

class Main:
    data = None
    time = None
    mysql_conn = None
    sqlite_conn = None

    def __init__(self, filename, columns, do_insert_data_to_mysql=False, do_insert_data_to_sqlite=False, database_file='first_task.db'):
        print('__init__ - BEG')
        self.prepare_csv(filename=filename, columns=columns)

        self.create_connection_to_mysql()
        if do_insert_data_to_mysql == True:
            self.insert_data_to_mysql(filename=filename)

        self.create_connection_to_sqlite(database_file=database_file)
        if do_insert_data_to_sqlite == True:
            self.insert_data_to_sqlite(filename=filename)

        print('__init__ - END')

    def start_time(self, method_name):
        self.time = time.time()
        print(method_name + " - BEG") 

    def end_time(self, method_name):        
        elapsed_time = (time.time() - self.time)
        print(method_name + " - END in " + str(elapsed_time) + " s.")
        self.time = None

    def prepare_csv(self, filename, columns):
        self.start_time(method_name="prepare_csv")
        self.data = pd.read_csv(filename, usecols=columns)
        self.end_time(method_name="prepare_csv")

    def create_connection_to_mysql(self):
        self.mysql_conn = mysql.connector.connect(
            host="localhost",
            user="user",
            password="password",
            database="mydatabase"
            )

    def insert_data_to_mysql(self, filename, do_create_table=False):
        self.start_time(method_name="insert_data_to_mysql")

        mycursor = self.mysql_conn.cursor()

        try:
            if do_create_table == True:
                mycursor.execute("CREATE TABLE first_task (id INT, borough VARCHAR(255), complaint_type VARCHAR(255), agency_name VARCHAR(255))")
                self.mysql_conn.commit()

            for index, row in self.data.iterrows():
                # print(index, row['Unique Key'], row['Borough'], row['Complaint Type'], row['Agency Name'])
                
                sql = "INSERT INTO first_task (id, borough, complaint_type, agency_name) VALUES (%s, %s, %s, %s)"
                val = (row['Unique Key'], row['Borough'], row['Complaint Type'], row['Agency Name'])
                mycursor.execute(sql, val)
                
            self.mysql_conn.commit()
            print(mycursor.rowcount, " record inserted.")

        except Exception as ex:
            print(ex)
        
        self.end_time(method_name="insert_data_to_mysql")

    def create_connection_to_sqlite(self, database_file):
        self.sqlite_conn = sqlite3.connect(database_file)

    def insert_data_to_sqlite(self, filename, do_create_table=False):
        self.start_time(method_name="insert_data_to_sqlite")

        try:
            if do_create_table == True:
                self.sqlite_conn.execute("CREATE TABLE first_task (id INT, borough VARCHAR(255), complaint_type VARCHAR(255), agency_name VARCHAR(255))")

            for index, row in self.data.iterrows():
                # print(index, row['Unique Key'], row['Borough'], row['Complaint Type'], row['Agency Name'])
                
                sql = "INSERT INTO first_task (id, borough, complaint_type, agency_name) VALUES (%s, %s, %s, %s)"
                val = (row['Unique Key'], row['Borough'], row['Complaint Type'], row['Agency Name'])
                self.sqlite_conn.execute(sql, val)
                
            print("All record inserted.")

        except Exception as ex:
            print(ex)

        self.end_time(method_name="insert_data_to_sqlite")


    def get_the_most_frequently_reported_complaints(self):
        self.start_time(method_name="get_the_most_frequently_reported_complaints")
        result = self.data['Complaint Type'].value_counts().idxmax()
        print("The most frequently reported complaint is: " + result)
        self.end_time(method_name="get_the_most_frequently_reported_complaints")
        
    def get_the_most_frequently_reported_complaints_from_mysql(self):
        self.start_time(method_name="get_the_most_frequently_reported_complaints_from_mysql")
        result = None
        mycursor = self.mysql_conn.cursor()

        try:
            mycursor.execute("""
            SELECT COUNT(id), complaint_type
            FROM first_task
            GROUP BY complaint_type
            ORDER BY COUNT(id) DESC;
            """)
            result = mycursor.fetchall()

        except Exception as ex:
            print(ex)

        print("[MySQL] The most frequently reported complaint is: ")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_complaints_from_mysql")
        
    def get_the_most_frequently_reported_complaints_from_sqlite(self):
        self.start_time(method_name="get_the_most_frequently_reported_complaints_from_sqlite")
        result = None

        try:
            result = self.sqlite_conn.execute("""
            SELECT COUNT(id), complaint_type
            FROM first_task
            GROUP BY complaint_type
            ORDER BY COUNT(id) DESC;
            """)

        except Exception as ex:
            print(ex)

        print("[SQLite] The most frequently reported complaint is: ")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_complaints_from_sqlite")



    def get_the_most_frequently_reported_complaints_in_each_borough(self):
        self.start_time(method_name="get_the_most_frequently_reported_complaints_in_each_borough")
        result = self.data.groupby('Borough')['Complaint Type'].apply(lambda x: x.value_counts().index[0]).reset_index()
        print("The most frequently reported complaint in each borough is:")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_complaints_in_each_borough")
        
    def get_the_most_frequently_reported_complaints_in_each_borough_from_mysql(self):
        self.start_time(method_name="get_the_most_frequently_reported_complaints_in_each_borough_from_mysql")
        result = None
        mycursor = self.mysql_conn.cursor()

        try:
            mycursor.execute("""
            SELECT COUNT(id), complaint_type, borough
            FROM first_task
            GROUP BY complaint_type, borough
            ORDER BY COUNT(id) DESC;
            """)
            result = mycursor.fetchall()

        except Exception as ex:
            print(ex)

        print("[MySQL] The most frequently reported complaint in each borough is:")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_complaints_in_each_borough_from_mysql")
        
    def get_the_most_frequently_reported_complaints_in_each_borough_from_sqlite(self):
        self.start_time(method_name="get_the_most_frequently_reported_complaints_in_each_borough_from_sqlite")
        result = None

        try:
            result = self.sqlite_conn.execute("""
            SELECT COUNT(id), complaint_type, borough
            FROM first_task
            GROUP BY complaint_type, borough
            ORDER BY COUNT(id) DESC;
            """)

        except Exception as ex:
            print(ex)

        print("[SQLite] The most frequently reported complaint in each borough is:")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_complaints_in_each_borough_from_sqlite")



    def get_the_most_frequently_reported_agency_name(self):
        self.start_time(method_name="get_the_most_frequently_reported_agency_name")
        result = self.data['Agency Name'].value_counts().idxmax()
        print("The most frequently reported agency name is: " + result)
        self.end_time(method_name="get_the_most_frequently_reported_agency_name")
    
    def get_the_most_frequently_reported_agency_name_from_mysql(self):
        self.start_time(method_name="get_the_most_frequently_reported_agency_name_from_mysql")
        result = None
        mycursor = self.mysql_conn.cursor()

        try:
            mycursor.execute("""
            SELECT COUNT(id), agency_name
            FROM first_task
            GROUP BY agency_name
            ORDER BY COUNT(id) DESC;
            """)
            result = mycursor.fetchall()

        except Exception as ex:
            print(ex)

        print("[MySQL] The most frequently reported agency name is: ")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_agency_name_from_mysql")
    
    def get_the_most_frequently_reported_agency_name_from_sqlite(self):
        self.start_time(method_name="get_the_most_frequently_reported_agency_name_from_sqlite")
        result = None

        try:
            result = self.sqlite_conn.execute("""
            SELECT COUNT(id), complaint_type, borough
            FROM first_task
            GROUP BY complaint_type, borough
            ORDER BY COUNT(id) DESC;
            """)

        except Exception as ex:
            print(ex)


        print("[SQLite] The most frequently reported agency name is: ")
        print(result)
        self.end_time(method_name="get_the_most_frequently_reported_agency_name_from_sqlite")



    



csv = Main(filename='./311_Service_Requests_from_2010_to_Present.csv', columns=['Unique Key', 'Borough', 'Complaint Type', 'Agency Name'], do_insert_data_to_mysql=True, do_insert_data_to_sqlite=True)
print("Part 1")
csv.get_the_most_frequently_reported_complaints()
csv.get_the_most_frequently_reported_complaints_in_each_borough()
csv.get_the_most_frequently_reported_agency_name()

print("Part 2")
print("MySQL")
csv.get_the_most_frequently_reported_complaints_from_mysql()
csv.get_the_most_frequently_reported_complaints_in_each_borough_from_mysql()
csv.get_the_most_frequently_reported_agency_name_from_mysql()

print("SQLite")
csv.get_the_most_frequently_reported_complaints_from_sqlite()
csv.get_the_most_frequently_reported_complaints_in_each_borough_from_sqlite()
csv.get_the_most_frequently_reported_agency_name_from_sqlite()
