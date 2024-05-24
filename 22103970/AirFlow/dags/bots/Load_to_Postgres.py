import psycopg2
import csv

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2


## Connecting

def get_connection(db = "airflow"):
    
    connection = psycopg2.connect(
        user = "airflow",
        password = "airflow",
        host = "localhost",
        port = "5432",
        database = db)
    
    connection.set_isolation_level(0) # AUTOCOMMIT
    
    return connection

db = Traffic_Data
def create_database(db, dropIfExists=True):
    try:
        dbConnection = get_connection()

        #dbConnection.set_isolation_level(0) # AUTOCOMMIT

        dbCursor = dbConnection.cursor()

        #Droping database if already exists.
        if dropIfExists:
            dbCursor.execute(f"DROP database IF EXISTS {db} WITH (FORCE);")

        dbCursor.execute(f'CREATE DATABASE {db};')
        dbCursor.close()

    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        if dbConnection in locals(): 
            dbConnection.close()


def checkIfTableExists(tableName):
    isExist = True
    print("Checking if table exist")
    try:
        dbConnection = get_connection(db)
            
        #dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName.lower(),))
        isExist = dbCursor.fetchone()[0]
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:        
        if dbConnection in locals():
            dbConnection.close()
        return isExist

def dropTable(tableName):
    try:
        dbConnection = get_connection(db)
            
        #dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("DROP table IF EXISTS " +tableName.lower())
        print("Table dropped..")
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while dropping table in PostgreSQL", dbError)
    finally:        
        if dbConnection in locals():
            dbConnection.close()

createTableQuery = """
CREATE TABLE Non_Motorist_Data (
	Incident_Id serial PRIMARY KEY,
	local_case_number VARCHAR ( 500 ),
    agency_name VARCHAR ( 500 ),
    acrs_report_type VARCHAR ( 500 ),
    related_non_motorist VARCHAR ( 500 ),
    collision_type VARCHAR ( 500 ),
    weather VARCHAR ( 500 ),
    surface_condition VARCHAR ( 500 ),
    light VARCHAR ( 500 ),
    traffic_control VARCHAR ( 500),
    driver_substance_abuse VARCHAR ( 500 ),
    non_motorist_substance_abuse VARCHAR ( 500 ),
    person_id VARCHAR ( 500 ),
    pedestrian_type VARCHAR ( 500 ),
    pedestrian_movement VARCHAR ( 500 ),
    pedestrian_actions VARCHAR ( 500 ),
    pedestrian_location VARCHAR ( 500 ),
    pedestrian_obeyed_traffic_signal VARCHAR ( 500 ),
    pedestrian_visibility VARCHAR ( 500 ),
    at_fault VARCHAR ( 500 ),
    injury_severity VARCHAR ( 500 ),
    safety_equipment VARCHAR ( 500 ),
    latitude VARCHAR ( 500 ),
    longitude VARCHAR ( 500 ),
    crash_Year VARCHAR ( 500 ),
    crash_Month VARCHAR ( 500 ),
    crash_Day VARCHAR ( 500 ),
    crash_Hour VARCHAR ( 500 )
);
"""

def createTable():
    if(checkIfTableExists("Non_Motorist_Data")):
        #drop table
        dropTable("Non_Motorist_Data")
    print("About to create table")
    try:
        dbConnection = get_connection(db)

        #dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute(createTableQuery)
        dbCursor.close()
        print("Table created")
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while creating table on PostgreSQL", dbError)
    finally:
        if dbConnection in locals(): 
            dbConnection.close()   
       
            



def populate_database_from_file(db, insert_sql, file):    
    
    try:
        dbConnection = get_connection("test")

        dbCursor = dbConnection.cursor()
        insertString = "INSERT INTO Non_Motorist_Data (local_case_number, agency_name, acrs_report_type, related_non_motorist, collision_type, weather, surface_condition, light, traffic_control, driver_substance_abuse, non_motorist_substance_abuse, person_id, pedestrian_type, pedestrian_movement, pedestrian_actions, pedestrian_location, pedestrian_obeyed_traffic_signal, pedestrian_visibility, at_fault, injury_severity, safety_equipment, latitude, longitude, crash_year, crash_month, crash_day, crash_hour) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"


        with open(file, 'r') as f: # ensure you chang
            reader = csv.reader(f)
            next(reader) # skip the header
            for row in reader:
                dbCursor.execute(insertString.format(*row))

        dbConnection.commit()
        dbCursor.close()

    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if dbConnection in locals(): 
            dbConnection.close()



insertString = "INSERT INTO Non_Motorist_Data (local_case_number, agency_name, acrs_report_type, related_non_motorist, collision_type, weather, surface_condition, light, traffic_control, driver_substance_abuse, non_motorist_substance_abuse, person_id, pedestrian_type, pedestrian_movement, pedestrian_actions, pedestrian_location, pedestrian_obeyed_traffic_signal, pedestrian_visibility, at_fault, injury_severity, safety_equipment, latitude, longitude, crash_year, crash_month, crash_day, crash_hour) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
populate_database_from_file(db, insertString, "clean_data.csv")