"""
Author : Laurent Cesaro
Topic : Python file to load start data in the database
 - Airline companies data
 - Planes data
 - Airports data
"""

import json
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine, insert, Table, Column, Float, String, Integer, DateTime, Boolean, MetaData, select
import datetime
import sys


# Function to load JSON data
def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)
    
# Function to insert data into Company table
def insert_airport_data(conn, airport_table, airport_data):
    with conn.connect() as conn:
        for doc_ in airport_data.items():
            AirportCode_ = doc_[0]
            Latitude_ = doc_[1][0]
            Longitude_ = doc_[1][1]
            LandingPrice_ = doc_[1][2]
            AirportCountry_ = doc_[1][3]
            query = insert(airport_table).values(AirportCode=AirportCode_, 
                                                Latitude=Latitude_, 
                                                Longitude=Longitude_, 
                                                LandingPrice=LandingPrice_, 
                                                AirportCountry=AirportCountry_)
            conn.execute(query)
            conn.commit()
            
# Function to insert data into Plane_Company table
def insert_plane_company_data(conn, company_table, plane_company_data):
    with conn.connect() as conn:
        for doc_ in plane_company_data.items():
            Name_ = doc_[0]
            Country_ = doc_[1]['country']
            IATACode_ = doc_[1]['iata_code']
            query = insert(company_table).values(Name=Name_, Country=Country_, IATACode=IATACode_)
            conn.execute(query)
            conn.commit()

# Function to insert data into Plane table
def insert_plane_data(conn, company_table, plane_table, plane_data, airline_plane_data):
    with conn.connect() as conn:
        for doc_ in airline_plane_data.items():
            slct_query = select(company_table).where(company_table.c.Name == doc_[0])
            result = conn.execute(slct_query)
            row = result.fetchone()
            # Loop through and print the result
            if row:
                CompanyID_ = int(row[0])
                for plane_ in doc_[1]['planes']:
                    Model_ = plane_data[plane_]['model']
                    Manufacturer_ = plane_data[plane_]['manufacturer']
                    RangeKM_ = plane_data[plane_]['range_km']
                    PassengerCapacity_ = plane_data[plane_]['passenger_capacity']
                    CruisingSpeedKPH_ = plane_data[plane_]['cruising_speed_kph']
                    WeightKG_ = plane_data[plane_]['weight_kg']
                    TankCapacityInGallon_ = plane_data[plane_]['tank_capacity_in_gallon']
                    insert_query = insert(plane_table).values(Model=Model_, 
                                                    Manufacturer=Manufacturer_, 
                                                    RangeKM=RangeKM_,
                                                    PassengerCapacity=PassengerCapacity_,
                                                    CruisingSpeedKPH=CruisingSpeedKPH_,
                                                    WeightKG=WeightKG_,
                                                    TankCapacityInGallon=TankCapacityInGallon_,
                                                    CompanyID=CompanyID_)
                    conn.execute(insert_query)
                    conn.commit()
   
def insert_plane_status(conn, plane_table, plane_status_table, airport_table, airport_data):
    with conn.connect() as conn:
        # Step 1: Fetch all PlaneIDs from the Plane table
        select_query = select(plane_table)
        result = conn.execute(select_query)

        # Step 2: For each plane, insert a record into PlaneStatus with InFlight = 0
        for plane in result:
            # Select a random airport
            random_airport = random.choice(list(airport_data.items()))
            airport_code, _ = random_airport
            
            query_aiportID = (
                    select(airport_table.c.AirportID)
                    .select_from(airport_table)
                    .where(airport_table.c.AirportCode == airport_code)
                )
            aiportID_ = conn.execute(query_aiportID).scalar()

            plane_id = plane[0]  # Get the PlaneID
            insert_query = insert(plane_status_table).values(InFlight=False, AirportID=aiportID_, PlaneID=plane_id)
            conn.execute(insert_query)

        # Commit the changes
        conn.commit()

# Main function
def main():
    # Load JSON data
    plane_data = load_json('/home/laurent/docker/airflight_project/Airflight-Simulator/data_loader/plane_data.json')
    airline_companies = load_json('/home/laurent/docker/airflight_project/Airflight-Simulator/data_loader/airline_companies.json')
    airline_plane_data = load_json('/home/laurent/docker/airflight_project/Airflight-Simulator/data_loader/airline_plane_data.json')
    airport_coordinates = load_json('/home/laurent/docker/airflight_project/Airflight-Simulator/data_loader/airport_coordinates.json')

    # Define metadata
    metadata = MetaData()

    # Define the 'airport' table
    airport_table = Table(
        'Airport', metadata,
        Column('AirportID', Integer, primary_key=True, autoincrement=True),
        Column('AirportCode', String(3)),
        Column('Latitude', Float),
        Column('Longitude', Float),
        Column('LandingPrice', Float),
        Column('AirportCountry', String(255))
    )
    # Define the 'company' table
    company_table = Table(
        'Company', metadata,
        Column('CompanyID', Integer, primary_key=True, autoincrement=True),
        Column('Name', String(255)),
        Column('Country', String(255)),
        Column('IATACode', String(3))
    )
    
    # Define the 'Plane' table
    plane_table = Table(
        'Plane', metadata,
        Column('PlaneID', Integer, primary_key=True, autoincrement=True),
        Column('Model', String(255)),
        Column('Manufacturer', String(255)),
        Column('RangeKM', Integer),
        Column('PassengerCapacity', Integer),
        Column('CruisingSpeedKPH', Integer),
        Column('WeightKG', Integer),
        Column('TankCapacityInGallon', Integer),
        Column('CompanyID', Integer)
    )

    # Define the 'Plane' table
    plane_status_table = Table(
        'Plane_Status', metadata,
        Column('PlaneStatusID', Integer, primary_key=True, autoincrement=True),
        Column('InFlight', Boolean),
        Column('AirportID', Integer),
        Column('PlaneID', Integer)
    )

    # Connect to the database
    # Database connection configuration
    DATABASE_TYPE = 'mysql'
    DBAPI = 'mysql+mysqlconnector'  # Adjust based on your SQLAlchemy version
    ENDPOINT = 'localhost'
    PORT = 3306
    USER = 'laurent'  # Replace with your actual MySQL username
    PASSWORD = '123456789'  # Replace with your actual MySQL password
    DATABASE = 'AIRFLIGHT_DB'

    # Construct the SQLAlchemy URL
    db_url = f"{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

    print("Connecting to database with URL:", db_url)

    # Create database connection
    try:
        engine = create_engine(db_url)
        print("Database connection successful")
    except Exception as e:
        print("Error connecting to database:", str(e))
        
    try:
        # Insert airport data in Airport Table
        insert_airport_data(engine, airport_table, airport_coordinates)
        # Insert company data in Company Table
        insert_plane_company_data(engine, company_table, airline_companies)
        # Insert plane data in Plane Table
        insert_plane_data(engine, company_table, plane_table, plane_data, airline_plane_data)
        # Insert Plane Status
        insert_plane_status(engine, plane_table, plane_status_table, airport_table, airport_coordinates)
        print('Data properly inserted !')
    except Exception as e:
        print("Error inserting data:", str(e))

if __name__ == "__main__":
    main()
