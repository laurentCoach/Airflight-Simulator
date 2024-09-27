"""
Author : Laurent Cesaro
Topic : Python file to load start data in the database
 - Airline companies data
 - Planes data
 - Airports data
"""

import json
from datetime import datetime, timedelta

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
            stmt = insert(airport_table).values(AirportCode=AirportCode_, Latitude=Latitude_, Longitude=Longitude_)
            conn.execute(stmt)
            conn.commit()
            
# Function to insert data into Plane_Company table
def insert_plane_company_data(conn, company_table, plane_company_data):
    with conn.connect() as conn:
        for doc_ in plane_company_data.items():
            Name_ = doc_[0]
            Country_ = doc_[1]['country']
            IATACode_ = doc_[1]['iata_code']
            stmt = insert(company_table).values(Name=Name_, Country=Country_, IATACode=IATACode_)
            conn.execute(stmt)
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
                    Capacity_ = plane_data[plane_]['capacity']
                    CruisingSpeedKPH_ = plane_data[plane_]['cruising_speed_kph']
                    WeightKG_ = plane_data[plane_]['WeightKG']
                    insert_query = insert(plane_table).values(Model=Model_, 
                                                    Manufacturer=Manufacturer_, 
                                                    RangeKM=RangeKM_,
                                                    Capacity=Capacity_,
                                                    CruisingSpeedKPH=CruisingSpeedKPH_,
                                                    WeightKG=WeightKG_,
                                                    CompanyID=CompanyID_)
                    conn.execute(insert_query)
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
        Column('Longitude', Float)
    )
    # Define the 'company' table
    company_table = Table(
        'Company', metadata,
        Column('CompanyID', Integer, primary_key=True, autoincrement=True),
        Column('Name', String(255)),
        Column('Country', String(255)),
        Column('IATACode', String(3))
    )
    
    plane_table = Table(
        'Plane', metadata,
        Column('PlaneID', Integer, primary_key=True, autoincrement=True),
        Column('Model', String(255)),
        Column('Manufacturer', String(255)),
        Column('RangeKM', Integer),
        Column('Capacity', Integer),
        Column('CruisingSpeedKPH', Integer),
        Column('WeightKG', Integer),
        Column('CompanyID', Integer)
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
        
    # Insert airport data in Airport Table
    insert_airport_data(engine, airport_table, airport_coordinates)
    # Insert company data in Company Table
    insert_plane_company_data(engine, company_table, airline_companies)
    # Insert plane data in Plane Table
    insert_plane_data(engine, company_table, plane_table, plane_data, airline_plane_data)

if __name__ == "__main__":
    main()
