"""
Author : Laurent Cesaro
Topic : Python file to insert data in the database
"""

import math
import json
import random, string
from datetime import datetime, timedelta
import airportsdata
import pycountry
from sqlalchemy import create_engine, DateTime, text, insert, Table, Column, Float, String, Integer, DateTime, Boolean, MetaData, select
import sys
import argparse
from faker import Faker # Generate fake Name/Surname/Phone/Gender
import pandas as pd
from functions import *



# Main execution
if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Flight simulation and database insertion.')
    
    # Adding arguments
    parser.add_argument('--db', type=str, required=True, choices=['yes', 'no'], help='Whether to insert the simulated flights into the database')
    parser.add_argument('--nb_f', type=int, required=True, help='The number of flights to simulate')

    # Parse the arguments
    args = parser.parse_args()

    # Use the provided arguments
    insert_in_db = args.db
    number_of_flights = args.nb_f
    
    # Connect to the database
    # Database connection configuration
    DATABASE_TYPE = 'mysql'
    DBAPI = 'mysql+mysqlconnector'  # Adjust based on your SQLAlchemy version
    ENDPOINT = 'localhost'
    PORT = 3306
    USER = 'laurent'  # Replace with your actual MySQL username
    PASSWORD = '123456789'  # Replace with your actual MySQL password
    DATABASE = 'AIRFLIGHT_DB' # Replace with your actual MySQL DATABASE NAME

    # Construct the SQLAlchemy URL
    db_url = f"{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

    print("Connecting to database with URL:", db_url)

    # Create database connection
    try:
        engine = create_engine(db_url)
        print("Database connection successful")
    except Exception as e:
        print("Error connecting to database:", str(e))
    # Define metadata
    metadata = MetaData()

    # Define the 'Airport' table
    airport_table = Table(
        'Airport', metadata,
        Column('AirportID', Integer, primary_key=True, autoincrement=True),
        Column('AirportCode', String(3)),
        Column('Latitude', Float),
        Column('Longitude', Float)
    )
    # Define the 'Company' table
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
        Column('Capacity', Integer),
        Column('CruisingSpeedKPH', Integer),
        Column('CompanyID', Integer)
    )
    # Define the 'Flight' table
    flight_table = Table(
        'Flight', metadata,
        Column('FlightID', Integer, primary_key=True, autoincrement=True),
        Column('FlightCode', String(13)),
        Column('AirportDeparture', Integer),
        Column('AirportArrival', Integer),
        Column('CountryDeparture', String(255)),
        Column('CountryArrival', String(255)),
        Column('TimeDeparture', DateTime),
        Column('TimeArrival', DateTime),
        Column('Distance', Integer),
        Column('FlightTimeMinutes', Integer),
        Column('NbPassenger', Integer),
        Column('PlaneID', Integer)
    )
    # Define the 'Passenger' table
    passenger_table = Table(
        'Passenger', metadata,
        Column('PassengerID', Integer, primary_key=True, autoincrement=True),
        Column('Name', String(255), nullable=False),
        Column('Surname', String(255), nullable=False),
        Column('PhoneNumber', String(10)),
        Column('Mail', String(255), nullable=False),
        Column('Gender', String(255), nullable=False),
        Column('FlightID', Integer)
    )
        
    # Load the airports database
    airports = airportsdata.load("IATA")
    
    for i in range(number_of_flights):
        try:
            # Get the flight code
            FlightCode_ = generate_random_code()
            #print(f"The flight code is {FlightCode_}.")
            
            # Select two random airports from database
            airport_departure, airport_arrival = select_two_random_airports(engine)
            #print(f"The airport departure is {airport_departure[1]} and airport arrival is {airport_arrival[1]}.")

            # Calculate the distance between the two randomly selected airports
            distance = calculate_distance_between_airports(airport_departure, airport_arrival)
            #print(f"The distance between {airport_departure[1]} and {airport_arrival[1]} is {distance} kilometers.")
            
            # Get the departure country and the arrival country
            country_departure = get_airport_info(airport_departure[1], airports)
            country_arrival = get_airport_info(airport_arrival[1], airports)
            #print(f"Country Departure is {country_departure} and country arrival is {country_arrival}.")
            
            # Select a plane with sufficient range
            plane_code = select_plane_with_sufficient_range(engine, plane_table, distance)
            #print(f"The selected plane is {plane_code[1]} with a cruising speed of {plane_code[5]} km/h.")
            
            flight_time = calculate_flight_time(distance, plane_code)
            #print(f"Estimated flight time for the distance {distance} km is {flight_time} minutes.")

            # Get passenger number
            passenger_number = get_passenger_number(plane_code)
            #print(f"Number of passengers in the fligt is {passenger_number}. The plane capacity is {plane_code[4]}")
            
            # Get the passenger information
            fake = Faker()
            passenger_df = generate_passengers_information(fake, passenger_number)
                    
            # Get the current departure time
            departure_time = get_current_time()
            #print(f"Departure time: {departure_time}")
            
            # Calculate the arrival time
            arrival_time = calculate_arrival_time(departure_time, flight_time)
            #print(f"Estimated arrival time: {arrival_time}")

            
            # Insert data in Flight Table
            if insert_in_db == 'yes':
                FlightCode_ = FlightCode_
                AirportDeparture_ = airport_departure[0]
                AirportArrival_ = airport_arrival[0]
                CountryDeparture_ = country_departure
                CountryArrival_ = country_arrival
                TimeDeparture_ = departure_time
                TimeArrival_ = arrival_time
                Distance_ = distance
                FlightTimeMinutes_ = flight_time
                NbPassenger_ = passenger_number
                PlaneID_ = plane_code[0]
                with engine.connect() as conn:
                    insert_query = insert(flight_table).values(FlightCode=FlightCode_, 
                                                                AirportDeparture=AirportDeparture_,
                                                                AirportArrival=AirportArrival_,
                                                                CountryDeparture=CountryDeparture_,
                                                                CountryArrival=CountryArrival_,
                                                                TimeDeparture=TimeDeparture_,
                                                                TimeArrival=TimeArrival_,
                                                                Distance=Distance_,
                                                                FlightTimeMinutes=FlightTimeMinutes_,
                                                                NbPassenger=NbPassenger_,
                                                                PlaneID=PlaneID_
                                                                )
                    result = conn.execute(insert_query)
                    new_flight_id = result.lastrowid
                    conn.commit()                        
                    
                    # Insert Passenger Information
                    passenger_df['FlightID'] = new_flight_id  # Add FlightID in dataframe
                    passenger_df = passenger_df[['Name', 'Surname', 'PhoneNumber', 'Mail', 'Gender', 'FlightID']]  # Reorder the columns to match the specified order
                    data = passenger_df.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
                    conn.execute(passenger_table.insert(), data)
                    conn.commit()
                    conn.close()
                    print(f"Data properly inserted!")
        except ValueError as e:
            print(f'Error in the simulator for flight {FlightCode_}: {e}')
            continue
