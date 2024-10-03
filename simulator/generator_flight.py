"""
Author : Laurent Cesaro
Topic : Python file to create flight in DB
"""

from datetime import datetime, timedelta
import airportsdata
from sqlalchemy import create_engine, DateTime, text, insert, Table, Column, Float, String, Integer, DateTime, Boolean, MetaData, select
import sys
import argparse
from faker import Faker # Generate fake Name/Surname/Phone/Gender
import pandas as pd
from functions import * # Import functions from functions.py


def insert_new_flight(conn, flight_table, airport_departure, airport_arrival, flight_distance_km):
    with conn.connect() as conn:

        query = insert(flight_table).values(FlightStatus=False,
                                                AirportDeparture=airport_departure,
                                                AirportArrival=airport_arrival,
                                                Distance=flight_distance_km
                                                )
        conn.execute(query)
        conn.commit()



# Main execution
if __name__ == "__main__":
    
    # Connect to the database
    engine = connect_db()
    
    # Define metadata
    metadata = MetaData()
    # Define the 'Flight' table
    flight_table = Table(
        'Flight', metadata,
        Column('FlightID', Integer, primary_key=True, autoincrement=True),
        Column('FlightCode', String(13)),
        Column('FlightStatus', Boolean),
        Column('AirportDeparture', Integer),
        Column('AirportArrival', Integer),
        Column('TimeDeparture', DateTime),
        Column('TimeArrival', DateTime),
        Column('Distance', Integer),
        Column('FlightTimeMinutes', Integer),
        Column('NbPassenger', Integer),
        Column('PlaneID', Integer)
    )
    
    # Select two random airports from database
    airport_departure, airport_arrival = select_two_random_airports(engine)
    #print(f"The airport departure is {airport_departure[1]} and airport arrival is {airport_arrival[1]}.")

    # Calculate the distance between the two randomly selected airports
    flight_distance_km = calculate_distance_between_airports(airport_departure, airport_arrival)
    #print(f"The distance between {airport_departure[1]} and {airport_arrival[1]} is {flight_distance_km} kilometers.")
    
    insert_new_flight(engine, flight_table, airport_departure[0], airport_arrival[0], flight_distance_km)

    print('Data inserted !')
    """
    FlightID INT AUTO_INCREMENT PRIMARY KEY,  -- Auto-increment primary key
    FlightCode VARCHAR(13) NOT NULL,
    FlightStatus BOOLEAN NOT NULL,  -- BOOLEAN YES->Done NO->ToDo
    AirportDeparture INT NOT NULL,  -- Foreign key referencing AirportID in the Airport table
    AirportArrival INT NOT NULL,    -- Foreign key referencing AirportID in the Airport table
    TimeDeparture DATETIME,
    TimeArrival DATETIME,
    Distance INT NOT NULL,
    FlightTimeMinutes INT NOT NULL,
    NbPassenger INT NOT NULL,
    PlaneID INT,
    FOREIGN KEY (PlaneID) REFERENCES Plane(PlaneID) ON DELETE CASCADE,
    FOREIGN KEY (AirportDeparture) REFERENCES Airport(AirportID) ON DELETE CASCADE,
    FOREIGN KEY (AirportArrival) REFERENCES Airport(AirportID) ON DELETE CASCADE
    """