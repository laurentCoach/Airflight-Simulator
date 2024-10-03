"""
Author : Laurent Cesaro
Topic : Python file to insert data in the database
"""

from datetime import datetime, timedelta
import airportsdata
from sqlalchemy import create_engine, DateTime, text, insert, Table, Column, Float, String, Integer, DateTime, Boolean, MetaData, select, update
import sys
import argparse
from faker import Faker # Generate fake Name/Surname/Phone/Gender
import pandas as pd
from functions import * # Import functions from functions.py


# Main execution
if __name__ == "__main__":
    # Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Flight simulation and database insertion.')
    
    # Adding arguments
    parser.add_argument('--db', type=str, required=True, choices=['yes', 'no'], help='Whether to insert the simulated flights into the database')
    parser.add_argument('--nb_f', type=int, required=True, help='The number of flights to simulate')

    # Parse the arguments
    args = parser.parse_args()

    # Use the provided arguments
    insert_in_db = args.db
    number_of_flights = args.nb_f
    """
    # Connect to the database
    engine = connect_db()
    
    # Define metadata
    metadata = MetaData()
    # Define the 'Airport' table
    airport_table = Table(
        'Airport', metadata,
        Column('AirportID', Integer, primary_key=True, autoincrement=True),
        Column('AirportCode', String(3)),
        Column('Latitude', Float),
        Column('Longitude', Float),
        Column('LandingPrice', Float),
        Column('AirportCountry', String(255))
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
        Column('PassengerCapacity', Integer),
        Column('CruisingSpeedKPH', Integer),
        Column('WeightKG', Integer),
        Column('TankCapacityInGallon', Integer),
        Column('CompanyID', Integer)
    )
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
    # Define the 'Passenger' table
    passenger_table = Table(
        'Passenger', metadata,
        Column('PassengerID', Integer, primary_key=True, autoincrement=True),
        Column('Name', String(255), nullable=False),
        Column('Surname', String(255), nullable=False),
        Column('PhoneNumber', String(10)),
        Column('Mail', String(255), nullable=False),
        Column('Gender', String(255), nullable=False),
        Column('TicketPriceDollar', Integer),
        Column('PurchaseDate', DateTime),
        Column('FlightID', Integer)
    )
    # Define the 'Company_Income' table
    company_income_table = Table(
        'Company_Income', metadata,
        Column('IncomeID', Integer, primary_key=True, autoincrement=True),
        Column('Income', Integer),
        Column('TransactionDate', DateTime),
        Column('Topic', String(10)),
        Column('CompanyID', Integer)
    )
    # Define the 'Consumption' table
    consumption_table = Table(
        'Consumption', metadata,
        Column('ConsumptionID', Integer, primary_key=True, autoincrement=True),
        Column('BarrelPriceDollar', Integer),
        Column('TotalFuelPriceDollar', Integer),
        Column('TotalFuelVolumeGallons', Integer),
        Column('FlightID', Integer)
    ) 
    # Define the 'Plane' table
    plane_status_table = Table(
        'Plane_Status', metadata,
        Column('PlaneStatusID', Integer, primary_key=True, autoincrement=True),
        Column('InFlight', Boolean),
        Column('AirportID', Integer),
        Column('PlaneID', Integer)
    )
    
    # Load the airports database
    airports = airportsdata.load("IATA")
    
    try:
        query = select(flight_table).where(flight_table.c.FlightStatus == 0) 
        with engine.connect() as connection:
            result_filght = connection.execute(query)

        
        #for i in range(number_of_flights):
        for flight_ in result_filght:
            flight_ = list(flight_)  # convert tuple to list
            flightID_ = flight_[0]
            flight_distance_km = flight_[7]
            airport_departure = flight_[3]
            
            plane_code = select_plane_with_sufficient_range(engine, plane_table, plane_status_table, airport_departure, flight_distance_km)
            print(plane_code)
            FlightCode_ = generate_random_code(engine, plane_code, company_table)
            #print(f"The selected plane is {plane_code[1]} with a cruising speed of {plane_code[5]} km/h.")
            
            flight_time = calculate_flight_time(flight_distance_km, plane_code)
            #print(f"Estimated flight time for the distance {flight_distance_km} km is {flight_time} minutes.")

            # Get passenger number
            passenger_number = get_passenger_number(plane_code)
            #print(f"Number of passengers in the fligt is {passenger_number}. The plane capacity is {plane_code[4]}")
                    
            # Get the current departure time
            departure_time = get_random_departure_time()
            #print(f"Departure time: {departure_time}")
            
            # Calculate the arrival time
            arrival_time = calculate_arrival_time(departure_time, flight_time)
            #print(f"Estimated arrival time: {arrival_time}")
            
            # Generate/Get the passenger information
            fake = Faker()
            passenger_df = generate_passengers_information(fake, passenger_number)
            #Compute ticket price
            passenger_df = compute_ticket_price(passenger_number, flight_distance_km, departure_time, passenger_df)

            # Compute Plane Consuption
            # Get Current Oil Price
            data = bmdOilPriceFetch.bmdPriceFetch() 
            Oil_Price = data['regularMarketPrice']
            TotalFuelPrice_, TotalFuelVolumeGallons_ = compute_fuel_cost(plane_code[6], get_oil_price(), flight_distance_km, passenger_number, 75, 15)
            
            print('flightID_:', flightID_)
            print('PlaneID_:', plane_code[0])
            print('FlightCode_:', FlightCode_)

            # Update Flight Table
            FlightCode_ = FlightCode_
            TimeDeparture_ = departure_time
            TimeArrival_ = arrival_time
            FlightTimeMinutes_ = flight_time
            NbPassenger_ = passenger_number
            PlaneID_ = plane_code[0]
            with engine.connect() as conn:
                update_flight = (
                            update(flight_table).values(FlightCode=FlightCode_, 
                                                                    TimeDeparture=TimeDeparture_,
                                                                    TimeArrival=TimeArrival_,
                                                                    FlightTimeMinutes=FlightTimeMinutes_,
                                                                    NbPassenger=NbPassenger_,
                                                                    PlaneID=PlaneID_
                                                                    ).where(flight_table.c.FlightID == flightID_)
                    )
                conn.execute(update_flight)
                print('ok')
                update_plane_status = (
                            update(plane_status_table).values(InFlight=True
                                                                    ).where(plane_status_table.c.PlaneID == PlaneID_)
                    )
                conn.execute(update_plane_status)
                print('ok')
                # Insert Passenger Information
                passenger_df['FlightID'] = flightID_  # Add FlightID in dataframe
                passenger_df = passenger_df[['Name', 'Surname', 'PhoneNumber', 'Mail', 'Gender', 'TicketPriceDollar', 'PurchaseDate', 'FlightID']]  # Reorder the columns to match the specified order
                passenger_df['PurchaseDate'] = pd.to_datetime(passenger_df['PurchaseDate'], format="%Y/%m/%d %H:%M:%S")
                data = passenger_df.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
                conn.execute(passenger_table.insert(), data)
                
                # Insert Consumption Information
                insert_query = insert(consumption_table).values(BarrelPriceDollar=get_oil_price(), 
                                                            TotalFuelPriceDollar=TotalFuelPrice_,
                                                            TotalFuelVolumeGallons=TotalFuelVolumeGallons_,
                                                            FlightID=flightID_
                                                            )
                conn.execute(insert_query)
                conn.commit()
                conn.close()
        
    except ValueError as e:
        print(f'Error in the simulator for flight {FlightCode_}: {e}')
        #continue
