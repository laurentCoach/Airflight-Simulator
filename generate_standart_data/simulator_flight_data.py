"""
Author : Laurent Cesaro
Topic : Python file to insert data in the database
 - Airline companies data
 - Planes data
 - Airports data
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

def generate_random_code():
    # Generate two random parts, each with 6 characters (uppercase letters and digits)
    part1 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    part2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    
    # Combine the two parts with a hyphen
    return f"{part1}-{part2}"

def get_airport_info(airport_code):
    """
    Get latitude, longitude, and country name from the IATA code using airportsdata and pycountry.
    """
    airport_info = airports.get(airport_code)
    
    if not airport_info:
        raise ValueError(f"Airport '{airport_code}' not found in the database.")
    
    # Accessing latitude, longitude, and country from dictionary keys
    lat = airport_info['lat']
    lon = airport_info['lon']
    country_code = airport_info['country']
    
    # Convert country code to full country name using pycountry
    country_name = pycountry.countries.get(alpha_2=country_code).name
    
    return country_name

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points 
    on the Earth's surface given their latitude and longitude using the Haversine formula.
    """
    R = 6371.0  # Radius of the Earth in kilometers

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distance in kilometers
    distance = R * c

    return distance

def calculate_distance_between_airports(airport_dept, airport_arr):
    """
    Calculate the distance between two airports given their IATA codes.
    """
    # Get coordinates of both airports
    lat_airport_dept = airport_dept[2]
    lon_airport_dept = airport_dept[3]
    lat_airport_arr = airport_arr[2]
    lon_airport_arr = airport_arr[3]
    
    # Calculate the distance using the Haversine formula
    distance = int(haversine(lat_airport_dept, lon_airport_dept, lat_airport_arr, lon_airport_arr))
    
    return distance

def select_plane_with_sufficient_range(conn, plane_data, distance):
    """
    Select a plane from the database that has a range greater than or equal to the given distance.
    """
    # Query to select all planes with sufficient range
    query = select(plane_table).where(plane_table.c.RangeKM >= distance)

    # Execute the query
    with conn.connect() as connection:
        result = connection.execute(query)
        suitable_planes = result.fetchall()

        if not suitable_planes:
            raise ValueError("No plane with sufficient range found.")

        # Select a random plane from the suitable planes
        selected_plane = random.choice(suitable_planes)
        return selected_plane

def calculate_flight_time(distance, plane_code):
    """
    Calculate the flight time based on the distance and plane's cruising speed.
    """
    speed = plane_code[5]

    if not speed:
        raise ValueError(f"Speed for plane '{plane_code[1]}' not available.")
    
    # Calculate the flight time in hours
    alea_time = 30 #Time for landing and takeoff
    flight_time = distance / speed
    flight_time_in_minutes = int(flight_time * 60)
    return flight_time_in_minutes

def get_current_time():
    """
    Get the current time in the format yyyy/mm/dd hh:mm:ss.
    """
    return datetime.now().strftime("%Y/%m/%d %H:%M:%S")

def calculate_arrival_time(departure_time_str, flight_time):
    """
    Calculate the arrival time based on departure time and flight time.
    """
    departure_time = datetime.strptime(departure_time_str, "%Y/%m/%d %H:%M:%S")
    arrival_time = departure_time + timedelta(minutes=flight_time)
    return arrival_time.strftime("%Y/%m/%d %H:%M:%S")

def get_passenger_number(plane):
    """
    Simulate the number of passengers on a flight based on predefined probabilities.
    - 75% of the time the plane is full.
    - 15% of the time the plane is 80-90% full.
    - 5% of the time the plane is 40-50% full.
    - 5% of the time the plane is 10-30% full.
    """
    max_passenger = plane[4]

    # Define the scenarios and their corresponding probabilities
    scenarios = ["full", "almost_full", "half_full", "low_full"]
    probabilities = [0.75, 0.15, 0.05, 0.05]

    # Select a scenario based on the defined probabilities
    scenario = random.choices(scenarios, probabilities)[0]

    if scenario == "full":
        nb_passenger = max_passenger
    elif scenario == "almost_full":
        nb_passenger = random.randint(int(0.80 * max_passenger), int(0.90 * max_passenger))
    elif scenario == "half_full":
        nb_passenger = random.randint(int(0.40 * max_passenger), int(0.50 * max_passenger))
    elif scenario == "low_full":
        nb_passenger = random.randint(int(0.10 * max_passenger), int(0.30 * max_passenger))
    # nb_passenger can't be less than 3
    if nb_passenger < 3:
        nb_passenger = 3
    return nb_passenger

def select_two_random_airports(conn):
    # Raw SQL query to select two random airports
    random_airports_query = text("SELECT * FROM Airport ORDER BY RAND() LIMIT 2")
    
    # Execute the query
    with conn.connect() as connection:
        result = connection.execute(random_airports_query)
        airports = result.fetchall()  # Fetch the results
        airp_dept = airports[0]
        airp_arr = airports[1]
        return airp_dept, airp_arr
 
def generate_phone_number():
    """Generate a random 10-digit phone number in the format like 0205040659."""
    return ''.join([str(random.randint(0, 9)) for _ in range(10)])

def generate_family_members(family_size, surname):
    """Generate family members with the same surname, different first names, gender, and phone numbers."""
    family_members = []
    for _ in range(family_size):
        gender = random.choice(["male", "female"])
        if gender == "male":
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()

        # Generate a fake 10-digit phone number
        phone_number = generate_phone_number()
        
        family_members.append({
            "Name": first_name,
            "Surname": surname,
            "Gender": gender,
            "PhoneNumber": phone_number
        })
    return family_members

def generate_passengers_information(num_passengers, flight_id):
    passengers = []
    while len(passengers) < num_passengers:
        # Randomly decide if a family or individual will be added
        if random.random() < 0.3:  # 30% chance to add a family
            family_size = random.randint(2, 5)  # Family size between 2 and 5
            surname = fake.last_name()
            family_members = generate_family_members(family_size, surname)
            
            # Ensure we don't exceed the total number of passengers
            if len(passengers) + len(family_members) <= num_passengers:
                passengers.extend(family_members)
            else:
                gender = random.choice(["male", "female"])
                first_name = fake.first_name_male() if gender == "male" else fake.first_name_female()
                phone_number = generate_phone_number()
                passengers.append({
                    "Name": first_name,
                    "Surname": fake.last_name(),
                    "Gender": gender,
                    "PhoneNumber": phone_number
                })
        else:
            # Add a single random passenger
            gender = random.choice(["male", "female"])
            first_name = fake.first_name_male() if gender == "male" else fake.first_name_female()
            phone_number = generate_phone_number()
            passengers.append({
                "Name": first_name,
                "Surname": fake.last_name(),
                "Gender": gender,
                "PhoneNumber": phone_number
            })
    
    # Convert the list of dictionaries to a pandas DataFrame
    passengers_df = pd.DataFrame(passengers)
    
    # Add the 'Mail' column
    passengers_df['Mail'] = passengers_df['Name'].str.lower() + '.' + passengers_df['Surname'].str.lower() + '@mail.com'
    passengers_df['FlightID'] = flight_id
    
    # Reorder the columns to match the specified order
    passengers_df = passengers_df[['Name', 'Surname', 'PhoneNumber', 'Mail', 'Gender', 'FlightID']]
    
    return passengers_df

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
    
    try:
        for i in range(number_of_flights):
            # Get the flight code
            FlightCode_ = generate_random_code()
            print(f"The flight code is {FlightCode_}.")
            
            # Select two random airports from database
            airport_departure, airport_arrival = select_two_random_airports(engine)
            print(f"The airport departure is {airport_departure[1]} and airport arrival is {airport_arrival[1]}.")

            # Calculate the distance between the two randomly selected airports
            distance = calculate_distance_between_airports(airport_departure, airport_arrival)
            print(f"The distance between {airport_departure[1]} and {airport_arrival[1]} is {distance} kilometers.")
            
            # Get the departure country and the arrival country
            country_departure = get_airport_info(airport_departure[1])
            country_arrival = get_airport_info(airport_arrival[1])
            print(f"Country Departure is {country_departure} and country arrival is {country_arrival}.")
            
            # Select a plane with sufficient range
            plane_code = select_plane_with_sufficient_range(engine, plane_table, distance)
            print(f"The selected plane is {plane_code[1]} with a cruising speed of {plane_code[5]} km/h.")
            
            flight_time = calculate_flight_time(distance, plane_code)
            print(f"Estimated flight time for the distance {distance} km is {flight_time} minutes.")

            # Get passenger number
            passenger_number = get_passenger_number(plane_code)
            print(f"Number of passengers in the fligt is {passenger_number}. The plane capacity is {plane_code[4]}")
            
            # Get the current departure time
            departure_time = get_current_time()
            print(f"Departure time: {departure_time}")
            
            # Calculate the arrival time
            arrival_time = calculate_arrival_time(departure_time, flight_time)
            print(f"Estimated arrival time: {arrival_time}")

            
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
                    
            
                    # Get the passenger information
                    fake = Faker()
                    passenger_df = generate_passengers_information(passenger_number, new_flight_id)
                    
                    data = passenger_df.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
                    conn.execute(passenger_table.insert(), data)
                    conn.commit()
                    conn.close()
                    print(f"Data properly inserted!")
    except ValueError as e:
        print(f'Error in the simulator for flight {FlightCode_}: {e}')
