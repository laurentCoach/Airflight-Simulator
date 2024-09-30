"""
Author : Laurent Cesaro
Topic : Python file which contains functions used in simulator_flight_data.py
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

def get_airport_info(airport_code, airports):
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

def select_plane_with_sufficient_range(conn, plane_table, distance):
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

def generate_family_members(fake, family_size, surname):
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

def generate_passengers_information(fake, num_passengers):
    passengers = []
    while len(passengers) < num_passengers:
        # Randomly decide if a family or individual will be added
        if random.random() < 0.3:  # 30% chance to add a family
            family_size = random.randint(2, 5)  # Family size between 2 and 5
            surname = fake.last_name()
            family_members = generate_family_members(fake, family_size, surname)
            
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
    
    return passengers_df

# Compute ticket price
def compute_ticket_price(num_passengers, distance_km, departure_time, df, departure_init):
    """
    Compute ticket prices based on aircraft capacity, distance, and class distribution.

    Args:
        num_passengers (int): The capacity of the plane.
        distance_km (float): The distance of the flight in kilometers.
        departure_time Datetime: Date of the departure
        df (dataframe): The list of the passenger randomly generated
    Returns:
        df
    """
    df['Departure_Time'] = departure_time
    df['Departure_Time'] = pd.to_datetime(df['Departure_Time'], format="%Y/%m/%d %H:%M:%S")
    df = df.reset_index()
    
    if num_passengers < 20:
        # For planes with capacity under 20, ticket cost between 10000 and 50000 dollars
        ticket_price = random.randint(10000, 50000)
        #return {"ticket_price": ticket_price}

    elif 21 <= num_passengers <= 99:
        # For planes with num_passengers between 21 and 99, ticket cost between 2000 and 10000 dollars
        ticket_price = random.randint(2000, 10000)
        #return {"ticket_price": ticket_price}

    else:
        # For planes with num_passengers more than 99, we simulate first, second, and third class prices.

        # Define class distribution percentages
        first_class_percentage = 0.05
        second_class_percentage = 0.15
        third_class_percentage = 0.80

        # Calculate the number of passengers in each class
        first_class_seats = int(num_passengers * first_class_percentage)
        second_class_seats = int(num_passengers * second_class_percentage)
        third_class_seats = int(num_passengers * third_class_percentage)

        if distance_km >= 1000:   
            # Define base ticket prices based on distance (longer distance = higher price)
            base_price_per_km = 0.15  # base price in dollars per km
            first_class_price = base_price_per_km * distance_km * 10 # First class: more expensive, e.g., 10 times the base price
            second_class_price = base_price_per_km * distance_km * 5 # Second class: mid-range, e.g., 5 times the base price
            third_class_price = base_price_per_km * distance_km * 2 # Third class: cheapest, e.g., 2 times the base price
        elif distance_km > 1000 and distance_km < 5000:   
            # Define base ticket prices based on distance (longer distance = higher price)
            base_price_per_km = 0.10  # base price in dollars per km            
            first_class_price = base_price_per_km * distance_km * 10 # First class: more expensive, e.g., 10 times the base price
            second_class_price = base_price_per_km * distance_km * 5 # Second class: mid-range, e.g., 5 times the base price
            third_class_price = base_price_per_km * distance_km * 2 # Third class: cheapest, e.g., 2 times the base price
        else:
            # Define base ticket prices based on distance (longer distance = higher price)
            base_price_per_km = 0.05  # base price in dollars per km
            first_class_price = base_price_per_km * distance_km * 10 # First class: more expensive, e.g., 10 times the base price
            second_class_price = base_price_per_km * distance_km * 5 # Second class: mid-range, e.g., 5 times the base price
            third_class_price = base_price_per_km * distance_km * 2 # Third class: cheapest, e.g., 2 times the base price
            
        #####################################################################
        # Calculate initial number of passengers for each class (rounding)
        # If passenger number > 99

        first_class_passengers = round(num_passengers * first_class_percentage)
        second_class_passengers = round(num_passengers * second_class_percentage)
        third_class_passengers = round(num_passengers * third_class_percentage)

        # Ensure the total number of passengers adds up to num_passengers
        total_passengers = first_class_passengers + second_class_passengers + third_class_passengers

        # If there's a discrepancy, adjust the third class passengers (largest group)
        if total_passengers != num_passengers:
            adjustment = num_passengers - total_passengers
            third_class_passengers += adjustment
        percentage_sold = 0  # If more than 99 passengers
        #####################################################################
    df['Price'] = None
    previous_surname = None
    for index, row in df.iterrows():
        current_surname = row['Surname'] # Select surname to apply same price to family members
        departure_date = row['Departure_Time']
        if num_passengers <= 99:
            df = attribute_price_to_people(ticket_price, previous_surname, current_surname, df, index, departure_date)
        else:
            df = attribute_price_to_people(ticket_price, previous_surname, current_surname, df, index, departure_date)
    
        # Update the previous_surname for the next iteration
        previous_surname = current_surname
    # Return a dictionary with ticket prices for each class and the number of seats in each class
    return df

def attribute_price_to_people(ticket_price, previous_surname, current_surname, df, index, departure_date):
    
    if previous_surname is not None:
        if current_surname == previous_surname:
            print(f"Same surname as the last: {current_surname}")
            discounted_price = df.loc[index, 'Price']
            df.at[index, 'Price'] = discounted_price
        else:
            print(f"Different surname. Previous: {previous_surname}, Current: {current_surname}")
            purchase_date = generate_random_purchase_date(departure_date)
            df.at[index, 'purchase_date'] = purchase_date
            discounted_price = compute_discount_price(ticket_price, purchase_date, departure_date)
            df.at[index, 'Price'] = discounted_price
    else:
        print(f"First surname: {current_surname}")
        purchase_date = generate_random_purchase_date(departure_date)
        df.at[index, 'purchase_date'] = purchase_date
        discounted_price = compute_discount_price(ticket_price, purchase_date, departure_date)
        df.at[index, 'Price'] = discounted_price

    return df

# Function to generate a random purchase date between 90 days before departure and the departure date
def generate_random_purchase_date(departure_date):
    # Generate a random number of days before the departure (between 1 and 90 days)
    days_before_departure = random.randint(1, 90)
    # Calculate the purchase date by subtracting the random number of days from the departure date
    purchase_date = departure_date - timedelta(days=days_before_departure)
    return purchase_date

def compute_discount_price(base_price, purchase_date, departure_date):
    # Calculate the number of days between purchase and departure
    days_until_departure = (departure_date - purchase_date).days

    # Apply discounts based on the number of days before departure
    if 90 >= days_until_departure >= 75:
        return base_price * 0.40  # 60% discount
    elif 74 >= days_until_departure >= 60:
        return base_price * 0.50  # 50% discount
    elif 59 >= days_until_departure >= 45:
        return base_price * 0.70  # 30% discount
    else:
        return base_price  # No discount, full price
    
# Function to generate a random purchase date between 90 days before departure and the departure date
def generate_random_purchase_date(departure_date):
    # Generate a random number of days before the departure (between 1 and 90 days)
    days_before_departure = random.randint(1, 90)
    # Calculate the purchase date by subtracting the random number of days from the departure date
    purchase_date = departure_date - timedelta(days=days_before_departure)
    return purchase_date

def compute_dicount_price(base_price, purchase_date, departure_date):
    # Calculate the number of days between purchase and departure
    days_until_departure = (departure_date - purchase_date).days

    # Apply discounts based on the number of days before departure
    if 90 >= days_until_departure >= 75:
        return base_price * 0.40  # 60% discount
    elif 74 >= days_until_departure >= 60:
        return base_price * 0.50  # 50% discount
    elif 59 >= days_until_departure >= 45:
        return base_price * 0.70  # 30% discount
    else:
        return base_price  # No discount, full price

# Compute the consumption of fuel
def compute_fuel_cost(weighKG, gas_price_per_gallon, flight_distance_km, num_people=0, avg_weight_per_person=75, efficiency_constant=15):
    """
    Calculates the total fuel cost for a flight.

    Parameters:
    - weighKG: Weight of the plane in kilograms.
    - gas_price_per_gallon: Price of fuel per gallon.
    - flight_distance_km: The distance of the flight in kilometers.
    - num_people: Number of passengers (default is 0 if not provided).
    - avg_weight_per_person: Average weight of each person in kilograms (default is 75 kg).
    - efficiency_constant: Constant that reflects fuel efficiency of the aircraft (default is 15, a typical value).

    Returns:
    - fuel_cost: The total cost of fuel for the flight.
    - fuel_volume_gallons: The total fuel volume required in gallons.
    """

    # Adjust weight to include passengers (optional)
    total_weight = weighKG + (num_people * avg_weight_per_person)

    # Calculate fuel consumption (kg per km)
    fuel_consumption_per_km = total_weight / efficiency_constant

    # Calculate the total fuel needed for the flight in kg
    total_fuel_kg = fuel_consumption_per_km * flight_distance_km

    # Convert fuel from kg to liters (1 liter = 0.8 kg)
    total_fuel_liters = total_fuel_kg / 0.8

    # Convert liters to gallons (1 gallon = 3.785 liters)
    total_fuel_gallons = total_fuel_liters / 3.785

    # Calculate total fuel cost
    total_fuel_cost = total_fuel_gallons * gas_price_per_gallon

    return total_fuel_cost, total_fuel_gallons

# Example usage
weighKG = 277000  # weight of a Boeing 777-200LR in kg
gas_price_per_gallon = 4.50  # example gas price per gallon
flight_distance_km = 14800  # flight distance (max range of a 777-200LR in km)
num_people = 300  # number of passengers
avg_weight_per_person = 75  # average weight of a person in kg
efficiency_constant = 15  # a typical value for a large commercial plane

fuel_cost, fuel_volume_gallons = compute_fuel_cost(weighKG, gas_price_per_gallon, flight_distance_km, num_people, avg_weight_per_person, efficiency_constant)

print(f"Total fuel cost: ${fuel_cost:.2f}")
print(f"Total fuel volume: {fuel_volume_gallons:.2f} gallons")
