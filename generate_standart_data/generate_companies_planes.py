"""
Author : Laurent Cesaro
Topic : Python file to generate random planes for airline companies
"""
import json
import random

def load_data(file_path):
    """
    Load data from a JSON file.
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def generate_airline_plane_data(airline_data, plane_data):
    """
    Generate data where each airline has between 10 and 50 planes.
    """
    airline_plane_data = {}
    plane_models = list(plane_data.keys())
    num_plane_models = len(plane_models)
    
    for airline, details in airline_data.items():
        # Randomly select a number of planes between 10 and 50
        num_planes = random.randint(10, 50)
        
        # Ensure num_planes does not exceed the number of available plane models
        if num_planes > num_plane_models:
            num_planes = num_plane_models
        
        # Randomly select plane models from the available plane data
        selected_planes = random.sample(plane_models, num_planes)
        
        airline_plane_data[airline] = {
            "country": details["country"],
            "iata_code": details["iata_code"],
            "planes": selected_planes
        }
    
    return airline_plane_data

# Load data
airline_data = load_data('/home/laurent/docker/airflight_project/cronjob/airline_companies.json')
plane_data = load_data('/home/laurent/docker/airflight_project/cronjob/plane_data.json')

# Generate the airline-plane data
airline_plane_data = generate_airline_plane_data(airline_data, plane_data)

# Save the result to a JSON file
with open('/home/laurent/docker/airflight_project/cronjob/airline_plane_data.json', 'w') as f:
    json.dump(airline_plane_data, f, indent=4)

print("Generated airline-plane data saved to 'airline_plane_data.json'.")
