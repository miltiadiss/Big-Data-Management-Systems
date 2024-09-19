from pymongo import MongoClient
from datetime import datetime

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["vehicle_data"]
processed_collection = db["processed_data"]

# Function to query MongoDB based on a predefined time period
def query_data_between_period(start_time, end_time):
    # Create a filter for the time range
    time_filter = {
        "time": {
            "$gte": start_time,
            "$lte": end_time
        }
    }
    # Fetch data from MongoDB based on time filter
    data = list(processed_collection.find(time_filter))
    return data

# 1. Find the link with the fewest vehicles (vcount)
def find_link_with_fewest_vehicles(data):
    if not data:
        return None, 0
    min_vehicle_link = min(data, key=lambda x: x["vcount"])
    return min_vehicle_link["link"], min_vehicle_link["vcount"]

# 2. Find the link with the highest average speed (vspeed)
def find_link_with_highest_avg_speed(data):
    if not data:
        return None, 0
    max_speed_link = max(data, key=lambda x: x["vspeed"])
    return max_speed_link["link"], max_speed_link["vspeed"]

# 3. Find the longest route (using vcount as a proxy for route length)
def find_longest_route(data):
    if not data:
        return None, 0
    longest_route = max(data, key=lambda x: x["vcount"])
    return longest_route["link"], longest_route["vcount"]

# Main function to run the queries
def main():
    # Input the time range for querying
    start_time_str = input("Enter the start time (dd/mm/yyyy HH:MM:SS): ")
    end_time_str = input("Enter the end time (dd/mm/yyyy HH:MM:SS): ")

    # Convert the input strings to datetime objects
    start_time = datetime.strptime(start_time_str, "%d/%m/%Y %H:%M:%S")
    end_time = datetime.strptime(end_time_str, "%d/%m/%Y %H:%M:%S")

    # Query data from MongoDB for the specified time period
    data = query_data_between_period(start_time, end_time)

    # Ask user which query they want to execute
    print("Choose a query to execute:")
    print("1. Find the link with the fewest vehicles.")
    print("2. Find the link with the highest average speed.")
    print("3. Find the longest route (based on vehicle count).")

    choice = input("Enter your choice (1/2/3): ")

    if choice == '1':
        # Answer question 1: Link with the fewest vehicles
        link_fewest_vehicles, vcount = find_link_with_fewest_vehicles(data)
        print(f"Link with the fewest vehicles: {link_fewest_vehicles} with {vcount} vehicles.")
    
    elif choice == '2':
        # Answer question 2: Link with the highest average speed
        link_highest_speed, avg_speed = find_link_with_highest_avg_speed(data)
        print(f"Link with the highest average speed: {link_highest_speed} with an average speed of {avg_speed}.")
    
    elif choice == '3':
        # Answer question 3: Longest route (based on vehicle count)
        link_longest_route, longest_route = find_longest_route(data)
        print(f"Longest route (based on vehicle count): {link_longest_route} with {longest_route} vehicles.")
    
    else:
        print("Invalid choice. Please select a valid option.")

if __name__ == "__main__":
    main()
