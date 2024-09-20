from pymongo import MongoClient
from datetime import datetime

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["vehicle_data"]
processed_collection = db["processed_data"]
raw_collection = db["raw_positions"]  # Updated to raw_positions

# Function to query MongoDB based on a predefined time period
def query_data_between_period(start_time, end_time):
    time_filter = {
        "time": {
            "$gte": start_time,
            "$lte": end_time
        }
    }
    data = list(processed_collection.find(time_filter))
    return data

# 1. Find the links with the fewest vehicles (vcount)
def find_links_with_fewest_vehicles(data):
    if not data:
        print("No data found for the specified time period.")
        return []
   
    min_vehicle_count = min(x["vcount"] for x in data)
    links_with_fewest_vehicles = {x["link"]: x["vcount"] for x in data if x["vcount"] == min_vehicle_count}

    print(f"Minimum vehicle count: {min_vehicle_count}")
    if links_with_fewest_vehicles:
        print("Links with the fewest vehicles:")
        for link, vcount in links_with_fewest_vehicles.items():
            print(f"{link}")
    else:
        print("No links found with the fewest vehicles.")
   
    return links_with_fewest_vehicles

# 2. Find the links with the highest average speed (vspeed)
def find_links_with_highest_avg_speed(data):
    if not data:
        print("No data found for the specified time period.")
        return []
   
    max_speed = max(x["vspeed"] for x in data)
    links_with_highest_speed = {x["link"]: x["vspeed"] for x in data if x["vspeed"] == max_speed}

    print(f"Maximum average speed: {max_speed}")
    if links_with_highest_speed:
        print("Links with the highest average speed:")
        for link, vspeed in links_with_highest_speed.items():
            print(f"{link}")
    else:
        print("No links found with the highest average speed.")
   
    return links_with_highest_speed

# 3. Find the longest routes (using position as a proxy for route length)
def find_longest_routes(start_time, end_time):
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
   
    pipeline = [
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {
            "_id": "$link",
            "max_position": {"$max": "$position"},
            "min_position": {"$min": "$position"}
        }},
        {"$project": {
            "link": "$_id",
            "distance": {"$subtract": ["$max_position", "$min_position"]}
        }},
        {"$sort": {"distance": -1}}  # Sort by distance
    ]
   
    longest_routes = list(raw_collection.aggregate(pipeline))

    if longest_routes:
        max_distance = longest_routes[0]['distance']  # Maximum distance
        print(f"Maximum distance: {max_distance} meters")
       
        print("Longest routes (based on maximum distance):")
        max_routes = [route for route in longest_routes if route['distance'] == max_distance]
       
        for route in max_routes:
            print(route['link'])  # Print each route link
    else:
        print("No data found for the specified time period.")

# Main function to run the queries
def main():
    while True:
        start_time_str = input("Enter the start time (dd/mm/yyyy HH:MM:SS) or 'exit' to quit: ")
        if start_time_str.lower() == 'exit':
            break
        end_time_str = input("Enter the end time (dd/mm/yyyy HH:MM:SS): ")

        start_time = datetime.strptime(start_time_str, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(end_time_str, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

        data = query_data_between_period(start_time, end_time)

        print("Choose a query to execute:")
        print("1. Find the links with the fewest vehicles.")
        print("2. Find the links with the highest average speed.")
        print("3. Find the longest routes (based on maximum distance).")
        print("Enter your choice (1/2/3) or 'exit' to quit:")

        choice = input("Your choice: ")
        if choice == '1':
            find_links_with_fewest_vehicles(data)
        elif choice == '2':
            find_links_with_highest_avg_speed(data)
        elif choice == '3':
            find_longest_routes(start_time, end_time)
        elif choice.lower() == 'exit':
            break
        else:
            print("Invalid choice. Please select a valid option.")

if __name__ == "__main__":
    main()
