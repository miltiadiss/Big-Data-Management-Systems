from pymongo import MongoClient
from bson.json_util import dumps, loads

def find_link_with_fewest_vehicles(start_time, end_time):
    return [ 
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {"_id": "$link", "totalVehicles": {"$sum": "$vcount"}}},
        {"$sort": {"totalVehicles": 1}},
        {"$limit": 1}
    ]

def find_link_with_highest_avg_speed(start_time, end_time):
    return [ 
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {"_id": "$link", "avgSpeed": {"$avg": "$vspeed"}}},
        {"$sort": {"avgSpeed": -1}},
        {"$limit": 1}
    ]

def find_longest_route(start_time, end_time):
    return [ 
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {"_id": "$link", "totalDistance": {"$sum": "$vcount"}}},
        {"$sort": {"totalDistance": -1}},
        {"$limit": 1}
    ]

def main():
    # Input the time range for querying
    start_time_str = input("Enter the start time (dd/mm/yyyy HH:MM:SS): ")
    end_time_str = input("Enter the end time (dd/mm/yyyy HH:MM:SS): ")

    # Convert the input strings to datetime objects
    start_time = datetime.strptime(start_time_str, "%d/%m/%Y %H:%M:%S")
    end_time = datetime.strptime(end_time_str, "%d/%m/%Y %H:%M:%S")

    # Ask user which query they want to execute
    print("Choose a query to execute:")
    print("1. Find the link with the fewest vehicles.")
    print("2. Find the link with the highest average speed.")
    print("3. Find the longest route (based on vehicle count).")

    choice = input("Enter your choice (1/2/3): ")

    client = MongoClient('localhost', 27017)
    db = client['vehicle_data']
    collection = db['processed_data']

    result = None
    try:
        # Execute the aggregation
        if int(query) == 1:
            result = collection.aggregate(find_link_with_fewest_vehicles(start_time, end_time))
            result = list(result)
            if result:
                r = result[0]
                print(f"Link with the fewest vehicles is {r['_id']} and has in total {r['totalVehicles']} vehicles.")
            else:
                print("Error.")
        
        elif int(query) == 2:
            result = collection.aggregate(find_link_with_highest_avg_speed(start_time, end_time))
            result = list(result)
            if result:
                r = result[0]
                print(f"Link with highest average speed is {r['_id']} and has average speed  {r['avgSpeed']}.")
            else:
                print("Error.")
        
        elif int(query) == 3:
            result = collection.aggregate(find_longest_route(start_time, end_time))
            result = list(result)
            if result:
                r = result[0]
                print(f"Longest route is {r['_id']} with distance {r['totalDistance']}.")
            else:
                print("Error.")
        else:
            print("Invalid choice. Please select a valid option.")
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    main()
