from pymongo import MongoClient
from bson.json_util import dumps, loads

def min_vehicles(start_time, end_time):
    return [ 
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {"_id": "$link", "totalVehicles": {"$sum": "$vcount"}}},
        {"$sort": {"totalVehicles": 1}},
        {"$limit": 1}
    ]

def max_avg_speed(start_time, end_time):
    return [ 
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {"_id": "$link", "avgSpeed": {"$avg": "$vspeed"}}},
        {"$sort": {"avgSpeed": -1}},
        {"$limit": 1}
    ]

def max_route(start_time, end_time):
    return [ 
        {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
        {"$group": {"_id": "$link", "totalDistance": {"$sum": "$vcount"}}},
        {"$sort": {"totalDistance": -1}},
        {"$limit": 1}
    ]

def main():
    start_time = input('Give Start Time (YYYY-MM-DD HH:MM:SS): ')
    end_time = input('Give End Time (YYYY-MM-DD HH:MM:SS): ')
    query = input('Select query (1 or 2 or 3): ')

    client = MongoClient('localhost', 27017)
    db = client['vehicle_data']
    collection = db['processed_data']

    result = None
    try:
        # Execute the aggregation
        if int(query) == 1:
            result = collection.aggregate(min_vehicles(start_time, end_time))
            result = list(result)
            if result:
                r = result[0]
                print(f"Link with the fewest vehicles: {r['_id']} with {r['totalVehicles']} vehicles.")
            else:
                print("No data found.")
        
        elif int(query) == 2:
            result = collection.aggregate(max_avg_speed(start_time, end_time))
            result = list(result)
            if result:
                r = result[0]
                print(f"Link with the highest average speed: {r['_id']} with an average speed of {r['avgSpeed']}.")
            else:
                print("No data found.")
        
        elif int(query) == 3:
            result = collection.aggregate(max_route(start_time, end_time))
            result = list(result)
            if result:
                r = result[0]
                print(f"Longest route: {r['_id']} with a total distance of {r['totalDistance']}.")
            else:
                print("No data found.")
        else:
            print("Invalid choice. Please select a valid option.")
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    main()
