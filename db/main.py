import openrouteservice
import msgpack
import json
from itertools import combinations, islice
from multiprocessing import Pool
from pandas import read_csv
import redis
from openrouteservice import convert

def get_location_key(index):
    return f'{index}:location'

def get_route_key(point_one, point_two):
    return f'{point_one}:route:{point_two}'

CLIENT = openrouteservice.Client(base_url="http://172.21.1.3:8080/ors")

def get_mexico_super_chargers() -> list:
    df = read_csv("data/SuperchargeLocations.csv")
    filtered_df = df[df["Country"] == "Mexico"]
    filtered_df["GPS"] = filtered_df["GPS"].apply(
        lambda x: tuple(map(float, x.split(",")))
    )
    filtered_df["IsSuperCharger"] = True
    return filtered_df[["City", "GPS", "IsSuperCharger"]].to_dict(orient="records")


def get_distance(to_coords, from_coords):
    routes = CLIENT.directions((to_coords, from_coords))
    return routes


def get_all_mexican_cities():
    with open("data/all_cities.json", "r") as file:
        data = json.load(file)
        return [
            {
                "City": item["name"],
                "GPS": (item["coordinates"]["lat"], item["coordinates"]["lon"]),
                "IsSuperCharger": False,
            }
            for item in data
        ]

def insert_distance_data(key, distance):
    redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)
    redis_client.set(key, distance)

def transform_response(raw_response):
    try:
        route = raw_response['routes'][0]
        total_distance = route['summary']['distance']
        # total_steps = sum(len(segment['steps']) for segment in route['segments'])
        return total_distance
        # points = convert.decode_polyline(route['geometry'])

        # return {
            # 'distance': total_distance,
            # 'steps': total_steps,
            # 'points': points
        # }
    except:
        print('SHIT THE BED')
        return 1

def process_combinations_chunk(combos):
    for (point_one_index, point_one_location), (point_two_index, point_two_location) in combos:
        lat1, long1 = point_one_location["GPS"]
        lat2, long2 = point_two_location["GPS"]
        distance = transform_response(get_distance((long1, lat1), (long2, lat2)))
        insert_distance_data(get_route_key(point_one_index, point_two_index), distance)


if __name__ == "__main__":
    super_chargers = get_mexico_super_chargers()
    cities = get_all_mexican_cities()
    relevant_points = list(enumerate(super_chargers + cities))

    redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)
    redis_client.flushall()

    print('')
    for index, data in enumerate(relevant_points):
        packed_data = msgpack.packb(data)
        redis_client.set(get_location_key(index), packed_data)

    combo_iterator = combinations(relevant_points, 2)

    num_processes = 12
    pool = Pool(num_processes)

    chunk_size = 1000

    while True:
        chunk = list(islice(combo_iterator, chunk_size))
        if not chunk:
            break
        pool.apply_async(process_combinations_chunk, args=[chunk])

    pool.close()
    pool.join()
