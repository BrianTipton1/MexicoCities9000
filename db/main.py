import itertools
from multiprocessing import Pool
import multiprocessing
import aiohttp
import msgpack
import redis as redis
import json
import asyncio
from pandas import read_csv

ORS_BASE_URL = "http://172.21.1.3:8080/ors/v2/matrix/driving-car"


def get_mexico_super_chargers() -> list:
    df = read_csv("data/SuperchargeLocations.csv")
    filtered_df = df[df["Country"] == "Mexico"]
    filtered_df["GPS"] = filtered_df["GPS"].apply(
        lambda x: tuple(map(float, x.split(",")))
    )
    filtered_df["IsSuperCharger"] = True
    return filtered_df[["City", "GPS", "IsSuperCharger"]].to_dict(orient="records")


def transform_response(raw_response):
    try:
        distances = raw_response["distances"]
        durations = raw_response["durations"]
        return (distances, durations)
    except KeyError:
        print("Error in the response data")
        return False


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


def get_payload(locations, srcs, dests):
    return {
        "locations": locations,
        "metrics": ["distance", 'duration'],
        "resolve_locations": "false",
        "units": "km",
        "sources": srcs,
        "destinations": dests,
    }


def get_headers():
    return {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8",
    }


def find_value(matrix, target):
    target_whole = int(target)
    for row_index, row in enumerate(matrix):
        for col_index, value in enumerate(row):
            if int(value) == target_whole:
                return row_index, col_index
    return None


def mutate_tuple(key, data):
    return (key, data["GPS"])

def get_distinct_srcs_dsts_from_chunk(chunk):
    return map(
        list,
        map(
            set,
            zip(
                *map(
                    lambda src_dst: (
                        mutate_tuple(*src_dst[0]),
                        mutate_tuple(*src_dst[1]),
                    ),
                    chunk,
                )
            ),
        ),
    )


def create_redis_key_matrix_map(lst, src_indices, dst_indices):
    return {
        f"{lst[i]}:route:{lst[j]}": (src_indices.index(i), dst_indices.index(j))
        for i in src_indices
        for j in dst_indices
    }


async def make_req(pool, session, pairs_chunk):
    sources, destinations = get_distinct_srcs_dsts_from_chunk(pairs_chunk)
    locations = sources + destinations
    src_indices = list(range(0, len(sources)))
    dest_indices = list(range(len(sources), len(destinations)))

    mapping = create_redis_key_matrix_map(
        list(zip(*locations))[0], src_indices, dest_indices
    )
    payload = get_payload(list(zip(*locations))[1], src_indices, dest_indices)
    headers = get_headers()

    timeout = aiohttp.ClientTimeout(total=7200)
    async with session.post(
        ORS_BASE_URL, json=payload, headers=headers, timeout=timeout
    ) as response:
        if response.status == 200:
            response_data = await response.json()
            distances_matrix, durations_matrix = transform_response(response_data)
            if distances_matrix:
                args = (mapping, distances_matrix, durations_matrix)
                pool.apply_async(update_redis_process, (args,))
        else:
            response_text = await response.text()
            print(
                f"Failed to get a successful response: {response.status} - {response_text}"
            )


def update_redis_process(args):
    redis_client = redis.StrictRedis(
        host="127.0.0.1", port=6379, db=0, decode_responses=True
    )
    mapping, distances_matrix, durations_matrix = args
    redis_pipeline = redis_client.pipeline()
    for key, (matrix_row, matrix_column) in mapping.items():
        maybe_distance = distances_matrix[matrix_row][matrix_column]
        maybe_duration = durations_matrix[matrix_row][matrix_column]

        data = '' if maybe_distance is None or maybe_distance is None else (maybe_distance, maybe_duration)

        redis_pipeline.set(key, msgpack.packb(data))

    redis_pipeline.execute()


def init_redis(relevant_points):
    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
    redis_client.flushall()

    for index, data in enumerate(relevant_points):
        packed_data = msgpack.packb(data)
        redis_client.set(f"{index}", packed_data)
        redis_client.set(f"{index}:route:{index}", msgpack.packb((0, 0)))


def get_relevant_points(all_points):
    return [
        (
            index,
            {
                "City": point["City"],
                "GPS": (point["GPS"][1], point["GPS"][0]),
                "IsSuperCharger": point["IsSuperCharger"],
            },
        )
        for index, point in enumerate(all_points)
    ]


async def main():
    super_chargers = get_mexico_super_chargers()
    cities = get_all_mexican_cities()
    all_points = super_chargers + cities
    relevant_points = get_relevant_points(all_points=all_points)

    init_redis(relevant_points=relevant_points)

    all_permutations = itertools.permutations(relevant_points, 2)
    chunk_size = 1000
    tasks = []
    with Pool(processes=multiprocessing.cpu_count()) as pool:
        async with aiohttp.ClientSession() as session:
            while True:
                pairs_chunk = list(itertools.islice(all_permutations, chunk_size))
                if not pairs_chunk:
                    break
                task = make_req(pool, session, pairs_chunk)
                tasks.append(task)

                if len(tasks) >= 10:
                    await asyncio.gather(*tasks)
                    tasks = []

        if tasks:
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
