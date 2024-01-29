import itertools
import aiohttp
import msgpack
import redis.asyncio as redis
import json
import asyncio
from pandas import read_csv
import pandas as pd
import numpy as np

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
        return distances
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


def get_location_key(index):
    return f"{index}:location"


def get_route_key(point_one, point_two):
    return f"{point_one}:route:{point_two}"


def get_payload(locations):
    return {
        "locations": locations,
        "metrics": ["distance"],
        "resolve_locations": "false",
        "units": "km",
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


def create_redis_key_matrix_map(lst):
    return {f'{i}:route:{j}': (lst.index(i), lst.index(j)) for i in lst for j in lst}


async def make_req(redis_client, session, pairs_chunk):
    locations = list(
        set(
            itertools.chain.from_iterable(
                [
                    [(point1[0], point1[1]["GPS"]), (point2[0], point2[1]["GPS"])]
                    for point1, point2 in pairs_chunk
                ]
            )
        )
    )
    
    mapping = create_redis_key_matrix_map(list(zip(*locations))[0])
    payload = get_payload(list(zip(*locations))[1])
    headers = get_headers()

    async with session.post(ORS_BASE_URL, json=payload, headers=headers) as response:
        if response.status == 200:
            response_data = await response.json()
            distances_matrix = transform_response(response_data)
            if distances_matrix:
                redis_pipeline = redis_client.pipeline()

                for key, (matrix_row, matrix_column) in mapping.items():
                    distance = distances_matrix[matrix_row][matrix_column]
                    redis_pipeline.set(
                        key,
                        "NONE" if distance is None else float(distance),
                    )

                await redis_pipeline.execute()
        else:
            response_text = await response.text()
            print(
                f"Failed to get a successful response: {response.status} - {response_text}"
            )


async def main():
    super_chargers = get_mexico_super_chargers()
    cities = get_all_mexican_cities()
    all_points = super_chargers + cities
    relevant_points = [
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

    redis_client = redis.Redis(
        host="172.21.1.9", port=6379, db=0, decode_responses=True
    )

    await redis_client.flushall()

    for index, data in enumerate(relevant_points):
        packed_data = msgpack.packb(data)
        await redis_client.set(get_location_key(index), packed_data)

    all_combinations = itertools.combinations(relevant_points, 2)
    chunk_size = 100

    tasks = []
    async with aiohttp.ClientSession() as session:
        while True:
            pairs_chunk = list(itertools.islice(all_combinations, chunk_size))
            if not pairs_chunk:
                break
            task = make_req(redis_client, session, pairs_chunk)
            tasks.append(task)

            if len(tasks) >= 10:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
