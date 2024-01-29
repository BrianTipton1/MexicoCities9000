import redis
import msgpack
import requests
import json

ORS_BASE_URL = "http://172.21.1.3:8080/ors/v2/matrix/driving-car"

def get_matrix(locations):
    url = ORS_BASE_URL
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
    }
    payload = {
        "locations": locations,
        "metrics": ["distance"],
        "resolve_locations": "false",
        "units": "km",
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.json()

def extract_data(redis_client, key):
    packed_data = redis_client.get(f'{key}')
    if packed_data is not None:
        return msgpack.unpackb(packed_data)
    else:
        return None

def main():
    redis_client = redis.Redis(
        host="127.0.0.1", port=6380, db=0, decode_responses=False
    )

    key1 = '31:location'
    key2 = '5045:location'

    data1 = extract_data(redis_client, key1)
    data2 = extract_data(redis_client, key2)

    # Assuming data format is [index, data_dict]
    start_gps = data1[1]['GPS']
    end_gps = data2[1]['GPS']

    request_url = f"http://172.21.1.3:8080/ors/v2/directions/driving-car?start={start_gps[0]},{start_gps[1]}&end={end_gps[0]},{end_gps[1]}"

    
    # print(request_url)
    print(json.dumps(get_matrix([start_gps, end_gps])))
    print('')

if __name__ == "__main__":
    main()
