import pymongo
import requests
import json


def filter_json_data(json_data):
    '''
    filters out only the needed fields
    '''

    filtered_json_data = []

    for gateway in json_data:
        try:

            gateway = dict(gateway[1])
            filtered_gateway = {}
            filtered_gateway['id'] = gateway['id']
            filtered_gateway['name'] = gateway['name']
            filtered_gateway['country_code'] = gateway['country_code']
            filtered_gateway['online'] = gateway['online']
            filtered_gateway['frequency_plan'] = gateway['attributes']['frequency_plan']
            filtered_gateway['last_seen'] = gateway['last_seen']

            if 'latitude' in gateway['location'] and 'longitude' in gateway['location']:
                filtered_gateway['location'] = gateway['location']
            else:
                print('missing info')
                continue

            filtered_json_data.append(filtered_gateway)

        except Exception as err:
            print(err)
            print('Missing', err.args[0])

    return filtered_json_data


def upload_json_data(json_data, mongoclient, collection):
    '''
    uploads filtered json data to mongo
    '''

    mydb = mongoclient["maindb"]
    mycol = mydb[collection]

    ids = []

    for item in json_data:
        item_id = mycol.insert_one(item)
        print(item_id.inserted_id)
        ids.append(item_id.inserted_id)

    return ids


if __name__ == '__main__':

    mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")

    response = requests.get('https://www.thethingsnetwork.org/gateway-data/')

    with open('raw_data.json', 'w') as rawdata_file:
        json.dump(response.json(), rawdata_file)

    with open('raw_data.json', 'r') as rawdata_file:
        raw_data = json.load(rawdata_file)
        raw_items = raw_data.items()

    filtered_items = filter_json_data(raw_items)
    with open('./filtered_data.json', 'w') as filtered__file:
        json.dump(filtered_items, filtered__file)

    try:
        upload_json_data(filtered_items, mongoclient, 'lora_gateways')
    except Exception:
        print('unable to upload to mongodb')
