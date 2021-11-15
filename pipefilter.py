import pymongo
import random
import requests
import json
import time

from requests.models import guess_json_utf


def filter_json_data(json_data):
    '''
    filters out only the needed fields
    '''

    filtered_json_data = []

    for gateway in json_data:
        try:
            filtered_gateway = {}
            filtered_gateway['id'] = gateway['id']
            filtered_gateway['name'] = gateway['name']
            filtered_gateway['country_code'] = gateway['country_code']

            if ['latitude', 'longitude'] in gateway['location']:
                filtered_gateway['location'] = gateway['location']
            else:
                continue

            filtered_json_data.append(filtered_gateway)
        except Exception as err:
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
        keys = raw_data.keys()

        out_items = []
        for key in keys:
            
            

    # try:
    #     pass
    #     user_ids = upload_json_data(
    #         filtered_json_data, mongoclient, 'users')
    # except Exception as err:
    #     raise Exception('Unable to upload data to mongo')

    # skopje_center = (41.9946653, 21.4308611)
    # scooters = generate_scooter_data(skopje_center, 200)
    # try:
    #     scooter_ids = upload_json_data(scooters, mongoclient, 'scooters')
    # except Exception as err:
    #     raise Exception('Unable to upload data to mongo')

    # ride_history = generate_riding_history(user_ids, scooter_ids)

    # try:
    #     upload_json_data(ride_history, mongoclient, 'ride_history')
    # except Exception as err:
    #     raise Exception('Unable to upload data to mongo')

