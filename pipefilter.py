import csv
import pymongo
import random


def csv_to_json(csv_file):
    '''
    converts csv data into json
    '''

    data = []

    with open(csv_file, encoding='utf-8') as input_file:
        csv_reader = csv.DictReader(input_file)

        for row in csv_reader:
            data.append(row)

    return data


def filter_json_data(json_data):
    '''
    filters out only the needed fields
    '''

    filtered_json_data = []

    for person in json_data:
        filtered_person = {}
        filtered_person['First Name'] = person['Last Name']
        filtered_person['Last Name'] = person['Last Name']
        filtered_person['Email'] = person['Email']
        filtered_person['Age'] = person['Age']
        filtered_person['Card Number'] = str(
            int(random.random() * pow(10, 16)))

        filtered_json_data.append(filtered_person)

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


def generate_scooter_data(location, numer_of_scooters):
    lat = location[0]
    lon = location[1]

    scooters = []

    for i in range(numer_of_scooters):
        scooter = {}
        scooter['brand'] = 'Xiaomi'
        scooter['battery_level'] = random.randrange(30, 100)

        offset = random.randrange(0, 100) / float(1000)
        new_lat = (lat - offset / 2) + offset
        new_lon = (lon - offset / 2) + offset

        scooter['location'] = (new_lat, new_lon)
        scooters.append(scooter)

    return scooters


def generate_riding_history(user_ids, scooter_ids):

    ride_history = []

    for i in range(0, len(user_ids), 2):
        ride = {}
        ride['user_id'] = user_ids[i]
        ride['scooter_id'] = scooter_ids[i]
        ride['minutes'] = random.randint(3, 45)
        ride['km'] = round(random.random() * 8, 2)
        ride_history.append(ride)

    return ride_history


if __name__ == '__main__':


    mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")

    json_data = csv_to_json('./data.csv')
    filtered_json_data = filter_json_data(json_data)

    try:
        pass
        user_ids = upload_json_data(
            filtered_json_data, mongoclient, 'users')
    except Exception as err:
        raise Exception('Unable to upload data to mongo')

    skopje_center = (41.9946653, 21.4308611)
    scooters = generate_scooter_data(skopje_center, 200)
    try:
        scooter_ids = upload_json_data(scooters, mongoclient, 'scooters')
    except Exception as err:
        raise Exception('Unable to upload data to mongo')

    ride_history = generate_riding_history(user_ids, scooter_ids)

    try:
        upload_json_data(ride_history, mongoclient, 'ride_history')
    except Exception as err:
        raise Exception('Unable to upload data to mongo')

