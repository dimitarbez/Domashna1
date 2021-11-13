import csv
import pymongo


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

        filtered_json_data.append(filtered_person)

    return filtered_json_data


def upload_json_data(json_data, mongoclient):
    '''
    uploads filtered json data to mongo
    '''

    mydb = mongoclient["maindb"]
    mycol = mydb["users"]

    for user in json_data:
        user_id = mycol.insert_one(user)
        print(f'inserted user {user_id}')


if __name__ == '__main__':

    try:

        mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")

        json_data = csv_to_json('./data.csv')
        filtered_json_data = filter_json_data(json_data)
        
        try:
            upload_json_data(filtered_json_data, mongoclient)
        except Exception as err:
            raise Exception('Unable to upload data to mongo')
    except Exception as err:
        print(err)
