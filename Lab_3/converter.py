import json
import os
import csv


def write_to_csv(data, csv_file):
    with open(csv_file, 'w', newline='') as csvf:
        csv_writer = csv.writer(csvf)
        csv_writer.writerow(data.keys())
        csv_writer.writerow(data.values())


def flatten_json(nested_json):
    flattened_json = {}

    def flatten(x, name='', parent_name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_', name)
        elif type(x) is list:
            for i, item in enumerate(x):
                if type(item) is dict:
                    flatten(item, name + f'_{i}', name + f'_{i}')
                else:
                    flattened_json[parent_name + name + f'{i}'] = item
        else:
            flattened_json[parent_name + name[:-1]] = x

    flatten(nested_json)
    return flattened_json


def convert(dir):
    subfolders = []

    for item in os.scandir(dir):
        if item.is_dir():
            subfolders.append(item.path)
        if item.is_file():
            if os.path.splitext(item.name)[1].lower() in [".json"]:
                data = json.load(open(item.path))
                csv_file = item.path.replace('.json', '.csv')
                write_to_csv(flatten_json(data), csv_file)

    for folder in list(subfolders):
        sf = convert(folder)
        subfolders.extend(sf)
    return subfolders
