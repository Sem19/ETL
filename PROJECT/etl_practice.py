import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
transformed_data = 'transformed_data.csv'

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


def get_xml_headers(file_to_process):
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    first = root[0]
    headers = [child.tag for child in first]
    return headers

headers = get_xml_headers('source\\used_car_prices1.xml')
print(headers)

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["model","year", "price", "fule" ])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root: 
        model = car.find("car_model").text
        year = car.find("year_of_manufacture").text
        price = car.find("price").text
        fuel = car.find("fuel").text
        dataframe = dataframe.append({"model": model, "year": year, "price": price, "fuel": fuel}, ignore_index=True)
    return dataframe