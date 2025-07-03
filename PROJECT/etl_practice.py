import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
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
    dataframe = pd.DataFrame(columns=["car_model","year_of_manufacture", "price", "fuel" ])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root: 
        model = car.find("car_model").text
        year = car.find("year_of_manufacture").text
        price = car.find("price").text
        fuel = car.find("fuel").text
        record = {
            "car_model": model,
            "year_of_manufacture": year,
            "price": price,
            "fuel": fuel
        }
        dataframe = pd.concat([dataframe, pd.DataFrame([record])], ignore_index=True)
    return dataframe

def extract():
    dataframe = pd.DataFrame(columns=["car_model","year_of_manufacture", "price", "fuel" ])

    #process all csv files, except the target file
    for csvfile in glob.glob("source\\*.csv"):
        if csvfile != target_file:
            dataframe = pd.concat([dataframe, extract_from_csv(csvfile)], ignore_index=True)

    #process all json files, except the target file
    for jsonfile in glob.glob("source\\*.json"):
        if jsonfile != target_file:
            dataframe = pd.concat([dataframe, extract_from_json(jsonfile)], ignore_index=True)

    #process all xml files, except the target file
    for xmlfile in glob.glob("source\\*.xml"):
        dataframe = pd.concat([dataframe, extract_from_xml(xmlfile)], ignore_index=True)
    return dataframe

def transform(data): 
    data["price"] = pd.to_numeric(data["price"], errors='coerce').round(2)
    return data
    
def load_data(target_file, data):
    data.to_csv(target_file, index=False)

def log_progress(message):
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message}\n")

if __name__ == "__main__":
    log_progress("ETL process started.")

    log_progress("Extraction phase started.")
    extracted_data = extract()
    log_progress(f"Extraction phase completed. Extracted {len(extracted_data)} records.")

    log_progress("Transformation phase started.")
    transformed_data = transform(extracted_data)
    
    log_progress("Transformation phase completed.")

    log_progress("Loading phase started.")
    load_data(target_file, transformed_data)
    log_progress(f"Loading phase completed. Data saved to {target_file}.")

    log_progress("ETL process completed.")
