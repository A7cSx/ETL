import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

# extraction
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['name', 'height', 'weight'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    
    for person in root:
        name = person.find('name').text
        height_element = person.find('height')
        weight_element = person.find('weight')

        # Ensure that height and weight exist before converting to float
        height = float(height_element.text) if height_element is not None else 0.0
        weight = float(weight_element.text) if weight_element is not None else 0.0

        dataframe = pd.concat([dataframe, pd.DataFrame([{'name': name, 'height': height, 'weight': weight}])], ignore_index=True)
        return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])
    
    # process csv
    for csvfile in glob.glob('*.csv'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)

    # process json
    for jsonfile in glob.glob('*.json'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    # process xml
    for xmlfile in glob.glob('*.xml'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True)

    
    return extracted_data


#   Tranforming
def transform(data):
    '''Convert inches to meters and pounds to kilograms, rounding to 2 decimals.'''
    
    # Convert height from inches to meters (1 inch = 0.0254 meters)
    data['height'] = round(data['height'] * 0.0254, 2)

    # Convert weight from pounds to kilograms (1 lb = 0.45359237 kg)
    data['weight'] = round(data['weight'] * 0.45359237, 2)
    
    return data


#   Loading
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # year-monthName-day-hour-minute-second
    now = datetime.now() # current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


#   Testing

# log the initialize of ETL process
log_progress("ETL job started")

# log the begining of the extraction process

log_progress("Extract phase started")
extracted_data = extract()

# log the completion of the extraction process
log_progress("Extract phase ended")

# log the beginning of the Transofmation process
log_progress("Transform phase started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# log the completion of the Transfomation process
log_progress("Transformed phase ended")

# log the beginning of the Loading process
log_progress("Load phase started")

# log the completion of the Loading process
log_progress("Load phases ended")

# log the completion of the ETL
log_progress("ETL Job Ended")
