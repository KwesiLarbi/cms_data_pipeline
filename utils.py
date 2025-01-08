import pandas as pd
import json

from datetime import datetime

def find_char(file, char_to_find)->int:
    total = 0

    with open(file, 'r') as csv_file:
        line = 0
        for i in csv_file.readlines():
            line += 1
            char = i.find(char_to_find)
            if char > 0:
                total += 1
    return total

def data_quality_check():
    todays_date = datetime.now()
    date = todays_date.strftime('%Y_%m_%d')
    file = f'C:\\Users\\kwesi\\data_projects\\cms_data_pipeline\\data\\{date}_monthly_enrollments.csv'
    print(f'[INFO]: 🕗 Initiating data quality check on the {file} \n')

    checks = {}

    # find and count total number of * in data set, * counts as missing values
    missing_values = find_char(file, '*')
    checks['missing_values'] = missing_values
    # print(missing_values)

    # checking for duplicates
    data = pd.read_csv(file)
    duplicates = data.duplicated().sum()
    checks['duplicate_rows'] = int(duplicates)
    # print(duplicates)

    print(f'[SUCCESS]: ✅ Data quality check on {file} is complete.\n')

    # format dict with json.dumps
    result = json.dumps(checks, indent=2)
    print(f'DQ Check Results: {result} \n')
