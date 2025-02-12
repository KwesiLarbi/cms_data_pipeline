import pandas as pd
import numpy
import json
import math

from datetime import datetime

def replace_year_value(s: str)->str:
    """
    In the Medicare Monthly Enrollment data set, if no month is given, 
    then Year is what is used as N/A
    """
    if s == 'Year':
        s = 'N/A'
    return s

def null_check(d):
    """
    In the Medicare Monthly Enrollment, missing data comes in as
    an asterisks(*). changing to NaN, whcih is a special floating-point
    value
    """
    if d == '*':
        d = math.nan
        return d
    else:
        return d

def find_char(file: str, char_to_find: str)->int:
    total = 0

    with open(file, 'r') as csv_file:
        line = 0
        for i in csv_file.readlines():
            line += 1
            char = i.find(char_to_find)
            if char > 0:
                total += 1
    return total

def dict_items_to_int(data: dict)->dict:
    """
    Pandas returns integers of Int64DType. Need to convert to type into for 
    json.dumps to work and avoid  the error: TypeError: Object of type Int64DType 
    is not JSON serializable
    """
    result = {}
    
    # loop through dict vaules
    keys = [keys for keys in data.keys()]
    values = [values for values in data.values()]
    for i in range(len(keys)):
        result.update({keys[i]: values[i]})
        # check dtype using numpy
        # if type(values[i]) == numpy.dtypes.ObjectDType:
        #     result.update({keys[i]: 'STRING'})
        # else:
        #     result.update({keys[i]: 'INTEGER'})


    return result

def data_quality_check():
    todays_date = datetime.now()
    date = todays_date.strftime('%Y_%m_%d')
    file = f'C:\\Users\\kwesi\\data_projects\\cms_data_pipeline\\data\\{date}_monthly_enrollments.csv'
    print(f'[INFO]: 🕗 Initiating data quality check on the {file} \n')

    data_checks = {}

    # find and count total number of * in data set, * counts as missing values
    missing_values = find_char(file, '*')
    data_checks['missing_values'] = missing_values
    # print(missing_values)

    # checking for duplicates
    data = pd.read_csv(file)
    duplicates = data.duplicated().sum()
    data_checks['duplicate_rows'] = int(duplicates)
    # print(duplicates)

    # checking data types, to ensure correct data format
    data_types = data.dtypes.to_dict()
    data_checks['data_types'] = dict_items_to_int(data_types)

    print(f'[SUCCESS]: ✅ Data quality check on {file} is complete.\n')

    # format dict with json.dumps
    # result = json.dumps(data_checks, indent=2)
    result = data_checks
    print(f'DQ Check Results: {result} \n')
