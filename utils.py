import pandas as pd

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

    # find and count total number of * in data set, * counts as missing values
    missing_values = find_char(file, '*')
    print(missing_values)

    # checking for duplicates
    data = pd.read_csv(file)
    duplicates = data.duplicated().sum()
    print(duplicates)
