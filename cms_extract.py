import requests
import io
import pandas as pd

from datetime import datetime
from utils import null_check

def process_monthly_enrollment_data(url):
    try:
        print(f'[INFO]: 💻 Making request to {url}\n')
        response = requests.get(url)
        status = response.raise_for_status()

        if response.status_code == 200 and status == None:
            # parse the response content as JSON
            data = response.json()

            # extract enrollement features
            print(f'[INFO]: 🔑 Extracting keys from json data to reference as features\n')
            features = []
            for key in data[0].keys():
                features.append(key)
            
            print('[INFO]: 🔑 Current Features:')

            for feature in features:
                print(feature)

            # # create a list of dictionaries containing data
            monthly_enrollments = []

            todays_date = datetime.now()

            date = todays_date.strftime('%Y_%m_%d')

            # file destination
            file = f'C:\\Users\\kwesi\\data_projects\\cms_data_pipeline\\data\\{date}_monthly_enrollments.csv'

            for i in data:
                monthly_enrollment = {
                    'year': str(i['YEAR']),
                    'month': str(i['MONTH']),
                    'state': str(i['BENE_STATE_ABRVTN']),
                    'total_beneficiaries': null_check(i['TOT_BENES']),
                    'male_total_beneficiaries': null_check(i['MALE_TOT_BENES']),
                    'female_total_beneficiaries': null_check(i['FEMALE_TOT_BENES']),
                    'black_total_beneficiaries': null_check(i['BLACK_TOT_BENES']),
                    'white_total_beneficiaries': null_check(i['WHITE_TOT_BENES']),
                    'hspnc_total_beneficiaries': null_check(i['HSPNC_TOT_BENES'])
                }
                monthly_enrollments.append(monthly_enrollment)
            
            # convert the list of dictionaries to a DataFrame
            df = pd.DataFrame(monthly_enrollments)

            # save to csv in specific location
            print(f'\n[INFO]: 📂 Saving data from {url} as CSV in data folder\n')
            df.to_csv(file, index=False)

        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f'Failed to retrieve data from {url}. Error: {e}')
