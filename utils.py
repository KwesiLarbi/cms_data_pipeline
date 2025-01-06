import requests
import io
import pandas as pd
from datetime import datetime

def process_monthly_enrollment_data(url):
    try:
        response = requests.get(url)
        status = response.raise_for_status()

        if response.status_code == 200 and status == None:
            # parse the response content as JSON
            data = response.json()

            # extract enrollement features
            features = []
            for key in data[0].keys():
                features.append(key)
            
            print('current features...\n')

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
                    'year': i['YEAR'],
                    'month': i['MONTH'],
                    'state': i['BENE_STATE_ABRVTN'],
                    'total_beneficiaries': i['TOT_BENES'],
                    'male_total_beneficiaries': i['MALE_TOT_BENES'],
                    'female_total_beneficiaries': i['FEMALE_TOT_BENES'],
                    'black_total_beneficiaries': i['BLACK_TOT_BENES'],
                    'white_total_beneficiaries': i['WHITE_TOT_BENES'],
                    'hspnc_total_beneficiaries': i['HSPNC_TOT_BENES']
                }
                monthly_enrollments.append(monthly_enrollment)
            
            # convert the list of dictionaries to a DataFrame
            df = pd.DataFrame(monthly_enrollments)

            # save to csv in specific location
            df.to_csv(file, index=False)

        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        raise Exception(f'Failed to retrieve data from {url}. Error: {e}')