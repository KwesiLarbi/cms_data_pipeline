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
                    'year':                                     str(i['YEAR']),
                    'month':                                    str(i['MONTH']),
                    'state':                                    str(i['BENE_STATE_ABRVTN']),
                    'tot_benes':                                null_check(i['TOT_BENES']),
                    'male_tot_benes':                           null_check(i['MALE_TOT_BENES']),
                    'female_tot_benes':                         null_check(i['FEMALE_TOT_BENES']),
                    'black_tot_benes':                          null_check(i['BLACK_TOT_BENES']),
                    'white_tot_Benes':                          null_check(i['WHITE_TOT_BENES']),
                    'hspnc_tot_benes':                          null_check(i['HSPNC_TOT_BENES']),
                    'prscrptn_drug_tot_benes':                  null_check(i['PRSCRPTN_DRUG_TOT_BENES']),
                    'prscrptn_drug_pdp_benes':                  null_check(i['PRSCRPTN_DRUG_PDP_BENES']),
                    'prscrptn_drug_mapd_benes':                 null_check(i['PRSCRPTN_DRUG_MAPD_BENES']),
                    'prscrptn_drug_deemed_eligible_lis_benes':  null_check(i['PRSCRPTN_DRUG_DEEMED_ELIGIBLE_FULL_LIS_BENES']),
                    'prscrptn_drug_full_lis_benes':             null_check(i['PRSCRPTN_DRUG_FULL_LIS_BENES']),
                    'prscrptn_drug_partial_lis_benes':          null_check(i['PRSCRPTN_DRUG_PARTIAL_LIS_BENES']),
                    'prscrptn_drug_no_lis_benes':               null_check(i['PRSCRPTN_DRUG_NO_LIS_BENES'])
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
