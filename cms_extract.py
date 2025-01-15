import requests
import io
import pandas as pd

from datetime import datetime
from utils import null_check

def retrieve_json_data(url)->list:
    """Dealing with a multidiminsional array/list: [[dict1, dict2, dictN], [[dict1, dict2, dictN]]]"""
    try:
        headers = {'content-type': 'application/json'}

        # set to 1,000,000 potential rows/records as a precaution in case the data source rows increase
        total_records = 1000000
        records = []

        for i in range(0, total_records, 1000):
            response = requests.get(url+f'?offset={i}', headers=headers)
            records.append(response.json())
        
        return records
    except ValueError as e:
        print(f'[ERROR]: ☠️ An error occurred in retrieving_json_data function {e}')
        return e

def extract_enrollment_features(records: list)->list:
    """Extracting table lables as features, have not found a use for them yet"""
    features = []
    for key in records[0][0].keys():
        features.append(key)
    
    return features

def process_monthly_enrollment_data(url):
    try:
        print(f'[INFO]: 💻 Making request to {url}\n')
        records = retrieve_json_data(url)

        # extract enrollement features
        print(f'[INFO]: 🔑 Extracting keys from json data to reference as features\n')
        features = extract_enrollment_features(records)
        
        print('[INFO]: 🔑 Current Features:')
        for feature in features:
            print(feature)

        # create a list of dictionaries containing data
        monthly_enrollments = []
        todays_date = datetime.now()
        date = todays_date.strftime('%Y_%m_%d')

        # file destination
        file = f'C:\\Users\\kwesi\\data_projects\\cms_data_pipeline\\data\\{date}_monthly_enrollments.csv'

        for i in records:
            for j in range(len(i)):
                monthly_enrollment = {
                    'year':                                     str(i[j]['YEAR']),
                    'month':                                    str(i[j]['MONTH']),
                    'state':                                    str(i[j]['BENE_STATE_ABRVTN']),
                    'tot_benes':                                null_check(i[j]['TOT_BENES']),
                    'male_tot_benes':                           null_check(i[j]['MALE_TOT_BENES']),
                    'female_tot_benes':                         null_check(i[j]['FEMALE_TOT_BENES']),
                    'black_tot_benes':                          null_check(i[j]['BLACK_TOT_BENES']),
                    'white_tot_Benes':                          null_check(i[j]['WHITE_TOT_BENES']),
                    'hspnc_tot_benes':                          null_check(i[j]['HSPNC_TOT_BENES']),
                    'prscrptn_drug_tot_benes':                  null_check(i[j]['PRSCRPTN_DRUG_TOT_BENES']),
                    'prscrptn_drug_pdp_benes':                  null_check(i[j]['PRSCRPTN_DRUG_PDP_BENES']),
                    'prscrptn_drug_mapd_benes':                 null_check(i[j]['PRSCRPTN_DRUG_MAPD_BENES']),
                    'prscrptn_drug_deemed_eligible_lis_benes':  null_check(i[j]['PRSCRPTN_DRUG_DEEMED_ELIGIBLE_FULL_LIS_BENES']),
                    'prscrptn_drug_full_lis_benes':             null_check(i[j]['PRSCRPTN_DRUG_FULL_LIS_BENES']),
                    'prscrptn_drug_partial_lis_benes':          null_check(i[j]['PRSCRPTN_DRUG_PARTIAL_LIS_BENES']),
                    'prscrptn_drug_no_lis_benes':               null_check(i[j]['PRSCRPTN_DRUG_NO_LIS_BENES'])
                }
                monthly_enrollments.append(monthly_enrollment)
        
        # convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(monthly_enrollments)

        # save to csv in specific location
        print(f'\n[INFO]: 📂 Saving data from {url} as CSV in data folder\n')
        df.to_csv(file, index=False)
    except requests.exceptions.RequestException as e:
        raise Exception(f'Failed to retrieve data from {url}. Error: {e}')
