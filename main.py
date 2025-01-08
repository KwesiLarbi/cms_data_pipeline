from cms_extract import process_monthly_enrollment_data
from utils import data_quality_check

def data_pipeline(url: str):
    try:
        print('[INFO]: 🕓 Starting processing for monthly enrollment data\n')
        process_monthly_enrollment_data(url)
        print('[SUCCESS]: ✅ Monthly Enrollment Data successfully processed \n')
    except ValueError as e:
        print('[FAIL]: 🟥 An error occurred in the Monthly enrollment method\n')
        
    return 'Success'
 
if __name__ == '__main__':
    url = 'https://data.cms.gov/data-api/v1/dataset/d7fabe1e-d19b-4333-9eff-e80e0643f2fd/data'
    # data_pipeline(url)
    data_quality_check()