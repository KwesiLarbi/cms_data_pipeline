from utils import process_monthly_enrollment_data

def data_pipeline(url):
    process_monthly_enrollment_data(url)

    return 'Success'
 
if __name__ == '__main__':
    url = 'https://data.cms.gov/data-api/v1/dataset/d7fabe1e-d19b-4333-9eff-e80e0643f2fd/data'
    data_pipeline(url)