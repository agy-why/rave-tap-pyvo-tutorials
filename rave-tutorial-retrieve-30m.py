from pkg_resources import parse_version
import requests
import pyvo as vo
import pandas as pd
from pyvo.dal.tap import AsyncTAPJob

#
# Verify the version of pyvo 
#
if parse_version(vo.__version__) < parse_version('1.0'):
    raise ImportError('pyvo version must larger than 1.0')
    
print('\npyvo version {version} \n'.format(version=vo.__version__))

#
# Setup tap_service
#
service_name = 'rave-survey.org'
url = "https://www.rave-survey.org/tap"
token = 'Token <your-token>'

print('TAP service {} \n'.format(service_name))

# Setup authorization
tap_session = requests.Session()
tap_session.headers['Authorization'] = token

# tap_service = vo.dal.TAPService(url, session=tap_session) # rmrk: this is not needed

#
# Recreate the job from url and session (token)
#

# read the url
with open('job_url.txt', 'r') as fd:
    job_url = fd.readline()

# recreate the job 
job = AsyncTAPJob(job_url, session=tap_session)

#
# Check the job status
#
print('JOB {name}: {status}'.format(name=job.job.runid , status=job.phase))

# if still running --> exit
if job.phase not in ("COMPLETED", "ERROR", "ABORTED"):
    exit(0)

#
# Fetch the results
#
job.raise_if_error()
print('\nfetching the results...')
tap_results = job.fetch_result()
print('...DONE\n')

#
# Convert to a pandas.DataFrame
#
results = tap_results.to_table().to_pandas()
print(results.head())
