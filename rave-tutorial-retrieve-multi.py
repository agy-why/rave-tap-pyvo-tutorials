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

# Setup authorization
tap_session = requests.Session()
tap_session.headers['Authorization'] = token

# tap_service = vo.dal.TAPService(url, session=tap_session) # rmrk: this is not needed

partial_results = []
running_job_names = []

#
# Recreate the job from url and session (token)
#

# read the url
with open('jobs_url.txt', 'r') as fd:
    job_urls = fd.readlines()

# reopen the file to store the non finished jobs
fd = open('jobs_url.txt', 'w')

for job_url in job_urls:

    # recreate the job 
    job = AsyncTAPJob(job_url, session=tap_session)

    #
    # Check the job status
    #
    print('JOB {name}: {status}'.format(name=job.job.runid , status=job.phase))

    # if still running --> exit
    if job.phase not in ("COMPLETED", "ERROR", "ABORTED"):
        running_job_names.append(job.job.runid)
        fd.write(job_url)
        continue

    #
    # Fetch the results
    #
    job.raise_if_error() # This need to be caught!!
    print('fetching the results...\n')
    partial_results.append(job.fetch_result())
    
print('...DONE\n')

try:
    assert(running_job_names == [])
except AssertionError:
    print("The following jobs are still executing: {}".format(running_job_names))
