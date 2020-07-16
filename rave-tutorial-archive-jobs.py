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

tap_service = vo.dal.TAPService(url, session=tap_session) 

#
# Archiving all COMPLETED jobs
#

# obtain the list of completed job_descriptions
completed_job_descriptions = tap_service.get_job_list(phases='COMPLETED')

# Archiving each of them
for job_description in completed_job_descriptions:
    
    # get the jobid
    jobid = job_description.jobid
    
    # recreate the url by hand
    job_url = tap_service.baseurl + '/async/' + jobid
    
    # recreate the job
    job = AsyncTAPJob(job_url, session=tap_session)
    
    print('Archiving: {url}'.format(url=job_url))
    job.delete() # archive job
