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
# Rerunning the last example01
#

target_runid = 'rave-example01'

# obtain the list of completed job_descriptions
archived_job_descriptions = tap_service.get_job_list(phases='ARCHIVED')

for job_description in archived_job_descriptions:
    
    # select the job with runid fitting target_runid
    if job_description.runid == target_runid:
        
        # get jobid
        jobid = job_description.jobid
    
        # recreate the url by hand
        job_url = tap_service.baseurl + '/async/' + jobid
    
        # recreate the archived job
        archived_job = AsyncTAPJob(job_url, session=tap_session)
    
        # extract the query
        query = archived_job.query
    
        # resubmit the query with corresponding parameters
        job = tap_service.submit_job(query, language='PostgreSQL', runid='rerun', queue='60s')
        print('{url}:\n{query}\n'.format(url=job_url, query=query))
    
        # restart the archived_job
        job.run()

        break

#
# Wait to be completed (or an error occurs)
#
job.wait(phases=["COMPLETED", "ERROR", "ABORTED"], timeout=60.)
print('JOB {name}: {status}'.format(name=job.job.runid , status=job.phase))

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
