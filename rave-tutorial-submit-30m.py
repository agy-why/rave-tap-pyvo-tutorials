from pkg_resources import parse_version
import requests
import pyvo as vo
import pandas as pd
from pyvo.dal import TAPService

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

tap_service = TAPService(url, session=tap_session)

#
# Submit the query as an async job
#
query_name = "multi_dr3"
lang = 'PostgreSQL'

query = '''
-- Multiple observations in DR3

SELECT F.raveid, F.radeg, F.dedeg, T.N 
FROM 
    ( SELECT raveid, count(*) AS N 
      FROM ravedr3.rave_dr3a 
          GROUP BY raveid
          HAVING count(*) > 1 
          ORDER BY N DESC ) T JOIN ravedr3.rave_dr3a F 
    ON F.raveid = T.raveid
'''

job = tap_service.submit_job(query, language=lang, runid=query_name, queue="30m")
job.run()

print('JOB {name}: SUBMITTED'.format(name=job.job.runid))
print('JOB {name}: {status}'.format(name=job.job.runid , status=job.phase))

#
# Save the job's url in a file to later retrieve results.
#
print('URL: {}'.format(job.url))

with open('job_url.txt', 'w') as fd:
    fd.write(job.url)
