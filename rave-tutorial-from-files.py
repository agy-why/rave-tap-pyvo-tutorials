from pkg_resources import parse_version
import requests
import os
import pyvo as vo
import pandas as pd
import glob

#
# Verify the version of pyvo 
#
if parse_version(vo.__version__) < parse_version('1.0'):
    raise ImportError('pyvo version must be at least than 1.0')
    
print('\npyvo version {version} \n'.format(version=vo.__version__))

#
# Setup tap_service
#
service_name = 'rave-survey.org'
url = "https://www.rave-survey.org/tap"
token = 'Token <your-token>'

print('TAP service {} \n'.format(service_name))

# Setup authorisation
tap_session = requests.Session()
tap_session.headers['Authorization'] = token

tap_service = vo.dal.TAPService(url, session=tap_session)

#
# Submit the query as an Asynchrone job
#

# find all .sql files in current directory
queries_filename = sorted(glob.glob('./*.sql'))
print('Sending {n} examples'.format(n=len(queries_filename)))

# initialize test results
jobs = []
failed = []

# send all queries
for query_filename in queries_filename:

    # read the .SQL file
    with open(query_filename, 'r') as fd:
        query = ' '.join(fd.readlines())

    # Set language from comments (default: PostgreSQL)
    if 'LANGUAGE = ADQL' in query:
        lang = 'ADQL'
    else:
        lang = 'PostgreSQL'

    # Set queue from comments (default: 30s)
    if 'QUEUE = 5m' in query:
        queue = "5m"
    elif 'QUEUE = 2h' in query:
        queue = "2h"
    else:
        queue = "30s"

    # Set the runid from sql filename
    base = os.path.basename(query_filename)
    runid = os.path.splitext(base)[0]
    
    print('\n> Query : {name}\n{query}\n'.format(name=runid, query=query))

    # Create the asnc job
    try:
        jobs.append(tap_service.submit_job(query, language=lang, runid=runid, queue=queue))
    except Exception as e:
        print('ERROR could not create the job.')
        print(e)
        failed.append(runid)
        continue

    # Run the run
    try:
        jobs[-1].run()
    except Exception as e:
        print('Error: could not run the job. Are you sure about the SQL query?')
        print(e)
        failed.append(runid)
        jobs.pop()

#
# Verify Results
#       
print('\nWait for job to finish...\n')
# check status all queries
for job in jobs: 

    # Wait for Completed
    job.wait(phases=["COMPLETED", "ERROR", "ABORTED"], timeout=10.)
    if job.phase != "COMPLETED":
        failed.append(job.job.runid)
    else:
        print("job {}: {}".format(job.job.runid, job.phase))

# Assert success or return failure
try:
    assert(failed == [])
    print('\nDONE!\n')
except AssertionError:
    print("\nSome test failed: {failed}\n".format(failed=failed))
