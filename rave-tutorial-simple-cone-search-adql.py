from pkg_resources import parse_version
import requests
import pyvo as vo
import pandas as pd

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
# Setting the cone search parameters
#

# RA (in degrees)
ra = 245.8962

# DEC (in degreees)
dec = -26.5222

# Radius (in arcsec)
sr = 0.5

#
# Submit the query as an async job
#
query_name = "simple_cs_adql"
lang = 'ADQL'

query = '''
-- Simple cone search

SELECT rave_obs_id, ra_input, dec_input, DISTANCE( POINT('ICRS', ra_input, dec_input), POINT('ICRS', {ra:.4f}, {dec:.4f}) ) AS dist
FROM ravedr6.dr6_obsdata
WHERE 1 = CONTAINS( POINT('ICRS', ra_input, dec_input), CIRCLE('ICRS', {ra:.4f}, {dec:.4f}, {radius:.4f}) )
'''.format(ra=ra, dec=dec, radius=sr)

job = tap_service.submit_job(query, language=lang, runid=query_name, queue="60s")
job.run()

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
