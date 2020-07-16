from pkg_resources import parse_version
import requests
import pyvo as vo
import os
import urllib

#
# Verify the version of pyvo 
#
if parse_version(vo.__version__) < parse_version('1.0'):
    raise ImportError('pyvo version must be at least than 1.0')
    
print('\npyvo version {} \n'.format(vo.__version__))

#
# Setup tap_service
#
name = 'rave-survey.org'
url = "https://www.rave-survey.org/tap"
token = 'Token <your-token>'

print('TAP service {} \n'.format(name))

# Setup authorisation
tap_session = requests.Session()
tap_session.headers['Authorization'] = token

tap_service = vo.dal.TAPService(url, session=tap_session)

#
# Query the fits files
#
lang = "PostgreSQL"

query = """
SELECT spectrum_fits 
FROM ravedr6.dr6_spectra 
  WHERE rave_obs_id LIKE '200304%' 
"""

# Submit the query as Synchronous job
tap_result = tap_service.run_sync(query, language=lang)
path_to_fits = tap_result.to_table()

#
# Download the fits files into local directory
#
target_directory = './fits/'
fit_file_base_url = 'https://www.rave-survey.org/files/'

for fit_file in path_to_fits[:10]: # the 10 first ones
    
    # extract name of the fits
    fit_name = os.path.basename(fit_file[0])
    
    # set the target local file
    fit_file_name = os.path.join(target_directory, fit_name)
    
    # build the url pointing to the fit file
    fit_file_url = os.path.join(fit_file_base_url, fit_file[0])
    
    # download and save into target file
    print("Downloaded {fitfile} into {target}".format(fitfile=fit_name, target=fit_file_name))
    urllib.request.urlretrieve(fit_file_url, fit_file_name)
    
print('\nDone')
