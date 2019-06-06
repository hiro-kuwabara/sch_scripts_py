import requests
import json
import getpass

SCH_USER = input('Enter SCH username: ')
SCH_PASS = getpass.getpass('Enter Password: ')

JOB_ID='<your_job_id>'

SCH_BASE_URL = 'https://cloud.streamsets.com'

LOGIN_URL = SCH_BASE_URL + '/security/public-rest/v1/authentication/login'
LOGIN_HEADERS = {'X-Requested-By': 'sdc', 'Content-Type': 'application/json'}
LOGIN_DATA = {'userName': SCH_USER, 'password': SCH_PASS}

RESET_URL = SCH_BASE_URL + '/jobrunner/rest/v1/job/' + JOB_ID + '/resetOffset'
START_URL = SCH_BASE_URL + '/jobrunner/rest/v1/job/' + JOB_ID + '/start'
START_DATA = {'jobId': JOB_ID}

print('Obtaining auth token...')
try:
	loginresponse = requests.post(LOGIN_URL, data=json.dumps(LOGIN_DATA), headers=LOGIN_HEADERS)
except requests.exceptions.RequestException as e:
	print(e)
	sys.exit(1)
if loginresponse.status_code != requests.codes.ok:
	loginresponse.raise_for_status()
token = loginresponse.headers['X-SS-User-Auth-token']

HEADERS = {'X-Requested-By': 'sdc',
	   'X-SS-REST-CALL': 'true',
	   'X-SS-User-Auth-Token': token,
	   'Content-type': 'application/json'}

print('Resetting offset...')
try:
	reset_response = requests.post(RESET_URL, headers=HEADERS)
except requests.exceptions.RequestException as e:
	print(e)
	sys.exit(1)

if reset_response.status_code != requests.codes.ok:
	print(reset_response.text)
	reset_response.raise_for_status()

print('Job offset has been reset!')
print(reset_response.text)

print('Starting pipeline...')
try:
	start_response = requests.post(START_URL, data=json.dumps(START_DATA), headers=HEADERS)
except requests.exceptions.RequestException as e:
	print(e)
	sys.exit(1)

if start_response.status_code != requests.codes.ok:
	print(start_response.text)
	start_response.raise_for_status()

print('Job has been started!')
print(start_response.text)
