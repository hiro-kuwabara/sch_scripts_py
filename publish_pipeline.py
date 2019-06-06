import requests
import json
import getpass

SCH_USER = input('Enter SCH username: ')
SCH_PASS = getpass.getpass('Enter Password: ')

JSON_PATH = input('Enter path to JSON file: ')
COMMIT_MESSAGE = input('Enter commit message: ')

SCH_BASE_URL = 'https://cloud.streamsets.com'

LOGIN_URL = SCH_BASE_URL + '/security/public-rest/v1/authentication/login'
LOGIN_HEADERS = {'X-Requested-By': 'sdc', 'Content-Type': 'application/json'}
LOGIN_DATA = {'userName': SCH_USER, 'password': SCH_PASS}

print('Obtaining auth token...')
try:
	loginresponse = requests.post(LOGIN_URL, data=json.dumps(LOGIN_DATA), headers=LOGIN_HEADERS)
except requests.exceptions.RequestException as e:
	print(e)
	sys.exit(1)
if loginresponse.status_code != requests.codes.ok:
	loginresponse.raise_for_status()
token = loginresponse.headers['X-SS-User-Auth-token']

with open(JSON_PATH, 'r') as f:
    pipconfig = json.load(f)

PUBLISH_URL= SCH_BASE_URL + '/pipelinestore/rest/v1/pipelines'
PIP_NAME=pipconfig['pipelineConfig']['title']
LIB_DEF=pipconfig['libraryDefinitions']
PIP_DEF=pipconfig['pipelineConfig']
RULE_DEF=pipconfig['pipelineRules']
PUBLISH_DATA = {'name': PIP_NAME,
				'commitMessage': COMMIT_MESSAGE,
				'libraryDefinitions': json.dumps(LIB_DEF),
				'pipelineDefinition': json.dumps(PIP_DEF),
				'rulesDefinition': json.dumps(RULE_DEF)}
HEADERS = {'X-Requested-By': 'sdc',
		   'X-SS-REST-CALL': 'true',
		   'X-SS-User-Auth-Token': token,
		   'Content-type': 'application/json'}

print('Publishing pipeline...')
try:
	publish_response = requests.put(PUBLISH_URL, headers=HEADERS, data=json.dumps(PUBLISH_DATA))
except requests.exceptions.RequestException as e:
	print(e)
	sys.exit(1)

if publish_response.status_code != requests.codes.ok:
	publish_response.raise_for_status()

print('Pipeline published!')
