import requests
from requests.sessions import Session
from BeautifulSoup import BeautifulSoup
from urllib import urlencode
import csv
import os
from cStringIO import StringIO
from string import ascii_lowercase
from boto.s3.connection import S3Connection
from boto.s3.key import Key

AWS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET = os.environ['AWS_SECRET_KEY']

CONT_GET_PARAMS = {
    'AddressSearchType': 'Starts with',
    'Archived': 'false',
    'CitySearchType': 'Starts with',
    'ContributionType': 'All Types',
    'DownloadListType': 'Receipts',
    'EmployerSearchType': 'Starts with',
    'FirstNameSearchType': 'Starts with',
    'LastOnlyNameSearchType': 'Starts with',
    'LastOnlyName': '',
    'FirstName': '',
    'Address': '',
    'City': '',
    'State': '',
    'Zip': '',
    'ZipThru': '',
    'Occupation': '',
    'Employer': '',
    'VendorLastOnlyName': '',
    'VendorFirstName': '',
    'VendorAddress': '',
    'VendorCity': '',
    'VendorState': '',
    'VendorZip': '',
    'VendorZipThru': '',
    'LinkedQuery': 'false',
    'OccupationSearchType': 'Starts with',
    'OrderBy': 'Date Received - most recent first',
    'PurposeState': 'Starts with',
    'QueryType': 'Contrib',
    'RcvDate': None,
    'RcvDateThru': None,
    'VendorAddressSearchType': 'Starts with',
    'VendorCitySearchType': 'Starts with',
    'VendorFirstNameSearchType': 'Starts with',
    'VendorLastOnlyNameSearchType': 'Starts with',
    'OtherReceiptsDescriptionSearchType': '',
    'OtherReceiptsDescription': '',
    'Purpose': '',
    'Amount': '',
    'AmountThru': ''
}

COMM_GET_PARAMS = {
    'DownloadListType': 'Committees',
    'Active': 'true',
    'NameSearchType': 'Starts with',
    'Name': None,
    'AddressSearchType': 'Starts with',
    'Address': '',
    'CitySearchType': 'Starts with',
    'City': '',
    'State': '',
    'Zip': '',
    'ZipThru': '',
    'CommitteeID': '',
    'CmteTypeSearch': 'Select a Type',
    'OrderBy': 'Committee Name - A to Z'
}

EXP_GET_PARAMS = {
    'DownloadListType': 'Expenditures',
    'ExpenditureSearchType': 'All',
    'ExpenditureType': 'All Types',
    'LastOnlyNameSearchType': 'Starts with',
    'LastOnlyName': '',
    'FirstNameSearchType': 'Starts with',
    'FirstName': '',
    'AddressSearchType': 'Starts with',
    'Address': '',
    'CitySearchType': 'Starts with',
    'City': '',
    'State': '',
    'Zip': '',
    'ZipThru': '',
    'CandidateNameSearchType': 'Starts with',
    'CandidateName': '',
    'OfficeSearchType': 'Starts with',
    'Office': '',
    'Opposing': '',
    'Supporting': '',
    'PurposeSearchType': 'Starts with',
    'Purpose': '',
    'BeneficiarySearchType': 'Starts with',
    'Beneficiary': '',
    'Amount': '',
    'AmountThru': '',
    'ExpendedDate': None,
    'ExpendedDateThru': None,
    'Archived': 'false',
    'QueryType': 'Expend',
    'LinkedQuery': 'false',
    'OrderBy': 'Last or Only Name - A to Z',
}

BASE_URL = 'http://www.elections.state.il.us/CampaignDisclosure'

ALPHA_NUM = [l for l in ascii_lowercase] + [n for n in range(10)]

def fetch_data(dl_type=None, **kwargs):
    """ 
    Fetch Receipts, Expenditures, and Committees. 
    dl_type is one of those three choices. 
    kwargs depend on the choice. 
    Receipts and Expenditures need start_date and end_date for search.
    Committees need a name_start kwarg to pass into the search.
    """
    s = Session()
    if dl_type == 'Receipts':
        CONT_GET_PARAMS['RcvDate'] = kwargs['start_date']
        CONT_GET_PARAMS['RcvDateThru'] = kwargs['end_date']
        url = '%s/DownloadList.aspx?%s' % (BASE_URL, urlencode(CONT_GET_PARAMS))
    elif dl_type == 'Committees':
        COMM_GET_PARAMS['Name'] = kwargs['name_start']
    elif dl_type == 'Expenditures':
        EXP_GET_PARAMS['ExpendedDate'] = kwargs['start_date']
        EXP_GET_PARAMS['ExpendedDateThru'] = kwargs['end_date']
        url = '%s/DownloadList.aspx?%s' % (BASE_URL, urlencode(EXP_GET_PARAMS))
    g = s.get(url)
    soup = BeautifulSoup(g.content)
    view_state = soup.find('input', attrs={'id': '__VIEWSTATE'}).get('value')
    event_val = soup.find('input', attrs={'id': '__EVENTVALIDATION'}).get('value')
    d = {
        '__VIEWSTATE': view_state,
        '__EVENTVALIDATION': event_val,
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btnText',
        'ctl00$pnlMenu_CollapsiblePanelExtender_ClientState': 'true',
        'ctl00$AccordionStateBoardMenu_AccordionExtender_ClientState': '0',
        'ctl00$mtbSearch': '',
        'ctl00$AccordionPaneStateBoardMenu_content$AccordionMainContent_AccordionExtender_ClientState': '-1',
        'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1',
        '__EVENTARGUMENT': '',
    }
    dl_page = s.post(url, data=d)
    conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = '%s_%s.tsv' % (year, dl_type.lower())
    k.set_contents_from_string(dl_page.content)
    k.make_public()
    return 'Saved it.'

