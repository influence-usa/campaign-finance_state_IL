import requests
from requests.sessions import Session
from BeautifulSoup import BeautifulSoup
from urllib import urlencode
import csv
import os
from cStringIO import StringIO
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
    'Active': 'false',
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
    'CmteTypeSearch': '',
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

def fetch_data(dl_type=None, **kwargs):
    """ 
    Fetch Receipts, Expenditures, and Committees. 
    dl_type is one of those three choices. 
    kwargs depend on the choice. 
    Receipts and Expenditures need start_date and end_date for search.
    Committees need a name_start kwarg to pass into the search.
    
    Seems like the maximum that you can get is about 250,000 records at a time.
    """
    s = Session()
    post_data = {
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btnText',
        'ctl00$pnlMenu_CollapsiblePanelExtender_ClientState': 'true',
        'ctl00$AccordionStateBoardMenu_AccordionExtender_ClientState': '0',
        'ctl00$mtbSearch': '',
        'ctl00$AccordionPaneStateBoardMenu_content$AccordionMainContent_AccordionExtender_ClientState': '-1',
        'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1',
        '__EVENTARGUMENT': '',
    }
    if dl_type == 'Receipts':
        CONT_GET_PARAMS['RcvDate'] = kwargs['start_date']
        CONT_GET_PARAMS['RcvDateThru'] = kwargs['end_date']
        url = '%s/DownloadList.aspx?%s' % (BASE_URL, urlencode(CONT_GET_PARAMS))
    elif dl_type == 'Committees':
        COMM_GET_PARAMS['Name'] = kwargs['name_start']
        url = '%s/DownloadList.aspx?%s' % (BASE_URL, urlencode(COMM_GET_PARAMS))
    elif dl_type == 'Expenditures':
        EXP_GET_PARAMS['ExpendedDate'] = kwargs['start_date']
        EXP_GET_PARAMS['ExpendedDateThru'] = kwargs['end_date']
        url = '%s/DownloadList.aspx?%s' % (BASE_URL, urlencode(EXP_GET_PARAMS))
    elif dl_type == 'Candidates':
      url = 'http://www.elections.state.il.us/ElectionInformation/CandDataFile.aspx?id=%s' % kwargs['election_id']
    g = s.get(url)
    soup = BeautifulSoup(g.content)
    view_state = soup.find('input', attrs={'id': '__VIEWSTATE'}).get('value')
    event_val = soup.find('input', attrs={'id': '__EVENTVALIDATION'}).get('value')
    post_data['__VIEWSTATE'] = view_state
    post_data['__EVENTVALIDATION'] = event_val
    dl_page = s.post(url, data=post_data)
    if dl_page.status_code == 200:
        return dl_page.content
    else:
        return None

def load_disclosure(dl_type, start, end):
    conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = conn.get_bucket('il-elections')
    for year in range(start, end):
        start_date = '1/1/%s' % year
        end_date = '12/31/%s' % year
        print 'Saving %s %s' % (year, dl_type)
        content = fetch_data(dl_type=dl_type, start_date=start_date, end_date=end_date)
        k = Key(bucket)
        k.key = '%s/%s_%s.tsv' % (dl_type, year, dl_type.lower())
        k.set_contents_from_string(content)
        k.make_public()
    return None

if __name__ == "__main__":
    import sys
    from datetime import datetime
    scrape_type = sys.argv[1]
    if scrape_type == 'receipts_expenditures':
        this_year = datetime.now().year
        for dl_type in ['Expenditures', 'Receipts']:
            load_disclosure(dl_type, 1989, this_year + 1)
    elif scrape_type == 'committees':
        from string import ascii_lowercase
        alpha_num = [l for l in ascii_lowercase] + [n for n in range(10)]
        all_comms = []
        header = None
        for char in alpha_num:
            comm_info = fetch_data(dl_type='Committees', name_start=char)
            inp = StringIO(comm_info)
            reader = csv.reader(inp, delimiter='\t')
            header = reader.next()
            all_comms.extend(list(reader))
        all_comms.sort()
        no_dup_comms = []
        for comm in all_comms:
            if comm not in no_dup_comms:
                no_dup_comms.append(comm)
        outp = StringIO()
        writer = csv.writer(outp)
        writer.writerow(header)
        writer.writerows(no_dup_comms)
        outp.seek(0)
        conn = S3Connection(AWS_KEY, AWS_SECRET)
        bucket = conn.get_bucket('il-elections')
        k = Key(bucket)
        k.key = 'Committees.csv'
        k.set_contents_from_file(outp)
        k.make_public()

    elif scrape_type == 'candidates':
        id = 1
        blank = 0
        all_cands = []
        header = None
        last = False
        while not last:
            cand_info = fetch_data(dl_type='Candidates', election_id=id)
            if not cand_info \
                or 'Unexpected errors occurred trying to populate page.' in cand_info:
                blank += 1
                if blank > 20:
                    last = True
            else:
                inp = StringIO(cand_info)
                reader = csv.reader(inp)
                header = reader.next()
                all_cands.extend(list(reader))
                blank = 0
            id += 1
        all_cands.sort()
        no_dup_cands = []
        for cand in all_cands:
            if cand not in no_dup_cands:
                no_dup_cands.append(cand)
        outp = StringIO()
        writer = csv.writer(outp)
        writer.writerow(header)
        writer.writerows(all_cands)
        outp.seek(0)
        conn = S3Connection(AWS_KEY, AWS_SECRET)
        bucket = conn.get_bucket('il-elections')
        k = Key(bucket)
        k.key = 'Candidates.csv'
        k.set_contents_from_file(outp)
        k.make_public()
