from il_elex_scraper import IllinoisElectionScraper
from urlparse import parse_qs, urlparse
from collections import OrderedDict
import lxml.html
from requests.sessions import Session
from cStringIO import StringIO
from BeautifulSoup import BeautifulSoup
from csvkit.unicsv import UnicodeCSVWriter, UnicodeCSVReader, \
    UnicodeCSVDictReader, UnicodeCSVDictWriter
import scrapelib

class CandidateScraper(IllinoisElectionScraper):
    
    def scrape_one(self, comm_id):
        url = self.url_pattern % comm_id
        try:
            page = self._lxmlize(url)
        except scrapelib.HTTPError:
            yield None
        data = OrderedDict((
            ('ID', ''),
            ('FullName', ''),
            ('FullAddress', ''),
            ('PartyName', ''),
            ('OfficeName', ''),
        ))
        names = page.xpath('//td[@class="tdCandidateName"]/a')
        for name in names:
            data['FullName'] = name.text_content()
            detail_url = name.attrib['href']
            if detail_url:
                print detail_url
                try:
                    detail_page = self._lxmlize(detail_url)
                except scrapelib.HTTPError:
                    yield None
                header_box = detail_page.xpath('//table[@summary="Candidate Detail Table"]')[0]
                address = header_box.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblAddress"]')
                if address:
                    a = lxml.html.tostring(address[0])
                    l_idx = a.find('>')
                    r_idx = a.find('</')
                    add_parts = a[l_idx + 1:r_idx].split('<br>')
                    data['FullAddress'] = ' '.join([a.strip() for a in add_parts])
                party = header_box.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblParty"]')
                if party:
                    data['PartyName'] = party[0].text_content().strip()
                office = header_box.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblOffice"]')
                if office:
                    data['OfficeName'] = office[0].text_content().strip()
                parsed = urlparse(detail_url)
                data['ID'] = parse_qs(parsed.query)['id'][0]
                yield data

def fetch_data(election_id):
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
    url = 'http://www.elections.state.il.us/ElectionInformation/CandDataFile.aspx?id=%s' % election_id
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

def scrape_by_election():
    id = 1
    blank = 0
    all_cands = []
    header = None
    last = False
    while not last:
        cand_info = fetch_data(id)
        if not cand_info \
            or 'Unexpected errors occurred trying to populate page.' in cand_info:
            blank += 1
            if blank > 20:
                last = True
        else:
            inp = StringIO(cand_info)
            reader = UnicodeCSVReader(inp)
            header = reader.next()
            all_cands.extend(list(reader))
            blank = 0
        id += 1
    all_cands.sort()
    no_dup_cands = []
    header.extend(['FullName', 'FullAddress'])
    for cand in all_cands:
        if cand not in no_dup_cands and cand != header:
            cand.insert(-2, '%s %s' % (cand[4], cand[3]))
            cand.insert(-1, '%s %s %s, %s %s' % \
                (cand[7], cand[8], cand[9], cand[10], cand[11]))
            no_dup_cands.append(cand)
    return header, no_dup_cands

if __name__ == "__main__":
    import os
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
    from csvkit.table import Table
    from csvkit.sql import make_table, make_create_table_statement
    import sqlite3

    AWS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET_KEY']
    DB_NAME = 'candidates.db'
    
   #create_table = 'CREATE TABLE candidates (\
   #    "ID" INTEGER,\
   #    "BracketID" INTEGER,\
   #    "SlateID" INTEGER,\
   #    "LastName" VARCHAR(24),\
   #    "FirstName" VARCHAR(27),\
   #    "AffilCommit" VARCHAR(13),\
   #    "HeadOfSlate" BOOLEAN,\
   #    "Address1" VARCHAR(35),\
   #    "Address2" VARCHAR(35),\
   #    "City" VARCHAR(19),\
   #    "State" VARCHAR(4),\
   #    "Zip" VARCHAR(10),\
   #    "FileDateTime" DATETIME,\
   #    "Sequence" INTEGERL,\
   #    "Status" VARCHAR(1),\
   #    "StatusDateTime" DATETIME,\
   #    "WebSiteAddress" VARCHAR(75),\
   #    "ElectionDate" DATE,\
   #    "ElectionType" VARCHAR(2),\
   #    "PartyName" VARCHAR(34),\
   #    "PartySequence" INTEGER,\
   #    "OfficeName" VARCHAR(84),\
   #    "OfficeBallotGroup" VARCHAR(2),\
   #    "OfficeSequence" INTEGER,\
   #    "FullName" VARCHAR(32),\
   #    "FullAddress" VARCHAR(32)\
   #)'

    create_table = 'CREATE TABLE candidates (\
        "ID" INTEGER,\
        "CommitteeID" INTEGER,\
        "PartyName" VARCHAR(34),\
        "OfficeName" VARCHAR(84),\
        "FullName" VARCHAR(32),\
        "FullAddress" VARCHAR(32)\
    )'

    if os.path.exists('candidates.db'):
        os.remove('candidates.db')

   #header, cands_by_election = scrape_by_election()
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(create_table)
    conn.commit()
   #for row in cands_by_election:
   #    c.execute('insert into candidates ("ID","BracketID","SlateID",\
   #        "LastName","FirstName","AffilCommit","HeadOfSlate","Address1",\
   #        "Address2","City","State","Zip","FileDateTime","Sequence",\
   #        "Status","StatusDateTime","WebSiteAddress","ElectionDate",\
   #        "ElectionType","PartyName","PartySequence","OfficeName",\
   #        "OfficeBallotGroup","OfficeSequence","FullName","FullAddress")\
   #        values(:ID,:BracketID,:SlateID,\
   #        :LastName,:FirstName,:AffilCommit,:HeadOfSlate,:Address1,\
   #        :Address2,:City,:State,:Zip,:FileDateTime,:Sequence,\
   #        :Status,:StatusDateTime,:WebSiteAddress,:ElectionDate,\
   #        :ElectionType,:PartyName,:PartySequence,:OfficeName,\
   #        :OfficeBallotGroup,:OfficeSequence,:FullName,:FullAddress)',
   #        {k:v for k,v in zip(header, row)})
   #    conn.commit()

    inp = StringIO()
    s3_conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = s3_conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = 'Committees.tsv'
    committee_file = k.get_contents_to_file(inp)
    inp.seek(0)
    reader = UnicodeCSVDictReader(inp, delimiter='\t')
    comm_ids = [i['id'] for i in list(reader)]

    candidate_pattern = '/CommitteeDetailCandidates.aspx?id=%s'
    cand_scraper = CandidateScraper(url_pattern=candidate_pattern)
    cand_scraper.cache_storage = scrapelib.cache.FileCache('cache')
    cand_scraper.cache_write_only = False
    for comm_id in comm_ids:
        for cand in cand_scraper.scrape_one(comm_id):
            if cand:
                cand['CommitteeID'] = comm_id
                insert = 'insert into candidates("ID", "FullName", "FullAddress", \
                    "PartyName", "OfficeName", "CommitteeID") values (:ID, :FullName, :FullAddress, \
                    :PartyName, :OfficeName, :CommitteeID)'
                c.execute(insert, cand)
                conn.commit()
            else:
                print 'Got a 500 for %s' % comm_id
