from il_elex_scraper import IllinoisElectionScraper
from urlparse import parse_qs, urlparse

class CandidateScraper(IllinoisElectionScraper):
    
    def scrape_one(self, comm_id):
        url = self.url_pattern % comm_id
        print url
        page = self._lxmlize(url)
        table_rows = page.xpath('//tr[starts-with(@class, "SearchListTableRow")]')
        for row in table_rows:
            data = {}
            name = row.xpath('td[contains(@headers, "thCandidateName")]/a')[0]
            data['name'] = name.text_content()
            data['url'] = name.attrib['href']
            if data['url']:
                detail_page = self._lxmlize(data['url'])
                header_box = detail_page.xpath('//td[contains(@headers, "CandidateInformation")]')[0]
                address = header_box.xpath('span[contains(@class, "lblAddress")]')
                if address:
                    data['address'] = address[0].text_content()
                else:
                    data['address'] = row.xpath('td[contains(@class, "tdCandidateAddress")]')[0].text_content()
                party = header_box.xpath('span[contains(@class, "lblParty")]')
                if party:
                    data['party'] = party[0].text_content()
                office = header_box.xpath('span[contains(@class, "lblOffice")]')
                if office:
                    data['office'] = office[0].text_content()
                parsed = urlparse(data['url'])
                data['id'] = parse_qs(parsed.query)['id'][0]
                yield data

if __name__ == "__main__":
    from cStringIO import StringIO
    import os
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
    from csvkit.unicsv import UnicodeCSVDictWriter, UnicodeCSVDictReader
    
    AWS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET_KEY']
    
    inp = StringIO()
    s3_conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = s3_conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = 'Committees.tsv'
    committee_file = k.get_contents_to_file(inp)
    inp.seek(0)
    reader = UnicodeCSVDictReader(inp, delimiter='\t')
    comm_ids = [c['id'] for c in list(reader)]

    candidate_pattern = '/CommitteeDetailCandidates.aspx?id=%s'
    cand_scraper = CandidateScraper(url_pattern=candidate_pattern)
    for comm_id in comm_ids:
        for cand in cand_scraper.scrape_one(comm_id):
            print cand
