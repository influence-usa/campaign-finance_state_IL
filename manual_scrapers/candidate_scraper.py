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
            data['address'] = row.xpath('td[contains(@class, "tdCandidateAddress")]')[0].text_content()
            party = row.xpath('td[contains(@class, "tdParty")]')
            if party:
                data['party'] = party[0].text_content()
            office = row.xpath('td[contains(@class, "tdOffice")]')
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
    from csvkit.unicsv import UnicodeCSVDictWriter

    AWS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET_KEY']
    candidate_pattern = '/CommitteeDetailCandidates.aspx?id=%s'
    cand_scraper = CandidateScraper(url_pattern=candidate_pattern)
    for cand in cand_scraper.scrape_all():
        print cand
