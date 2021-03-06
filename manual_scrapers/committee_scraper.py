import scrapelib
from urllib import urlencode
import lxml.html
from urlparse import parse_qs, urlparse
from datetime import date, datetime
from il_elex_scraper import IllinoisElectionScraper

class CommitteeScraper(IllinoisElectionScraper):

    def scrape_some(self, start_id, end_id):
        blank_pages = 0
        id = start_id
        last = False
        while not last and id <= end_id:
            url = self.url_pattern % id
            response = self._lxmlize(url)
            if self.string_on_page in lxml.html.tostring(response):
                blank_pages = 0
                yield self.scrape_one(url, response)
            else:
                blank_pages += 1
                if blank_pages > 50:
                    last = True
            id += 1

    def scrape_one(self, url, page):
        data = None
        print url
        if page:
            data = {}
            data['name'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblName"]')[0].text
            data['id'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblCommitteeID"]')[0].text.split(' ')[-1]
            data['url'] = self.url_pattern % data['id']
            add_el = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblAddress"]')[0]
            address = ' '.join([a for a in [add_el.text, add_el.tail] if a is not None]).strip()
            csz_el = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblCityStateZip"]')[0]
            city_st_zip = ' '.join([c for c in [csz_el.text, csz_el.tail] if c is not None]).strip()
            data['address'] = '%s %s' % (address.strip(), city_st_zip.strip())
            data['status'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblStatus"]')[0].text
            data['purpose'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblPurpose"]')[0].text
            data['creation_date'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblCreationDate"]')[0].text
            ids = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblCommitteeIDs"]/text()')
            for i in ids:
                if 'State' in i:
                    data['state_id'] = i.split(' ')[1]
                elif 'Local' in i:
                    data['local_id'] = i.split(' ')[1]
            data['type'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblTypeOfCommittee"]')[0].text
        return data

if __name__ == "__main__":
    from cStringIO import StringIO
    import os
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
    from csvkit.unicsv import UnicodeCSVDictWriter

    AWS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET_KEY']

    url_pattern = '/CommitteeDetail.aspx?id=%s'
    string_on_page = 'ctl00_ContentPlaceHolder1_CommitteeResultsLayout'
    comm_scraper = CommitteeScraper(url_pattern=url_pattern, string_on_page=string_on_page)
    # comm_scraper.cache_storage = scrapelib.cache.FileCache('cache')
    # comm_scraper.cache_write_only = False
    committees = []
    comms_outp = StringIO()
    comm_header = ['id', 'name', 'type', 'url', 'address', 'status', 'purpose', 'state_id', 'local_id', 'creation_date']
    comm_writer = UnicodeCSVDictWriter(comms_outp, comm_header, delimiter='\t')
    comm_writer.writeheader()
    for committee in comm_scraper.scrape_all():
        # Save to DB and maybe write as JSON?
        committees.append(committee)
    comm_writer.writerows(committees)
    s3_conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = s3_conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = 'Committees.tsv'
    k.set_contents_from_string(comms_outp.getvalue())
    k.make_public()

