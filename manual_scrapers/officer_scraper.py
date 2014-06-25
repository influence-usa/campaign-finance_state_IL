from il_elex_scraper import IllinoisElectionScraper

class OfficerScraper(IllinoisElectionScraper):
    
    def scrape_one(self, comm_id):
        url = self.url_pattern % comm_id
        print url
        officer_page = self._lxmlize(url)
        rows = officer_page.xpath('//tr[starts-with(@class, "SearchListTableRow")]')
        for row in rows:
            officer_data = {}
            id_sel = 'ctl00_ContentPlaceHolder1_OfficerNameRow'
            officer_id = row.xpath('th[starts-with(@id, "%s")]' % id_sel)[0].attrib['id']
            officer_data['id'] = officer_id.replace(id_sel, '')
            officer_data['name'] = ' '.join(row.find('td[@class="tdOfficerName"]/').xpath('.//text()'))
            officer_data['title'] = ' '.join(row.find('td[@class="tdOfficerTitle"]/').xpath('.//text()'))
            officer_data['address'] = ' '.join(row.find('td[@class="tdOfficerAddress"]/').xpath('.//text()'))
            yield officer_data

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
    
    # Now scrape Officer pages
    officer_pattern = '/CommitteeDetailOfficers.aspx?id=%s'
    officer_scraper = OfficerScraper(url_pattern=officer_pattern)
    # officer_scraper.cache_storage = scrapelib.cache.FileCache('cache')
    # officer_scraper.cache_write_only = False
    officer_header = ['id', 'committee_id', 'name', 'title', 'address']
    officer_outp = StringIO()
    officer_writer = UnicodeCSVDictWriter(officer_outp, officer_header, delimiter='\t')
    officer_writer.writeheader()
    officers = []
    for comm_id in comm_ids:
        for officer in officer_scraper.scrape_one(comm_id):
            officer['committee_id'] = comm_id
            officers.append(officer)
    officer_writer.writerows(officers)
    k.key = 'Officers.tsv'
    k.set_contents_from_string(officer_outp.getvalue())
    k.make_public()

