import scrapelib
from urllib import urlencode
import lxml.html
from urlparse import parse_qs, urlparse
from datetime import date, datetime

class IllinoisElectionScraper(scrapelib.Scraper):
    def __init__(self,
                 raise_errors=True,
                 requests_per_minute=60,
                 follow_robots=True,
                 retry_attempts=10,
                 retry_wait_seconds=5,
                 header_func=None,
                 url_pattern=None,
                 string_on_page=None):
        self.base_url = 'http://www.elections.state.il.us/CampaignDisclosure'
        self.url_pattern = self.base_url + url_pattern
        self.string_on_page = string_on_page
        super(IllinoisElectionScraper, self).__init__(raise_errors,
                                            requests_per_minute,
                                            follow_robots,
                                            retry_attempts,
                                            retry_wait_seconds,
                                            header_func)

    def scrape_all(self):
        for url, page in self._generate_pages():
            yield self.scrape_one(url, page)

    def scrape_one(self):
        """ This must be implemented by sub-classes"""
        pass
    
    def _lxmlize(self, url, payload=None):
        if payload :
            entry = self.urlopen(url, 'POST', payload)
        else :
            entry = self.urlopen(url)
        page = lxml.html.fromstring(entry)
        page.make_links_absolute(url)
        return page
 
    def _generate_pages(self):
        blank_pages = 0
        id = 1
        last = False
        while not last:
            url = self.url_pattern % id
            response = self._lxmlize(url)
 
            if self.string_on_page in lxml.html.tostring(response) :
                blank_pages = 0
                yield url, response
            else :
                blank_pages += 1
                if blank_pages > 3000 :
                    last = True
 
            id += 1
    
    def _grok_pages(self, start_page, id):
        page_counter = start_page.xpath("//span[@id='ctl00_ContentPlaceHolder1_lbRecordsInfo']")[0].text
        if page_counter:
            page_count = (int(page_counter.split(' ')[-1]) / 15) + 1
            if page_count > 1:
                for page_num in range(page_count):
                    url = self.url_pattern % (id, page_num)
                    yield self._lxmlize(url)
            else:
                yield start_page
        else:
            yield start_page

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
            ids = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblCommitteeIDs"]/text()')
            for i in ids:
                if 'State' in i:
                    data['state_id'] = i.split(' ')[1]
                elif 'Local' in i:
                    data['local_id'] = i.split(' ')[1]
            data['type'] = page.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblTypeOfCommittee"]')[0].text
        return data

class ReportScraper(IllinoisElectionScraper):
    
    def scrape_one(self, url):
        start_page = self._lxmlize(url)
        committee_id = parse_qs(urlparse(url).query)['id'][0]
        print url
        for page in self._grok_pages(start_page, committee_id):
            if page is not None:
                reports = page.xpath("//tr[starts-with(@class, 'SearchListTableRow')]")
                for report in reports:
                    report_data = {}
                    id_sel = 'ctl00_ContentPlaceHolder1_FiledDocRow'
                    report_id = report.xpath('th[starts-with(@id, "%s")]' % id_sel)[0].attrib['id']
                    report_data['id'] = report_id.replace(id_sel, '')
                    raw_period = report.find("td[@headers='ctl00_ContentPlaceHolder1_thReportingPeriod']/span").xpath('.//text()')
                    if raw_period:
                        report_data['period_from'], report_data['period_to'] = self._parse_period(raw_period)
                    date_filed = report.find("td[@headers='ctl00_ContentPlaceHolder1_thFiled']/span").text
                    date, time, am_pm = date_filed.split(' ')
                    date = '/'.join([d.zfill(2) for d in date.split('/')])
                    time = ':'.join([t.zfill(2) for t in time.split(':')])
                    report_data['date_filed'] = datetime.strptime(' '.join([date, time, am_pm]), '%m/%d/%Y %I:%M:%S %p')
                    detailed = report.find("td[@headers='ctl00_ContentPlaceHolder1_thReportType']/a")
                    if detailed is not None:
                        report_data['type'] = detailed.text
                        report_data['generic_type'] = detailed.text.replace(' (Amendment)', '')
                        detail_url = detailed.attrib['href']
                        report_data['detail_url'] = detail_url
                        qs = parse_qs(urlparse(detail_url).query)
                        report_detail = self._lxmlize(detail_url)
                        funds_start = report_detail.xpath("//span[@id='ctl00_ContentPlaceHolder1_lblBegFundsAvail']")
                        # TODO: Looks like there are Pre-Election reports are formatted differently
                        # Need to be able to get funding data from all report formats
                        if funds_start:
                            report_data['funds_start'] = self._clean_float(funds_start[0].text)
                        funds_end = report_detail.xpath("//span[@id='ctl00_ContentPlaceHolder1_lblEndFundsAvail']")
                        if funds_end:
                            report_data['funds_end'] = self._clean_float(funds_end[0].text)
                        expenditures = report_detail.xpath("//span[@id='ctl00_ContentPlaceHolder1_lblTotalExpendTot']")
                        if expenditures:
                            report_data['expenditures'] = self._clean_float(expenditures[0].text)
                        receipts = report_detail.xpath("//span[@id='ctl00_ContentPlaceHolder1_lblTotalReceiptsTot']")
                        if receipts:
                            report_data['receipts'] = self._clean_float(receipts[0].text)
                        invest_total = report_detail.xpath("//span[@id='ctl00_ContentPlaceHolder1_lblTotalInvest']")
                        if receipts:
                            report_data['invest_total'] = self._clean_float(invest_total[0].text)
                    # For now skipping reports with no details page
                    else:
                        detailed = report.find("td[@headers='ctl00_ContentPlaceHolder1_thReportType']/span")
                        raw_period = report.find("td[@headers='ctl00_ContentPlaceHolder1_thReportingPeriod']/span").text
                        if raw_period:
                            report_data['period_from'], report_data['period_to'] = self._parse_period(raw_period)
                        report_data['type'] = detailed.text
                    yield report_data
    
    def _clean_float(self, num):
       num = num.replace(',', '').replace('$', '') 
       if num.find('(') is 0:
           num = '-' + num.replace('(', '').replace(')', '')
       return float(num)

    def _parse_period(self, raw_period):
        if len(raw_period) > 1:
            raw_period = raw_period[1]
        else:
            raw_period = raw_period[0]
        period = raw_period.split(' to ')
        period_to = None
        period_from = None
        if len(period) > 1:
            f_month, f_day, f_year = period[0].strip().split('/')
            t_month, t_day, t_year = period[1].strip().split('/')
            period_from = date(int(f_year), int(f_month), int(f_day))
            period_to = date(int(t_year), int(t_month), int(t_day))
        else:
            year = raw_period.strip().split(' ')[0]
            try:
                period_from = date(int(year), 1, 1)
            except ValueError:
                pass
        return period_from, period_to

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

    url_pattern = '/CommitteeDetail.aspx?id=%s'
    string_on_page = 'ctl00_ContentPlaceHolder1_CommitteeResultsLayout'
    comm_scraper = CommitteeScraper(url_pattern=url_pattern, string_on_page=string_on_page)
    # comm_scraper.cache_storage = scrapelib.cache.FileCache('cache')
    # comm_scraper.cache_write_only = False
    committees = []
    comms_outp = StringIO()
    comm_header = ['id', 'name', 'type', 'url', 'address', 'status', 'purpose', 'state_id', 'local_id']
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

    # Now scrape Officer pages
    officer_pattern = '/CommitteeDetailOfficers.aspx?id=%s'
    officer_scraper = OfficerScraper(url_pattern=officer_pattern)
    # officer_scraper.cache_storage = scrapelib.cache.FileCache('cache')
    # officer_scraper.cache_write_only = False
    officer_header = ['id', 'committee_id', 'name', 'title', 'address']
    officer_outp = StringIO()
    officer_writer = UnicodeCSVDictWriter(officer_outp, officer_header, delimiter='\t')
    officer_writer.writeheader()
    comm_ids = [c['id'] for c in committees]
    officers = []
    for comm_id in comm_ids:
        for officer in officer_scraper.scrape_one(comm_id):
            officer['committee_id'] = comm_id
            officers.append(officer)
    officer_writer.writerows(officers)
    k.key = 'Officers.tsv'
    k.set_contents_from_string(officer_outp.getvalue())
    k.make_public()



