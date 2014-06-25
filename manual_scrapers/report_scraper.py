from il_elex_scraper import IllinoisElectionScraper
from urlparse import parse_qs, urlparse
from datetime import date, datetime
from collections import OrderedDict

class ReportScraper(IllinoisElectionScraper):
    
    def scrape_one(self, url):
        start_page = self._lxmlize(url)
        committee_id = parse_qs(urlparse(url).query)['id'][0]
        print url
        for page in self._grok_pages(start_page, committee_id):
            if page is not None:
                reports = page.xpath("//tr[starts-with(@class, 'SearchListTableRow')]")
                for report in reports:
                    report_data = OrderedDict((
                        ('id', ''),
                        ('committee_id', ''),
                        ('period_from', ''),
                        ('period_to', ''),
                        ('date_filed', ''),
                        ('type', ''),
                        ('generic_type', ''),
                        ('funds_start', ''),
                        ('funds_end', ''),
                        ('receipts', ''),
                        ('expenditures', ''),
                        ('invest_total', ''),
                        ('detail_url', ''),
                    ))
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

dthandler = lambda obj: obj.isoformat() \
    if isinstance(obj, datetime) or isinstance(obj, date) else None

if __name__ == "__main__":
    from cStringIO import StringIO
    import os
    import json
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
    from csvkit.unicsv import UnicodeCSVDictWriter, UnicodeCSVDictReader
    from itertools import groupby
    import scrapelib

    AWS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET_KEY']
    
    comm_pattern = 'http://www.elections.state.il.us/CampaignDisclosure/CommitteeDetail.aspx?id=%s'
    inp = StringIO()
    s3_conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = s3_conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = 'Committees.tsv'
    committee_file = k.get_contents_to_file(inp)
    inp.seek(0)
    reader = UnicodeCSVDictReader(inp, delimiter='\t')
    comm_urls = [comm_pattern % c['id'] for c in list(reader)]

    # comm_urls = [comm_pattern % i for i in range(1,5)]
    
    report_pattern = '/CommitteeDetail.aspx?id=%s&pageindex=%s'
    report_scraper = ReportScraper(url_pattern=report_pattern)
    # report_scraper.cache_storage = scrapelib.cache.FileCache('cache')
    # report_scraper.cache_write_only = False
    reports = []
    for comm_url in comm_urls:
        for report_data in report_scraper.scrape_one(comm_url):
            comm_id = parse_qs(urlparse(comm_url).query)['id'][0]
            report_data['committee_id'] = comm_id
            reports.append(report_data)
    reports = sorted(reports, key=lambda x: x['date_filed'].year)
    all_reports = {}
    for dt,group in groupby(reports, key=lambda x: x['date_filed'].year):
        gr_list = list(group)
        for gr in gr_list:
            for row_k,row_v in gr.items():
                if isinstance(row_v, date) or isinstance(row_v, datetime):
                    gr[row_k] = row_v.isoformat()
        all_reports[dt] = gr_list
    for dt, group in all_reports.items():
        outp = StringIO()
        writer = UnicodeCSVDictWriter(outp, fieldnames=group[0].keys())
        writer.writeheader()
        writer.writerows(group)
        outp.seek(0)
        k.key = 'Reports/%s.csv' % dt
        k.set_contents_from_file(outp)
        k.make_public()

