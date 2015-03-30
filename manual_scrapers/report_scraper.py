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
    from csvkit.unicsv import UnicodeCSVDictWriter, UnicodeCSVDictReader, UnicodeCSVWriter
    from csvkit.table import Table
    from csvkit.sql import make_table, make_create_table_statement
    from itertools import groupby
    import scrapelib
    import sqlite3
    from dateutil import parser
    
    cache_dir = '/cache/cache'

    AWS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET_KEY']
    DB_NAME = os.path.join(cache_dir, 'reports.db')

    comm_pattern = 'http://www.elections.state.il.us/CampaignDisclosure/CommitteeDetail.aspx?id=%s'
    inp = StringIO()
    s3_conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = s3_conn.get_bucket('il-elections')
    k = Key(bucket)
    k.key = 'Committees.csv'
    committee_file = k.get_contents_to_file(inp)
    inp.seek(0)
    reader = UnicodeCSVDictReader(inp)
    comm_urls = [comm_pattern % c['id'] for c in list(reader)]

    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
    
    report_pattern = '/CommitteeDetail.aspx?id=%s&pageindex=%s'
    report_scraper = ReportScraper(url_pattern=report_pattern)
    report_scraper.cache_storage = scrapelib.cache.FileCache(cache_dir)
    report_scraper.cache_write_only = False
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    for comm_url in comm_urls:
        for report_data in report_scraper.scrape_one(comm_url):
            comm_id = parse_qs(urlparse(comm_url).query)['id'][0]
            report_data['committee_id'] = comm_id
            outp = StringIO()
            writer = UnicodeCSVDictWriter(outp, fieldnames=report_data.keys())
            writer.writeheader()
            writer.writerow(report_data)
            outp.seek(0)
            t = Table.from_csv(outp, name='reports')
            sql_table = make_table(t)
            try:
                c.execute('select * from reports limit 1')
            except sqlite3.OperationalError:
                create_st = make_create_table_statement(sql_table)
                c.execute(create_st)
                conn.commit()
            c.execute('select * from reports where id = ?', (int(report_data['id']),))
            existing = c.fetchall()
            if not existing:
                insert = sql_table.insert()
                headers = t.headers()
                rows = [dict(zip(headers, row)) for row in t.to_rows()]
                for row in rows:
                    c.execute(str(insert), row)
                conn.commit()
            else:
                print 'Already saved report %s' % report_data['detail_url']
    c.execute('select date_filed from reports order by date_filed limit 1')
    oldest_year = parser.parse(c.fetchone()[0]).year
    c.execute('select date_filed from reports order by date_filed desc limit 1')
    newest_year = parser.parse(c.fetchone()[0]).year
    c.execute('select * from reports limit 1')
    header = list(map(lambda x: x[0], c.description))
    for year in range(oldest_year, newest_year + 1):
        oldest_date = '%s-01-01' % year
        newest_date = '%s-12-31' % year
        c.execute('select * from reports where date_filed >= ? and date_filed <= ?', (oldest_date, newest_date))
        rows = c.fetchall()
        outp = StringIO()
        writer = UnicodeCSVWriter(outp)
        writer.writerow(header)
        writer.writerows(rows)
        outp.seek(0)
        k.key = 'Reports/%s.csv' % year
        k.set_contents_from_file(outp)
        k.make_public()

