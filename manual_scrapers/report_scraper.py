from il_elex_scraper import IllinoisElectionScraper
from urlparse import parse_qs, urlparse

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

