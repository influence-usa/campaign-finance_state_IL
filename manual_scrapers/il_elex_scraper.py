import scrapelib
import lxml.html

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
                if blank_pages > 50 :
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

