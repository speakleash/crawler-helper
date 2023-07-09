import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from multiprocessing import Pool, Manager, freeze_support
import time
from url_filterer import UrlFilterer
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class Crawler:
    def __init__(self, start_url, proxies=None, max_workers=4):
        self.start_url = start_url
        self.start_domain = urlparse(start_url).netloc

        # UrlFilterer instance
        self.url_filterer = UrlFilterer(
            allowed_domains=self.start_domain,
            allowed_schemes={"http", "https"},
            allowed_filetypes={".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cgi", ""},
            restricted_urls=[
			"web.archive.org", "plugins", ":8080", "moodle", "kalendarz",
			"password", "mobile", "query", "sort=", "calendar", "css", "view",
			"ajax", "Zaloguj", "reddit.", "search?", "source=", "rozmiar=",
			"ssid=", "f_ov", "Facebook=", "cookies", "add", "cart", "comment",
			"reply", "en_US", "/login", "/logowanie", "producer_", "register",
			"orderby", "tumblr.", "redirect", "linkedin.", "facebook.",
			"instagram.", "youtube.", "twitter.", "whatsapp.", "pinterest.",
			"login.", "google.", "wykop.", "/en/", "kalendarz-", "filtr,", "kalendarium/",
			"month,", "export,", "wydarzenia/dzien/", "dzien/"
		]
        )
        self.headers = {
            'User-Agent': 'Speakleash-v0.1',
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive"
        }
        self.proxies = proxies or ['']
        self.max_workers = max_workers
    
     
    
    def start(self):
        visited_urls = Manager().list()


        with Pool(initializer=Crawler.initialize_worker, processes=self.max_workers) as pool:
            backlog = Manager().list([self.start_url])
            while backlog:
                args_list = [(url, visited_urls, self.start_url, self.url_filterer, self.proxies) for url in backlog]
                results = pool.imap(Crawler.crawl, args_list)
                
                for url, response, new_urls in results:
                    if response is not None:
                        if url is not None:
                            yield url, response
                    backlog.extend(new_urls)
                    try:
                        backlog.remove(url)
                    except:
                        print("ERROR removing"+url)

    @staticmethod
    def initialize_worker():
        global session
        session = requests.Session()
        headers = {
            'User-Agent': 'Speakleash-v0.1',
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive"
        }
        session.headers.update(headers)
        retries = Retry(total=2, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        
    @staticmethod
    def get_random_proxy(proxies):
        proxy = random.choice(proxies)
        return {"http": proxy, "https": proxy} if proxy else {}

    @staticmethod
    def crawl(args):
        url, visited_urls, start_url, url_filterer, proxies = args

        url = Crawler.clean_url(url)
       

        if url in visited_urls or not url_filterer.filter_url(start_url, url):
            if not url_filterer.filter_url(start_url, url):
                visited_urls.append(url)
            return url, None, []  # Return None values when the URL is not valid
        
        visited_urls.append(url)

        try:
            session.proxies=Crawler.get_random_proxy(proxies)
            with session.get(url, timeout=10) as response:

                if response.status_code == 429 and 'Retry-After' in response.headers:
                    retry_after = int(response.headers['Retry-After'])
                    print(f"Retry after {response.headers['Retry-After']} encountered. WAITING...")
                    time.sleep(retry_after)
                    response = session.get(url, timeout=10)

            soup = BeautifulSoup(response.text, 'lxml')
            found_urls = []

            for link in soup.find_all('a'):
                href = link.get('href')
                absolute_url = Crawler.clean_url(urljoin(url, href, allow_fragments=False))
                if url_filterer.filter_url(start_url, absolute_url):
                    if absolute_url not in visited_urls:
                        found_urls.append(Crawler.clean_url(absolute_url))
                            

            return url, response, found_urls

        except Exception as e:
            print(f"Exception while crawling URLs occurred: {e} url: {url}")
            return url, None, []
        
    @staticmethod
    def clean_url(url):
        url = url.strip()
        if url.endswith('/'):
            url = url[:-1]
        return url



        


