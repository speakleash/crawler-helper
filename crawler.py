import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
from url_filterer import UrlFilterer
import random

class Crawler:
    def __init__(self, start_url, proxies=None, max_workers=8):
        self.start_url = start_url
        self.visited_urls = set()
        self.start_domain = urlparse(start_url).netloc

        # UrlFilterer instance
        self.url_filterer = UrlFilterer(
            allowed_domains=self.start_domain,
            allowed_schemes={"http", "https"},
            allowed_filetypes={".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cgi", ""},
            restricted_urls=[
                "web.archive.org", "plugins",
                ":8080", "moodle", "kalendarz",
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
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.proxies = proxies or ['']

    def get_random_proxy(self):
        proxy = random.choice(self.proxies)
        return {"http": proxy, "https": proxy} if proxy else {}


    def crawl(self, url):
        url = self.clean_url(url)

        if url in self.visited_urls or not self.url_filterer.filter_url(self.start_url, url):
            return

        self.visited_urls.add(url)

        try:
            response = self.session.get(url, proxies=self.get_random_proxy())

            if response.status_code == 429 and 'Retry-After' in response.headers:
                retry_after = int(response.headers['Retry-After'])
                print(f"Retry after {response.headers['Retry-After']} encountered. WAITING...")
                time.sleep(retry_after)
                response = self.session.get(url, proxies=self.get_random_proxy())

            soup = BeautifulSoup(response.text, 'lxml')

            yield url, response

            futures = []
            for link in soup.find_all('a'):
                href = link.get('href')
                absolute_url = self.clean_url(urljoin(url, href, allow_fragments=False))
                if self.url_filterer.filter_url(self.start_url, absolute_url):
                    if absolute_url in self.visited_urls:
                        continue
                    futures.append(self.executor.submit(self.crawl, absolute_url))

            for future in concurrent.futures.as_completed(futures):
                try:
                    yield from future.result()
                except Exception as e:
                    print(f"Exception in task handling: {e}")

        except Exception as e:
            print(f"Exception while crawling URLs occurred: {e}")

    def start(self):
        yield from self.crawl(self.start_url)

    def clean_url(self, url):
        url = url.strip()
        if url.endswith('/'):
            url = url[:-1]
        return url

