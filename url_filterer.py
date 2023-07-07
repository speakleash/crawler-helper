import urllib
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import pathlib
import socket
import httpx

class UrlFilterer:
    def __init__(
        self,
        allowed_domains: set[str] | None = None,
        allowed_schemes: set[str] | None = None,
        allowed_filetypes: set[str] | None = None,
        restricted_urls: set[str] | None = None,
    ):
        self.allowed_domains = allowed_domains
        self.allowed_schemes = allowed_schemes
        self.allowed_filetypes = allowed_filetypes
        self.restricted_urls = restricted_urls
        self.robots_parsers = {}
        self.login_patterns = ["login", "signin", "auth", "logon", "signon", "logowanie", "rejestracja"]

    def filter_url(self, base: str, url: str) -> str | None:
        parsed_url = urlparse(url)

        # Join relative URL with the base URL
        if not parsed_url.netloc:
            base_parsed = urlparse(base)
            url = urljoin(base, url)

        base_count = url.count(base)
        # If the base URL is present more than once, remove it
        if base_count > 1:
            url = url.replace(base, '', 1)

        url, _ = urllib.parse.urldefrag(url)
        parsed = urlparse(url)
        segments = parsed.path.split('/')

        # Remove duplicates
        unique_segments = []
        seen_segments = set()
        for segment in segments:
            if segment not in seen_segments:
                unique_segments.append(segment)
                seen_segments.add(segment)

        # Reconstruct the URL
        parsed = parsed._replace(path='/'.join(unique_segments))
        url = urllib.parse.urlunparse(parsed)

        
        if any(pattern in url.lower() for pattern in self.login_patterns):
            return None

        if self.allowed_schemes is not None and parsed.scheme not in self.allowed_schemes:
            return None

        if self.restricted_urls is not None and any(substring in url for substring in self.restricted_urls):
            return None

        if self.allowed_domains is not None and not self._is_domain_allowed(url):
            return None

        if not self._is_allowed_by_robots(url):
            print("***ROBOTS NOT ALLOWED***")
            return None

        ext = pathlib.Path(parsed.path).suffix
        if self.allowed_filetypes is not None and ext not in self.allowed_filetypes:
            return None

        return url

    def _is_domain_allowed(self, url: str) -> bool:
        parsed_url = urlparse(url)
        return parsed_url.netloc in self.allowed_domains

    def _is_allowed_by_robots(self, url: str) -> bool:
        parsed_url = urlparse(url)
        base_url = parsed_url.scheme + '://' + parsed_url.netloc

        if base_url not in self.robots_parsers:
            rp = RobotFileParser()
            try:
                rp.set_url(urljoin(base_url, '/robots.txt'))
                rp.read()
                self.robots_parsers[base_url] = rp
            except (httpx.RequestError, urllib.error.URLError, socket.gaierror):
                print("***Error fetching robots.txt***")
                # Handle exceptions when retrieving robots.txt
                return False  # Allow crawling if unable to retrieve robots.txt
        else:
            rp = self.robots_parsers[base_url]

        return rp.can_fetch('*', url)