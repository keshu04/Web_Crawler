# crawler.py

import threading
import queue
import time
import requests
import os
import csv
from collections import defaultdict
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

# --- CONSTANTS AND CONFIGURATION ---

# Crawler control: Exit when page limit is reached and active threads are below this number
_DEL_STOP_AT = 5

# Color codes for console output
RED = "\033[31m"
CYAN = "\033[36m"
GREEN = "\033[32m"
BLUE = "\033[34m"
C_END = "\033[0m"

class Crawler:
    """
    A multi-threaded web crawler that discovers web pages, extracts links,
    and generates a graph of domain relationships for ranking.
    """
    def __init__(self, max_links, pages_limit, max_threads):
        # Parameters
        self.max_links = max_links
        self.pages_limit = pages_limit
        self.max_threads = max_threads

        # Thread-safe data structures and counters
        self.link_queue = queue.Queue()
        self.discovered_sites = set()
        self.page_rank = defaultdict(set)
        self.working_threads = 0
        self.total_visited_pages = 0
        self.pages_limit_reached = False

        # Synchronization primitives
        self.data_lock = threading.Lock()  # Protects counters, discovered_sites, page_rank
        self.timing_lock = threading.Lock()
        self.condition = threading.Condition()  # For sleep/awake mechanism in the main loop

        # Logging and performance metrics
        self.log_file = open("logs.txt", "w")
        self.thread_timings = []

    def log(self, message):
        """Writes a message to the log file."""
        self.log_file.write(message + "\n")
        self.log_file.flush()

    def initialize(self):
        """Initializes the crawler by creating necessary directories and loading seed URLs."""
        self.log("Crawler initialized")
        os.makedirs("OUTPUT", exist_ok=True)
        
        try:
            with open("initialLinks.txt", "r") as f:
                next(f)  # Skip the count line
                for line in f:
                    url = line.strip()
                    if url:
                        self.link_queue.put(url)
        except FileNotFoundError:
            print(f"{RED}Error: 'initialLinks.txt' not found. Using default URL.{C_END}")
            self.link_queue.put("https://www.google.com")

    @staticmethod
    def get_domain(url):
        """Extracts the network location (domain) from a URL."""
        try:
            return urlparse(url).netloc
        except Exception:
            return ""

    def downloader(self, url):
        """Downloads the HTML content of a given URL."""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; PythonCrawler/1.0)'}
            response = requests.get(url, timeout=5, headers=headers)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response.text
        except requests.RequestException as e:
            print(f"{RED}Error downloading {url}: {e}{C_END}")
            return ""

    def get_links(self, html, base_url):
        """Parses HTML to extract up to `max_links` absolute URLs."""
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        for a_tag in soup.find_all('a', href=True):
            if len(links) >= self.max_links:
                break
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)
            if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc:
                clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}".strip('/')
                links.add(clean_url)
        return links

    def child_thread(self, url, th_no):
        """The target function for each worker thread."""
        # 1. Download
        t1 = time.perf_counter()
        html = self.downloader(url)
        d_time = (time.perf_counter() - t1) * 1_000_000  # microseconds
        if html:
            print(f"{CYAN}Thread {th_no} downloaded page.{C_END}")

        # 2. Parse
        t1 = time.perf_counter()
        linked_sites = self.get_links(html, url)
        p_time = (time.perf_counter() - t1) * 1_000_000
        print(f"{CYAN}Thread {th_no} extracted {len(linked_sites)} links.{C_END}")

        # 3. Update shared state
        t1 = time.perf_counter()
        curr_domain = self.get_domain(url)
        if curr_domain:
            with self.data_lock:
                for link in linked_sites:
                    if link not in self.discovered_sites:
                        self.discovered_sites.add(link)
                        self.link_queue.put(link)
                        link_domain = self.get_domain(link)
                        if link_domain:
                            self.page_rank[curr_domain].add(link_domain)
        u_time = (time.perf_counter() - t1) * 1_000_000
        print(f"{CYAN}Thread {th_no} updated shared variables.{C_END}")

        with self.timing_lock:
            self.thread_timings.append([d_time, p_time, u_time])

        # Notify main thread of completion
        with self.condition:
            with self.data_lock:
                self.working_threads -= 1
            print(f"{BLUE}Thread {th_no} finished, total working: {self.working_threads}{C_END}")
            self.condition.notify()

    def create_thread(self):
        """Creates and starts a new worker thread."""
        with self.data_lock:
            if self.link_queue.empty():
                return
            current_site = self.link_queue.get()
            self.total_visited_pages += 1
            th_no = self.total_visited_pages
            self.working_threads += 1

            self.log(current_site)
            print(f"{GREEN}Creating a thread for {current_site}, total: {self.working_threads}{C_END}")

            if self.total_visited_pages >= self.pages_limit:
                self.pages_limit_reached = True
                print(f"{RED}~!!! Page Limit Reached Here !!!~{C_END}")

        thread = threading.Thread(target=self.child_thread, args=(current_site, th_no))
        thread.daemon = True
        thread.start()

    def run_crawler(self):
        """The main control loop for the crawler."""
        while True:
            with self.condition:
                with self.data_lock:
                    limit_hit = self.pages_limit_reached
                    threads_working = self.working_threads
                    queue_has_links = not self.link_queue.empty()

                if limit_hit:
                    if threads_working < _DEL_STOP_AT:
                        print(f"{RED}Exiting: Page limit reached and active threads are below threshold.{C_END}")
                        break
                    else:
                        print("Going to sleep (page limit reached, waiting for threads to finish)...")
                        self.condition.wait()
                        print("Awakened.")
                else:
                    if threads_working < self.max_threads and queue_has_links:
                        self.create_thread()
                    elif threads_working == 0 and not queue_has_links:
                        print("Exiting: No more links in queue and no active threads.")
                        break
                    else:
                        print("Going to sleep (max threads reached or queue is empty)...")
                        self.condition.wait()
                        print("Awakened.")
        self.log("Crawling completed.")

    def show_results(self):
        """Writes the collected data to CSV files and prints a summary."""
        with open("OUTPUT/th_timings.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(self.thread_timings)

        with open("OUTPUT/pagerank.csv", "w", newline="") as f:
            writer = csv.writer(f)
            for domain, linked_domains in self.page_rank.items():
                writer.writerow([domain] + list(linked_domains))

        dashline = "-----------------------------------------------------"
        print("\nCrawl Summary:")
        print(dashline)
        print(f"{'Max Links/Page:':<30}{self.max_links}")
        print(f"{'Page Download Limit:':<30}{self.pages_limit}")
        print(f"{'Max Concurrent Threads:':<30}{self.max_threads}")
        print(f"{'Total Pages Visited:':<30}{self.total_visited_pages}")
        print(dashline)

    def close(self):
        """Closes any open resources, like the log file."""
        self.log(f"Final queue size: {self.link_queue.qsize()}")
        self.log_file.close()