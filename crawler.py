import aiohttp
import asyncio
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin
import re

# MongoDB setup
client = MongoClient("mongodb+srv://owwix:finalproject@cluster0.n1zt8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["cppbusinesscrawl"]
indexed_collection = db["faculty_info"]

# Logging setup
import logging
logging.basicConfig(filename='crawler_errors.log', level=logging.ERROR)

class Frontier:
    def __init__(self, seed_urls):
        self.to_visit = set(seed_urls)
        self.visited = set()

    def done(self):
        return not self.to_visit

    def nextURL(self):
        return self.to_visit.pop()

    def addURL(self, url):
        if url not in self.visited and url not in self.to_visit:
            self.to_visit.add(url)

async def retrieveURL(session, url):
    if "cpp.edu" not in url:
        print(f"Skipping non-cpp.edu URL: {url}")
        return None

    try:
        async with session.get(url, timeout=5) as response:
            print(f"URL retrieved: {url}")
            return await response.text()
    except Exception as e:
        logging.error(f"Failed to retrieve {url}: {e}")
        return None

def parse(html):
    return BeautifulSoup(html, 'html.parser')

def target_page(soup):
    div = soup.find("div", class_="span10")
    if div:
        title_dept = div.find("span", class_="title-dept")
        if title_dept:
            em_tag = title_dept.find("em")
            if em_tag:
                regex = r".*Professor.*,\s*International Business\s*(?:&|and|)?\s*Marketing\s*(?:Department)?,\s*College of Business Administration.*"
                em_text = em_tag.get_text().strip()
                if re.match(regex, em_text):
                    return True
    return False

def parse_faculty_content(soup):
    content_div = soup.find("div", class_="section-menu")
    if content_div:
        return content_div.get_text(strip=True)
    return None

async def crawl_and_store(frontier, num_targets, session):
    targets_found = 0
    while not frontier.done():
        url = frontier.nextURL()
        html = await retrieveURL(session, url)
        if not html:
            continue

        soup = parse(html)
        frontier.visited.add(url)

        if target_page(soup):
            print(f"{targets_found + 1} TARGET PAGE DETECTED {url}")
            targets_found += 1
            content = parse_faculty_content(soup)
            if content:
                indexed_collection.insert_one({"url": url, "content": content})
                print(f"Indexed: {url}")

            if targets_found == num_targets:
                print("Target limit reached, stopping crawl.")
                break

        for link in soup.find_all("a", href=True):
            full_url = urljoin(url, link["href"])
            frontier.addURL(full_url)

async def main():
    department_urls = ["https://www.cpp.edu/cba/international-business-marketing/faculty-staff/index.shtml"]
    frontier = Frontier(department_urls)

    # Open aiohttp session and run the crawler concurrently
    async with aiohttp.ClientSession() as session:
        await crawl_and_store(frontier, num_targets=10, session=session)

if __name__ == "__main__":
    asyncio.run(main())