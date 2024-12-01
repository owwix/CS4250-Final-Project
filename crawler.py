import aiohttp
import asyncio
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import defaultdict
import string
import logging

# MongoDB setup
client = MongoClient("mongodb+srv://owwix:finalproject@cluster0.n1zt8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["cppbusinesscrawl"]
pages_collection = db["pages"]  # Stores pages and content
index_collection = db["inverted_index"]  # Stores inverted index

# Logging setup
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

def preprocess(text):
    # Remove punctuation, lowercase the text, and remove non-alphanumeric characters
    text = re.sub(r"[^\w\s]", "", text)
    return text.lower()

def build_inverted_index(documents, vectorizer):
    """Builds an inverted index using TF-IDF."""
    inverted_index = defaultdict(list)
    tfidf_matrix = vectorizer.fit_transform(documents)
    feature_names = vectorizer.get_feature_names_out()

    for doc_id, doc_vector in enumerate(tfidf_matrix):
        for word_idx in doc_vector.nonzero()[1]:
            word = feature_names[word_idx]
            score = doc_vector[0, word_idx]
            inverted_index[word].append({"doc_id": doc_id, "score": score})

    return inverted_index

async def crawl_and_store(frontier, num_targets, session):
    targets_found = 0
    documents = []  # Text content of pages
    urls = []  # URLs of the crawled pages

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
            content = soup.get_text(separator=" ")
            cleaned_content = preprocess(content)
            documents.append(cleaned_content)
            urls.append(url)

            # Store page content in MongoDB
            pages_collection.insert_one({"url": url, "content": content})
            print(f"Indexed page: {url}")

            if targets_found == num_targets:
                print("Target limit reached, stopping crawl.")
                break

        # Add new links to the frontier
        for link in soup.find_all("a", href=True):
            full_url = urljoin(url, link["href"])
            frontier.addURL(full_url)

    # Build inverted index using TF-IDF
    if documents:
        vectorizer = TfidfVectorizer()
        inverted_index = build_inverted_index(documents, vectorizer)

        # Store inverted index in MongoDB
        for term, postings in inverted_index.items():
            index_collection.insert_one({"term": term, "postings": postings})
        print("Inverted index stored in MongoDB.")
        

async def main():
    department_urls = ["https://www.cpp.edu/cba/international-business-marketing/faculty-staff/index.shtml"]
    frontier = Frontier(department_urls)

    # Open aiohttp session and run the crawler
    async with aiohttp.ClientSession() as session:
        await crawl_and_store(frontier, num_targets=10, session=session)
    
if __name__ == "__main__":
    asyncio.run(main())