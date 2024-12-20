import pymongo
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import math
import re

class QueryEngine:
    def __init__(self, mongo_url="mongodb+srv://owwix:finalproject@cluster0.n1zt8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", db_name="cppbusinesscrawl", collection_name="pages"):
        self.client = pymongo.MongoClient(mongo_url) #enter db here
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.vectorizer = None
        self.tfidf_matrix = None
        self.documents = []
        self.urls = []

    def preprocess_query(self, query):
        query = re.sub(r"[^A-Za-z0-9 ]+", "", query)
        return query.lower()
    
    def load_data(self):
        """
        Load data from the database into memory for TF-IDF processing.
        """
        records = list(self.collection.find({}, {"_id": 0, "url": 1, "content": 1}))
        self.documents = [page["content"] for page in records]
        self.urls = [page["url"] for page in records]

        # Build TF-IDF matrix
        self.vectorizer = TfidfVectorizer()
        self.tfidf_matrix = self.vectorizer.fit_transform(self.documents)


    def query(self, query, page):
        
        if self.tfidf_matrix is None or self.vectorizer is None:
            raise ValueError("TF-IDF data has not been loaded. Call load_data() first.")

        preprocessed_query = self.preprocess_query(query)
        query_vector = self.vectorizer.transform([preprocessed_query])
        similarity_scores = cosine_similarity(query_vector, self.tfidf_matrix).flatten()

        results = self.rank_results(similarity_scores, page)
        return results

    def rank_results(self, similarity_scores, page):

        #rank by score
        ranked = similarity_scores.argsort()[::-1]
        results = [
            {"url": self.urls[i], "score": similarity_scores[i]}
            for i in ranked if similarity_scores[i] > 0
        ]

        #pagination
        results_per_page = 5
        total_results = len(results)
        total_pages = math.ceil(total_results / results_per_page)
        start = (page - 1) * results_per_page
        end = start + results_per_page
        paginated_results = results[start:end]

        return {
            "pagination": {
                "total_results": total_results,
                "current_page": page,
                "total_pages": total_pages,
            },
            "results": paginated_results
        }
    
        
if __name__ == "__main__":
    engine = QueryEngine(
        mongo_url="mongodb+srv://owwix:finalproject@cluster0.n1zt8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
        db_name="cppbusinesscrawl",
        collection_name="pages",
    )
    engine.load_data()

    query = input("Enter your search query: ")
    
    page = 1
    loop = True
    while loop:

        query_results = engine.query(query, page)

        if query_results["results"]:
            print(f"\nPage {query_results['pagination']['current_page']} of {query_results['pagination']['total_pages']}")
            for result in query_results["results"]:
                print(f"- URL: {result['url']}\nScore: {result['score']:.4f}\n")
        else:
            print("No relevant results found for the query.")
            query = input("Enter a new search query: ")
            page = 1
            continue  

        next_action = input("Enter 'n' for next page, 'p' for previous page, 'a' for a new query, or 'q' to quit: ").lower()
        if next_action == "n":
            if page < query_results["pagination"]["total_pages"]:
                page += 1
            else:
                print("You're already on the last page.")
        elif next_action == "p":
            if page > 1:
                page -= 1
            else:
                print("You're already on the first page.")
        elif next_action == "a":
            query = input("Enter your new search query: ")
            page = 1
        elif next_action == "q":
            loop = False
        else:
            print("Invalid input. Try again.")
