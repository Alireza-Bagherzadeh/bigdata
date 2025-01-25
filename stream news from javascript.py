import requests
import re
import time
from collections import defaultdict
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Lossy Counting Algorithm Implementation
class LossyCounting:
    def __init__(self, epsilon):
        self.epsilon = epsilon  # Error parameter
        self.bucket_width = int(1 / epsilon)  # Width of the bucket
        self.current_bucket = 1  # Current bucket ID
        self.data = defaultdict(int)  # Stores frequency counts
        self.bucket_map = {}  # Maps items to their minimum bucket IDs

    def add(self, item):
        # Increment the count of the item
        if item in self.data:
            self.data[item] += 1
        else:
            self.data[item] = 1
            self.bucket_map[item] = self.current_bucket - 1

        # Prune items with low frequency
        self.prune()

    def prune(self):
        # Remove items that fall below the threshold
        keys_to_delete = [
            item for item in self.data
            if self.data[item] + self.bucket_map[item] <= self.current_bucket
        ]
        for item in keys_to_delete:
            del self.data[item]
            del self.bucket_map[item]

    def get_frequent_items(self, threshold):
        # Return items whose count exceeds the threshold
        return {
            item: count
            for item, count in self.data.items()
            if count >= threshold
        }

    def increment_bucket(self):
        self.current_bucket += 1


# Function to normalize Persian text
def normalize_persian_text(text):
    # Replace Arabic "ي" with Persian "ی" and Arabic "ك" with Persian "ک"
    text = text.replace("ي", "ی").replace("ك", "ک")
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# Instantiate LossyCounting with an epsilon value
epsilon = 0.1  # Adjust based on desired accuracy
lossy_counting = LossyCounting(epsilon)

# Function to fetch and process news
def fetch_news():
    api_url = "http://www.parseek.com/Javascript/"
    params = {
        "items": 15,
        "type": "ALLL",
        "bullet": "true",
        "source": "false"
    }

    try:
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            raw_data = response.text
            # Extract text data from the HTML response
            cleaned_data = re.findall(r'>» (.*?)<', raw_data)

            if not cleaned_data:
                print("No news data found.")
                return

            print("News Data:")
            with open("news_data.txt", "w", encoding="utf-8") as file:
                for news_item in cleaned_data:
                    # Normalize Persian text
                    normalized_item = normalize_persian_text(news_item)
                    file.write(normalized_item + "\n")
                    print(normalized_item)
                    lossy_counting.add(normalized_item)  # Add item to Lossy Counting
                    time.sleep(1)

            # Increment bucket after processing this batch
            lossy_counting.increment_bucket()

            # Get and display frequent items
            threshold = 2  # Define a reasonable threshold
            frequent_items = lossy_counting.get_frequent_items(threshold)
            print("\nFrequent News Patterns:")
            if frequent_items:
                for item, count in frequent_items.items():
                    print(f"'{item}': {count} occurrences")
            else:
                print("No frequent items found.")
        else:
            print(f"Error: Unable to fetch news. Status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Call the function to fetch news
fetch_news()
