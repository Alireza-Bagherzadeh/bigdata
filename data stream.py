import requests
import sys
import time
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

# API base URL
BASE_URL = "https://v2.jokeapi.dev/joke/Programming"

# Lossy Counting Algorithm Class
class LossyCounting:
    def __init__(self, epsilon):
        self.epsilon = epsilon
        self.window_size = int(1 / epsilon)
        self.current_window = 1
        self.data = defaultdict(int)
        self.bucket_map = {}  # Track the minimum bucket for each item

    def add(self, item):
        # Add or increment the count for the item
        if item in self.data:
            self.data[item] += 1
        else:
            self.data[item] = 1
            self.bucket_map[item] = self.current_window - 1

        # Check if the current window has reached its size limit
        if sum(self.data.values()) >= self.current_window * self.window_size:
            self._trim_data()

    def _trim_data(self):
        print(f"\n[Trim Triggered] Window: {self.current_window}")
        # Identify items to remove (low-frequency items)
        items_to_remove = [
            item for item in self.data
            if self.data[item] + self.bucket_map[item] <= self.current_window
        ]
        # Remove items
        for item in items_to_remove:
            print(f"Removing low-frequency item: {item}")
            del self.data[item]
            del self.bucket_map[item]

        # Increment the window counter
        self.current_window += 1

    def get_frequencies(self, threshold):
        # Return items whose count exceeds the threshold
        return {
            item: count
            for item, count in self.data.items()
            if count >= threshold
        }

# Function to fetch jokes based on parameters
def fetch_jokes(
    category="Programming",  # Joke category
    language="en",
    blacklist_flags=None,    # Flags to blacklist (e.g., "nsfw,political")
    response_format="json",  # Response format: json, xml, yaml, or plain text
    joke_type="single",      # Joke type: single or twopart
    search_string=None,      # Search for jokes containing specific text
    id_range=None,           # Joke ID range (e.g., "0-1367")
    amount=1                 # Number of jokes to fetch
):
    # Build the query parameters
    params = {
        "type": joke_type,
        "lang": language,
        "amount": amount,
    }
    if blacklist_flags:
        params["blacklistFlags"] = blacklist_flags
    if search_string:
        params["contains"] = search_string
    if id_range:
        params["idRange"] = id_range

    # Send the GET request
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        jokes = response.json()

        # Process the response
        if "error" in jokes and jokes["error"]:
            print("Error fetching jokes:", jokes["message"])
        else:
            joke_list = []
            if isinstance(jokes, dict) and "joke" in jokes:
                # Single joke response
                joke_list.append(jokes["joke"])
            elif isinstance(jokes, dict) and "jokes" in jokes:
                # Multiple jokes response
                for joke in jokes["jokes"]:
                    if joke["type"] == "single":
                        joke_list.append(joke["joke"])
                    elif joke["type"] == "twopart":
                        joke_list.append(f"{joke['setup']} - {joke['delivery']}")
            return joke_list

    except requests.RequestException as e:
        print(f"An error occurred: {e}")

    return []

# Fetch jokes repeatedly with Lossy Counting
def fetch_jokes_repeatedly():
    lossy_counter = LossyCounting(epsilon=0.01)  # Set epsilon for lossy counting
    while True:
        jokes = fetch_jokes(
            joke_type="single",       # Fetch single jokes
            blacklist_flags="nsfw",  # Exclude NSFW jokes
            amount=1                  # Fetch 1 joke at a time
        )

        for joke in jokes:
            lossy_counter.add(joke)  # Add joke to Lossy Counting
            print(f"Fetched Joke: {joke}")

        # Print the current frequencies with a specific threshold
        threshold = 1  # Define the threshold for frequency
        print("\n[Current Joke Frequencies]")
        frequent_items = lossy_counter.get_frequencies(threshold=threshold)
        for joke, count in frequent_items.items():
            print(f"{joke}: {count}")

        # Wait 5 seconds before fetching the next joke
        time.sleep(1)

# Start fetching jokes repeatedly
fetch_jokes_repeatedly()
