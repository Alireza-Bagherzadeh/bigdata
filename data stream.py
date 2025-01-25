import requests
import sys
import time
import re
from collections import OrderedDict, defaultdict
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

BASE_URL = "https://v2.jokeapi.dev/joke/Programming"

STOP_WORDS = {
    'the', 'and', 'a', 'an', 'to', 'in', 'of', 'for', 'on', 'with', 
    'that', 'this', 'it', 'is', 'are', 'be', 'as', 'at', 'by', 'so'
}

class TextProcessor:
    @staticmethod
    def clean_and_generate_itemsets(text, max_length=4):
        text = re.sub(r'[^\w\s]', '', text.lower())
        words = [word for word in text.split() 
                if word not in STOP_WORDS and len(word) > 2]
        
        itemsets = []
        for i in range(len(words)):
            for j in range(1, max_length+1):
                if i+j <= len(words):
                    itemset = ' '.join(words[i:i+j])
                    itemsets.append(itemset)
        return itemsets

class EnhancedLossyCounting:
    def __init__(self, epsilon=0.25):
        self.epsilon = epsilon
        self.window_size = int(1 / epsilon)
        self.current_window = 1
        self.global_items = OrderedDict()
        self.window_history = []

    def add(self, itemsets):
        current_time = datetime.now()
        self.window_history.append({
            'timestamp': current_time,
            'itemsets': itemsets
        })

        # Update global counts
        for itemset in itemsets:
            if itemset in self.global_items:
                self.global_items[itemset]['count'] += 1
                self.global_items[itemset]['last_seen'] = current_time
            else:
                self.global_items[itemset] = {
                    'count': 1,
                    'first_seen': current_time,
                    'last_seen': current_time
                }

        # Window management
        if len(self.window_history) > self.window_size:
            self._manage_windows()

    def _manage_windows(self):
        oldest_window = self.window_history.pop(0)
        print(f"\n{'='*30}\n[Window Management] Removing window from {oldest_window['timestamp'].strftime('%H:%M:%S')}")
        
        # Decrement counts for old itemsets
        for itemset in oldest_window['itemsets']:
            if itemset in self.global_items:
                self.global_items[itemset]['count'] -= 1
                if self.global_items[itemset]['count'] <= 0:
                    del self.global_items[itemset]
        
        self.current_window += 1

    def get_frequent_itemsets(self, threshold=2):
        return {k: v for k, v in self.global_items.items() if v['count'] >= threshold}

def print_section(title, data, columns, max_width=80):
    print(f"\n{title}")
    print("-" * max_width)
    header = " | ".join(columns)
    print(header)
    print("-" * max_width)
    
    for item in data:
        if isinstance(item, tuple):
            row = [str(x) for x in item]
        else:
            row = [str(item)]
        print(" | ".join(row))
    print("-" * max_width)

def fetch_jokes_repeatedly():
    text_processor = TextProcessor()
    analyzer = EnhancedLossyCounting(epsilon=0.25)
    iteration = 1
    
    try:
        while True:
            # Fetch joke
            try:
                response = requests.get(BASE_URL, params={"type": "single", "blacklistFlags": "nsfw", "amount": 1})
                joke = response.json().get('joke', 'No joke found')
            except Exception as e:
                joke = f"Error: {e}"
            
            # Process text
            itemsets = text_processor.clean_and_generate_itemsets(joke)
            
            # Display processing info
            print(f"\n{'='*50}\nIteration: {iteration}")
            print(f"\n[New Joke] {datetime.now().strftime('%H:%M:%S')}:")
            print(f"{joke}")
            
            print_section(
                "Generated Itemsets (1-4 words)", 
                itemsets, 
                ["Itemsets"]
            )
            
            # Add to analyzer
            analyzer.add(itemsets)
            
            # Show frequent itemsets
            frequent_items = analyzer.get_frequent_itemsets(threshold=2)
            if frequent_items:
                sorted_items = sorted(
                    frequent_items.items(),
                    key=lambda x: (-x[1]['count'], x[1]['last_seen'])
                )
                display_data = [
                    (
                        itemset, 
                        info['count'], 
                        info['first_seen'].strftime('%H:%M:%S'), 
                        info['last_seen'].strftime('%H:%M:%S')
                    )
                    for itemset, info in sorted_items
                ]
                print_section(
                    "Frequent Itemsets (Count ≥ 2)",
                    display_data,
                    ["Itemset", "Count", "First Seen", "Last Seen"]
                )
            else:
                print("\nNo frequent itemsets meeting threshold yet")
            
            iteration += 1
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n\nFinal Report:")
        final_items = analyzer.get_frequent_itemsets(threshold=2)
        if final_items:
            sorted_final = sorted(
                final_items.items(),
                key=lambda x: (-x[1]['count'], x[1]['last_seen'])
            )
            display_data = [
                (
                    itemset, 
                    info['count'], 
                    info['first_seen'].strftime('%H:%M:%S'), 
                    info['last_seen'].strftime('%H:%M:%S')
                )
                for itemset, info in sorted_final
            ]
            print_section(
                "All Frequent Itemsets (Count ≥ 2)",
                display_data,
                ["Itemset", "Count", "First Seen", "Last Seen"]
            )
        else:
            print("No frequent itemsets met the threshold overall")

fetch_jokes_repeatedly()