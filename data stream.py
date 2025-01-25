import requests
import sys
import time
import re
from collections import OrderedDict
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

BASE_URL = "https://v2.jokeapi.dev/joke/Programming"

STOP_WORDS = {
    'the', 'and', 'a', 'an', 'to', 'in', 'of', 'for', 'on', 'with', 
    'that', 'this', 'it', 'is', 'are', 'be', 'as', 'at', 'by', 'so'
}

class TextProcessor:
    @staticmethod
    def clean_text(text):
        text = re.sub(r'[^\w\s]', '', text.lower())
        words = [word for word in text.split() 
                if word not in STOP_WORDS and len(word) > 2]
        return words

class EnhancedLossyCounting:
    def __init__(self, epsilon):
        self.epsilon = epsilon
        self.window_size = int(1 / epsilon)
        self.current_window = 1
        self.global_items = OrderedDict()
        self.window_history = []

    def add(self, words):
        # افزودن کلمات به تاریخچه پنجره فعلی
        current_time = datetime.now()
        self.window_history.append({
            'timestamp': current_time,
            'words': words
        })
        
        # به‌روزرسانی شمارش جهانی
        for word in words:
            if word in self.global_items:
                self.global_items[word]['count'] += 1
                self.global_items[word]['last_seen'] = current_time
            else:
                self.global_items[word] = {
                    'count': 1,
                    'first_seen': current_time,
                    'last_seen': current_time
                }

        # مدیریت پنجره‌ها هنگام رسیدن به اندازه محدود
        if len(self.window_history) >= self.window_size:
            self._manage_windows()

    def _manage_windows(self):
        print(f"\n{'='*30}\n[Window Management] Current Windows: {len(self.window_history)}")
        
        # مرحله 1: حذف قدیمی‌ترین پنجره
        oldest_window = self.window_history.pop(0)
        print(f"Removing oldest window from {oldest_window['timestamp'].strftime('%H:%M:%S')}")
        
        # مرحله 2: به‌روزرسانی شمارش جهانی
        for word in oldest_window['words']:
            self.global_items[word]['count'] -= 1
            if self.global_items[word]['count'] <= 0:
                del self.global_items[word]
                print(f"Word '{word}' removed from global count")

        # نمایش وضعیت فعلی
        self._show_global_stats()
        self.current_window += 1

    def _show_global_stats(self):
        print("\nGlobal Word Frequencies:")
        for word, info in sorted(self.global_items.items(), 
                               key=lambda x: (-x[1]['count'], x[1]['last_seen'])):
            print(f"- {word.upper()}: {info['count']} times "
                  f"(First: {info['first_seen'].strftime('%H:%M:%S')}, "
                  f"Last: {info['last_seen'].strftime('%H:%M:%S')})")

def fetch_jokes():
    try:
        response = requests.get(BASE_URL, params={
            "type": "single",
            "blacklistFlags": "nsfw",
            "amount": 1
        })
        joke = response.json().get('joke', '')
        return joke if joke else "No joke found"
    except Exception as e:
        return f"Error: {e}"

def fetch_jokes_repeatedly():
    text_processor = TextProcessor()
    analyzer = EnhancedLossyCounting(epsilon=0.25)
    
    while True:
        raw_joke = fetch_jokes()
        print(f"\n{'='*30}\nNew Joke at {datetime.now().strftime('%H:%M:%S')}:")
        print(f"{raw_joke}")
        
        words = text_processor.clean_text(raw_joke)
        print(f"\nCleaned Words: {words}")
        
        analyzer.add(words)
        time.sleep(2)

fetch_jokes_repeatedly()