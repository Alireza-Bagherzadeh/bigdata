import requests
import sys
import time
import re
import json
from datetime import datetime
from kafka import KafkaProducer
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
STOP_WORDS = set(stopwords.words('english'))  # بارگیری و تبدیل به مجموعه
sys.stdout.reconfigure(encoding='utf-8')

BASE_URL = "https://v2.jokeapi.dev/joke/Programming"

# STOP_WORDS = {
#     'the', 'and', 'a', 'an', 'to', 'in', 'of', 'for', 'on', 'with', 
#     'that', 'this', 'it', 'is', 'are', 'be', 'as', 'at', 'by', 'so'
# }

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

# تنظیم Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # آدرس‌های بروکر Kafka
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

topic = 'jokes'

print("Kafka Producer شروع به کار کرد...")

def fetch_and_send_joke():
    try:
        response = requests.get(BASE_URL, params={"type": "single", "blacklistFlags": "nsfw", "amount": 1})
        joke = response.json().get('joke', 'No joke found')
    except Exception as e:
        joke = f"Error: {e}"
    
    # استخراج آیتم‌ست‌ها
    text_processor = TextProcessor()
    itemsets = text_processor.clean_and_generate_itemsets(joke)
    
    message = {
        "timestamp": datetime.now().isoformat(),
        "joke": joke,
        "itemsets": itemsets
    }
    
    producer.send(topic, message)
    producer.flush()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ارسال شد: {message}")

if __name__ == '__main__':
    try:
        while True:
            fetch_and_send_joke()
            time.sleep(2)  # وقفه ۲ ثانیه‌ای
    except KeyboardInterrupt:
        print("توقف Producer")
