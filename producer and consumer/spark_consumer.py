from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import datetime
import sys
import time
import re
from collections import OrderedDict
# تعریف الگوریتم EnhancedLossyCounting به‌روز شده
class EnhancedLossyCounting:
    def __init__(self, epsilon=0.25):
        self.epsilon = epsilon
        self.window_size = int(1 / epsilon)  # برای ε=0.25، window_size = 4
        self.current_window = 1
        self.global_items = OrderedDict()
        self.window_history = []
    
    def add(self, itemsets):
        current_time = datetime.now()
        self.window_history.append({
            'timestamp': current_time,
            'itemsets': itemsets
        })
        
        # به‌روز‌رسانی شمارش آیتم‌ست‌ها
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
        
        # مدیریت پنجره: اگر پنجره‌های ذخیره‌شده از اندازه تعیین‌شده (4) تجاوز کرد،
        # پنجره قدیمی حذف شده و شمارش آیتم‌ست‌های مربوط کاهش می‌یابد.
        if len(self.window_history) > self.window_size:
            self._manage_windows()
        
        # اعمال محدودیت نگهداری state: حداکثر 4 آیتم‌ست نگهداری شود.
        if len(self.global_items) > 4:
            # مرتب‌سازی: ابتدا براساس count (نزولی) و در صورت تساوی، براساس first_seen (نزولی؛ به این ترتیب آیتم‌های تازه‌تر حفظ می‌شوند)
            sorted_items = sorted(self.global_items.items(), key=lambda x: (x[1]['count'], x[1]['first_seen']), reverse=True)
            top4 = sorted_items[:4]
            new_global = OrderedDict()
            for k, v in top4:
                new_global[k] = v
            self.global_items = new_global
    
    def _manage_windows(self):
        oldest_window = self.window_history.pop(0)
        print(f"\n{'='*30}\n[Window Management] حذف پنجره از {oldest_window['timestamp'].strftime('%H:%M:%S')}")
        for itemset in oldest_window['itemsets']:
            if itemset in self.global_items:
                self.global_items[itemset]['count'] -= 1
                if self.global_items[itemset]['count'] <= 0:
                    del self.global_items[itemset]
        self.current_window += 1
    
    def get_frequent_itemsets(self, threshold=2):
        # فقط آیتم‌ست‌هایی که شمارش آن‌ها ≥ threshold است انتخاب می‌شود.
        frequent = {k: v for k, v in self.global_items.items() if v['count'] >= threshold}
        # مرتب‌سازی به صورت نزولی بر اساس count و در صورت تساوی، آیتم‌های تازه‌تر (با first_seen بزرگتر) در صدر قرار می‌گیرند.
        sorted_items = sorted(frequent.items(), key=lambda x: (x[1]['count'], x[1]['first_seen']), reverse=True)
        return sorted_items
import os

# تنظیم متغیرهای محیطی مربوط به Hadoop
os.environ['HADOOP_HOME'] = r'C:\hadoop'
# اضافه کردن مسیر hadoop\bin به PATH در صورت نیاز
os.environ['PATH'] = os.environ['PATH'] + r';C:\hadoop\bin'

# تنظیم SparkSession
spark = SparkSession.builder.appName("JokeLossyCountingConsumer") \
.config("spark.hadoop.home.dir", "C:\\hadoop")\
.config("spark.executorEnv.PATH", os.environ['PATH'])\
 .config("spark.driver.extraJavaOptions", "-Dhadoop.native.lib=false -Djava.library.path=C:\\hadoop\\bin") \
.config("spark.executor.extraJavaOptions", "-Dhadoop.native.lib=false -Djava.library.path=C:\\hadoop\\bin") \
.config("spark.hadoop.fs.permissions.enabled", "false")\
.config("spark.hadoop.io.native.lib.available", "false") \
.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")\
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# تعریف schema پیام‌های دریافتی از Kafka
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("joke", StringType(), True),
    StructField("itemsets", ArrayType(StringType()), True)
])

# خواندن داده‌ها از Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "jokes") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# ایجاد یک نمونه global از الگوریتم lossy counting
analyzer = EnhancedLossyCounting(epsilon=0.25)

def process_batch(batch_df, batch_id):
    # در هر میکروبچ، داده‌ها را به driver جمع‌آوری می‌کنیم.
    rows = batch_df.collect()
    for row in rows:
        if row.itemsets:
            analyzer.add(row.itemsets)
    # چاپ وضعیت state به‌روز شده
    frequent_items = analyzer.get_frequent_itemsets(threshold=2)
    print("\nBatch ID:", batch_id, "Frequent Itemsets (Count ≥ 2):")
    for itemset, info in frequent_items:
        print(f"Itemset: {itemset} | Count: {info['count']} | First Seen: {info['first_seen'].strftime('%H:%M:%S')} | Last Seen: {info['last_seen'].strftime('%H:%M:%S')}")
    print("-"*80)

# استفاده از foreachBatch برای پردازش میکروبچ‌ها
query = df_parsed.writeStream.foreachBatch(process_batch).outputMode("append").start()
query.awaitTermination()
