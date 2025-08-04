from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, expr, col, struct, to_json, collect_list, from_json, sum as sql_sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

class SparkEmojiProcessor:
    def __init__(self, batch_interval=2):
        # Initialize SparkEmojiProcessor with batch interval
        self.batch_interval = batch_interval
        self.spark = self._create_spark_session()
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        # Emoji mapping for conversion
        self.emoji_mapping = {
            "\u2764\ufe0f": "❤️", 
            "\ud83d\ude02": "😂", 
            "\ud83d\udc4d": "👍", 
            "\ud83d\ude0d": "😍", 
            "\ud83d\ude00": "😀", 
            "\ud83c\udf89": "🎉"
        }

    def _create_spark_session(self):
        # Create a Spark Session with necessary configurations
        os.environ['SPARK_LOCAL_IP'] = '192.168.146.129'  
        
        return SparkSession.builder \
            .appName("EmojiStreamProcessor") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.streaming.kafka.maxRatePerTrigger", "2000") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.driver.host", "192.168.146.129") \
            .master("local[*]") \
            .getOrCreate()

    def _convert_emoji(self, unicode_emoji):
        
        return self.emoji_mapping.get(unicode_emoji, unicode_emoji)

    def _process_micro_batch(self, df, epoch_id):
        try:
            
            window_start = datetime.now().replace(microsecond=0).isoformat()
            window_end = (datetime.now() + timedelta(seconds=self.batch_interval)).replace(microsecond=0).isoformat()
            window_duration = f"{self.batch_interval} seconds"

            
            emoji_counts = df.groupBy("emoji_type").count().collect()

         
            output_data = {
                "window": {
                    "start": window_start,
                    "end": window_end,
                    "duration": window_duration
                },
                "emoji_aggregates": [],
                "most_popular_emoji": None,
                "popularity_count": 0
            }

            max_count = 0
            for row in emoji_counts:
                display_emoji = self._convert_emoji(row['emoji_type'])
                count = row['count']
                scaled_count = max(1, count // 10)

                output_data["emoji_aggregates"].append({
                    "emoji": display_emoji,
                    "count": count,
                    "scaled_count": scaled_count
                })

           
                if count > max_count:
                    max_count = count
                    output_data["most_popular_emoji"] = display_emoji
                    output_data["popularity_count"] = max_count

    
            for cluster_id in range(1, 4):
                self.producer.send(f'cluster_{cluster_id}_topic', output_data)
            self.producer.send('processed_emoji_topic', output_data)

       
            print(f"Processed batch {epoch_id}: {json.dumps(output_data, ensure_ascii=False, indent=2)}")

        except Exception as e:
            print(f"Error processing batch {epoch_id}: {str(e)}")

    def start_processing(self):
        # Define schema for incoming data
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("emoji_type", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])


        streaming_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "emoji_topic") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 2000) \
            .load()

       
        streaming_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "emoji_topic") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 2000) \
            .load()

        
        parsed_df = streaming_df \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

        
        query = parsed_df.writeStream \
            .foreachBatch(self._process_micro_batch) \
            .trigger(processingTime=f'{self.batch_interval} seconds') \
            .outputMode("update") \
            .start()

        
        query.awaitTermination()

if __name__ == "__main__":
    processor = SparkEmojiProcessor(batch_interval=2)
    processor.start_processing()
