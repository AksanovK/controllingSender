import json
import os
import logging
from telegram import Bot
from dotenv import load_dotenv
import psycopg2
import asyncio
from aiokafka import AIOKafkaConsumer

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SLEEP_INTERVAL = float(os.getenv('SLEEP_INTERVAL', 2))

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
telegram_bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))


def get_user_ids_for_tg(usernames, cursor):
    user_ids = []
    for username in usernames:
        clean_username = username["address"].lstrip('@')
        cursor.execute("SELECT user_id FROM telegram_data WHERE username = %s", (clean_username,))
        result = cursor.fetchone()
        if result:
            user_ids.append(result[0])
    return user_ids


async def send_telegram(message, users):
    count = 0
    for user_id in users:
        if count >= 30:
            await asyncio.sleep(60)
            count = 0
        await telegram_bot.send_message(chat_id=user_id, text=message)
        await asyncio.sleep(1)
        count += 1


async def consume():
    consumer = AIOKafkaConsumer(
        'TELEGRAM',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for message in consumer:
            data = message.value
            body = data['body']
            contacts = data['contacts']
            messenger_type = data['type']
            if messenger_type == 'TELEGRAM':
                conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
                cursor = conn.cursor()
                user_ids = get_user_ids_for_tg(contacts, cursor)
                cursor.close()
                conn.close()
                await send_telegram(body, user_ids)
            else:
                logging.warning(f"Не поддерживаемый тип мессенджера: {messenger_type}")
    finally:
        await consumer.stop()


asyncio.run(consume())
