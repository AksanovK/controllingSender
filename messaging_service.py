import json
import os
import time
import logging
from vk_api import VkApi
from telegram import Bot
import yagmail
from twilio.rest import Client
from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2
import asyncio

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

VK_BATCH_SIZE = int(os.getenv('VK_BATCH_SIZE', 100))
WHATSAPP_BATCH_SIZE = int(os.getenv('WHATSAPP_BATCH_SIZE', 100))
SLEEP_INTERVAL = float(os.getenv('SLEEP_INTERVAL', 2))

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

vk_session = VkApi(token=os.getenv('VK_TOKEN'))
telegram_bot = Bot(token=os.getenv('TELEGRAM_TOKEN'))
gmail_user = os.getenv('GMAIL_USER')
gmail_password = os.getenv('GMAIL_PASSWORD')
yag = yagmail.SMTP(user=gmail_user, password=gmail_password, host='smtp.mail.ru', port=465, smtp_ssl=True)
twilio_client = Client(os.getenv('TWILIO_SID'), os.getenv('TWILIO_AUTH_TOKEN'))
last_message_time = 0


def get_user_ids_for_tg(usernames, cursor):
    user_ids = []
    for username in usernames:
        clean_username = username["address"].lstrip('@')
        cursor.execute("SELECT user_id FROM telegram_data WHERE username = %s", (clean_username,))
        result = cursor.fetchone()
        if result:
            user_ids.append(result[0])
    return user_ids


def get_user_id_from_username(vk, username):
    try:
        response = vk.users.get(user_ids=username)
        if response:
            return response[0]['id']
        else:
            return None
    except Exception as e:
        logging.error(f"Ошибка при получении user_id: {e}")
        return None


def send_vk_batched(message, user_ids_batched):
    vk = vk_session.get_api()
    for batch in user_ids_batched:
        try:
            user_ids = []
            for user in batch:
                user_id = get_user_id_from_username(vk, user["address"])
                if user_id:
                    user_ids.append(user_id)
                time.sleep(2)

            for user_id in user_ids:
                vk.messages.send(user_id=user_id, message=message, random_id=0)
                time.sleep(SLEEP_INTERVAL)
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщений VK: {e}")


async def send_telegram(message, users):
    count = 0
    for user_id in users:
        if count >= 30:
            await asyncio.sleep(60)
            count = 0
        await telegram_bot.send_message(chat_id=user_id, text=message)
        await asyncio.sleep(1)
        count += 1


def send_gmail(subject, message, emails):
    for email in emails:
        try:
            address = email["address"]
            yag.send(to=address, subject=subject, contents=message)
            time.sleep(SLEEP_INTERVAL)
        except Exception as e:
            logging.error(f"Ошибка при отправке электронной почты {address}: {e}")


def send_whatsapp_batched(message, phone_numbers_batched):
    print(f"Функция пока не доступна")


consumer = KafkaConsumer(
    'send-instructions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

try:
    for message in consumer:
        data = message.value
        subject = data.get('subject', 'No Subject')
        body = data['body']
        contacts = data['contacts']
        messenger_type = data['type']
        if messenger_type == 'VK':
            user_ids_batched = [contacts[i:i + VK_BATCH_SIZE] for i in range(0, len(contacts), VK_BATCH_SIZE)]
            send_vk_batched(body, user_ids_batched)
        elif messenger_type == 'TELEGRAM':
            conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cursor = conn.cursor()
            user_ids = get_user_ids_for_tg(contacts, cursor)
            cursor.close()
            conn.close()
            asyncio.run(send_telegram(body, user_ids))
        elif messenger_type == 'GMAIL':
            send_gmail(subject, body, contacts)
        elif messenger_type == 'WHATSAPP':
            phone_numbers_batched = [contacts[i:i + WHATSAPP_BATCH_SIZE] for i in
                                     range(0, len(contacts), WHATSAPP_BATCH_SIZE)]
            send_whatsapp_batched(body, phone_numbers_batched)
        else:
            logging.warning(f"Не поддерживаемый тип мессенджера: {messenger_type}")
except KeyboardInterrupt:
    logging.info("Прервано пользователем")
except Exception as e:
    logging.error(f"Ошибка во время обработки сообщений: {e}")
finally:
    consumer.close()
    logging.info("Потребитель Kafka закрыт")
