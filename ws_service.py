import json
import os
import logging
import time
import requests

from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

WHATSAPP_BATCH_SIZE = int(os.getenv('WHATSAPP_BATCH_SIZE', 100))
SLEEP_INTERVAL = float(os.getenv('SLEEP_INTERVAL', 2))
MAYTAPI_API_URL = os.getenv('MAYTAPI_API_URL')
MAYTAPI_TOKEN = os.getenv('MAYTAPI_TOKEN')


def send_whatsapp_batched(message, phone_numbers_batched):
    headers = {
        'x-maytapi-key': MAYTAPI_TOKEN,
        'Content-Type': 'application/json'
    }

    for batch in phone_numbers_batched:
        for number in batch:
            data = {
                'to_number': number["address"],
                'message': message,
                'type': 'text'
            }

            try:
                response = requests.post(
                    f'{MAYTAPI_API_URL}/sendMessage',
                    headers=headers,
                    json=data
                )
                response_data = response.json()

                if response.status_code == 200:
                    logging.info(f"Сообщение отправлено на {number['address']}: {response_data}")
                else:
                    logging.error(f"Ошибка при отправке сообщения на {number['address']}: {response_data}")

                time.sleep(SLEEP_INTERVAL)
            except Exception as e:
                logging.error(f"Ошибка при отправке сообщения на {number['address']} через Maytapi: {e}")


consumer = KafkaConsumer(
    'WHATSAPP',
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
        if messenger_type == 'WHATSAPP':
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
