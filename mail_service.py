import json
import os
import time
import logging
import yagmail
import random
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levellevelname)s - %(message)s')

SLEEP_INTERVAL_MIN = float(os.getenv('SLEEP_INTERVAL', 2))
SLEEP_INTERVAL_MAX = SLEEP_INTERVAL_MIN + 3
gmail_user = os.getenv('GMAIL_USER')
gmail_password = os.getenv('GMAIL_PASSWORD')
yag = yagmail.SMTP(user=gmail_user, password=gmail_password, host='smtp.mail.ru', port=465, smtp_ssl=True)


def send_gmail(subject, message, emails):
    for email in emails:
        try:
            address = email["address"]
            yag.send(to=address, subject=subject, contents=message)
            time.sleep(random.uniform(SLEEP_INTERVAL_MIN, SLEEP_INTERVAL_MAX))
        except Exception as e:
            logging.error(f"Ошибка при отправке электронной почты {address}: {e}")


consumer = KafkaConsumer(
    'EMAIL',
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
        if messenger_type == 'EMAIL':
            send_gmail(subject, body, contacts)
        else:
            logging.warning(f"Не поддерживаемый тип мессенджера: {messenger_type}")
except KeyboardInterrupt:
    logging.info("Прервано пользователем")
except Exception as e:
    logging.error(f"Ошибка во время обработки сообщений: {e}")
finally:
    consumer.close()
    logging.info("Потребитель Kafka закрыт")
