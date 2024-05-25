import json
import os
import time
import logging
from vk_api import VkApi
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

VK_BATCH_SIZE = int(os.getenv('VK_BATCH_SIZE', 100))
SLEEP_INTERVAL = float(os.getenv('SLEEP_INTERVAL', 2))
vk_session = VkApi(token=os.getenv('VK_TOKEN'))


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


consumer = KafkaConsumer(
    'VK',
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
        else:
            logging.warning(f"Не поддерживаемый тип мессенджера: {messenger_type}")
except KeyboardInterrupt:
    logging.info("Прервано пользователем")
except Exception as e:
    logging.error(f"Ошибка во время обработки сообщений: {e}")
finally:
    consumer.close()
    logging.info("Потребитель Kafka закрыт")
