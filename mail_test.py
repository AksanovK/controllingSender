import os
import time
import logging
import yagmail
from dotenv import load_dotenv

load_dotenv()

gmail_user = os.getenv('GMAIL_USER')
gmail_password = os.getenv('GMAIL_PASSWORD')
yag = yagmail.SMTP(user=gmail_user, password=gmail_password, host='smtp.mail.ru', port=465, smtp_ssl=True)


def send_gmail(subject, message, emails):
    for email in emails:
        try:
            address = email
            yag.send(to=address, subject=subject, contents=message)
            time.sleep(3)
        except Exception as e:
            logging.error(f"Ошибка при отправке электронной почты {address}: {e}")


def main():
    contacts = 'aksanovkamil@gmail.com'
    subject = 'test subject'
    message = 'Это тестовое сообщение отправлено из Python с использованием mail.ru.'
    yag.send(to=contacts, subject=subject, contents=message)
    # send_gmail(subject, message, contacts)
    print(f"Сообщение отправлено!")


if __name__ == '__main__':
    main()
