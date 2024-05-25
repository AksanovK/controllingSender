from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def db_connect():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)


def insert_user(user_id, username):
    conn = db_connect()
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO telegram_data (user_id, username) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username;
        """, (user_id, username))
        conn.commit()
    conn.close()


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        user_id = user.id
        username = user.username or "unknown"
        insert_user(user_id, username)
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text="Регистрация прошла успешно!")


app = Application.builder().token(os.getenv('TELEGRAM_TOKEN')).build()
app.add_handler(CommandHandler("start", handle_start))
app.run_polling()
