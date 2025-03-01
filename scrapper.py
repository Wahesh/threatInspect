#!/usr/bin/env python3
"""
Telegram Scraper with Translation (Using Gemini API)

- Reads channels & credentials from `config.json`.
- Scrapes Telegram messages.
- Translates messages using Google Gemini API.
- Stores the translated messages in PostgreSQL.
"""

import asyncio
import json
import os
from datetime import datetime
from telethon import TelegramClient
import psycopg2
from psycopg2.extras import execute_values
from translate import Translator

# ----------------------------
# Load Configuration
# ----------------------------
def load_config(config_path="config.json"):
    """Load API keys and settings from config.json"""
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()

# ----------------------------
# PostgreSQL Database Functions
# ----------------------------
def create_db_connection():
    """Connect to PostgreSQL."""
    db_config = config["postgres"]
    return psycopg2.connect(
        dbname=db_config["db_name"],
        user=db_config["db_user"],
        password=db_config["db_password"],
        host=db_config["db_host"],
        port=db_config.get("db_port", "5432")
    )

def setup_database():
    """Create database and tables if they don't exist."""
    conn = create_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
        select 1; 
        """)
        conn.commit()
    conn.close()

def insert_messages(messages):
    """Insert messages into PostgreSQL, avoiding duplicates."""
    conn = create_db_connection()
    with conn.cursor() as cur:
        sql = """
            INSERT INTO scraped_messages (
                security_area, region, city_town_area, event_date, event_time, 
                source_message_original, source_message_translated, target_group, perpetrator_group, 
                threat_type, incident_type, no_of_explosives, analysis_comments, 
                total_casualties, deaths, injuries, source_channel
            ) VALUES %s
            ON CONFLICT (source_channel, event_date, event_time, source_message_original) DO NOTHING;
        """
        values = [
            (
                msg.get('security_area', 'Unknown'),
                msg.get('region', 'Unknown'),
                msg.get('city_town_area', 'Unknown'),
                msg.get('event_date'),
                msg.get('event_time'),
                msg.get('source_message_original'),
                msg.get('source_message_translated', ''),
                msg.get('target_group', ''),
                msg.get('perpetrator_group', ''),
                msg.get('threat_type', 'Unknown'),
                msg.get('incident_type', ''),
                msg.get('no_of_explosives', ''),
                msg.get('analysis_comments', ''),
                msg.get('total_casualties', 0),
                msg.get('deaths', 0),
                msg.get('injuries', 0),
                msg.get('source_channel')
            ) for msg in messages
        ]
        if values:
            execute_values(cur, sql, values)
            conn.commit()
    conn.close()

# ----------------------------
# Google Gemini Translation
# ----------------------------

# Configure API once (instead of every function call)

def translate_text(text, to_language="en"):
    """Translate text using the translate library from PyPI"""
    if not text:
        return ""

    try:
        # Create a translator object
        translator = Translator(to_lang=to_language)
        
        # Translate the text
        translation = translator.translate(text)
        
        return translation.strip()
    except Exception as e:
        print(f"Translation error: {e}")
        return text  # Return original text if translation fails

# ----------------------------
# Telegram Scraping Functions
# ----------------------------
async def fetch_messages_from_channel(client, channel_identifier):
    """Fetch messages from a specific Telegram channel."""
    messages = []
    
    # Check devmode: If enabled, fetch only 10 messages
    message_limit = 1 if config.get("devmode", 0) == 1 else 100

    try:
        entity = await client.get_entity(channel_identifier)
    except Exception as e:
        print(f"Error getting entity for {channel_identifier}: {e}")
        return messages

    async for message in client.iter_messages(entity, limit=message_limit):
        if message.message:
            event_date = message.date.strftime("%Y-%m-%d")
            event_time = message.date.strftime("%H:%M:%S")

            messages.append({
                'security_area': 'Unknown',
                'region': 'Unknown',
                'city_town_area': 'Unknown',
                'event_date': event_date,
                'event_time': event_time,
                'source_message_original': message.message,
                'source_message_translated': '',
                'target_group': '',
                'perpetrator_group': '',
                'threat_type': 'Unknown',
                'incident_type': '',
                'no_of_explosives': '',
                'analysis_comments': '',
                'total_casualties': 0,
                'deaths': 0,
                'injuries': 0,
                'source_channel': channel_identifier
            })

    print(f"Fetched {len(messages)} messages from {channel_identifier}")
    return messages
async def fetch_all_messages():
    """Fetch messages from all channels listed in `config.json`."""
    telegram_config = config["telegram"]
    client = TelegramClient('session', telegram_config["api_id"], telegram_config["api_hash"])
    await client.start()

    all_messages = []
    for channel in telegram_config["channels"]:
        print(f"Fetching messages from channel: {channel}")
        msgs = await fetch_messages_from_channel(client, channel)  # Remove 'limit=100'
        all_messages.extend(msgs)

    await client.disconnect()
    return all_messages


# ----------------------------
# Main Function
# ----------------------------
async def main():
    setup_database()  # Ensure tables exist
    messages = await fetch_all_messages()  # Scrape Telegram messages
    insert_messages(messages)  # Store data in PostgreSQL

# ----------------------------
# Entry Point
# ----------------------------
if __name__ == '__main__':
    asyncio.run(main())
