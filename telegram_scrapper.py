#!/usr/bin/env python3
"""
Telegram Scraper

This script reads configuration from 'config.json' for Telegram API credentials,
channels to scrape, and PostgreSQL connection details. It then scrapes the latest
messages from the specified Telegram channels and stores them in the existing `scraped_messages` table.
"""

import asyncio
import json
import os
from datetime import datetime
from telethon import TelegramClient
import psycopg2
from psycopg2.extras import execute_values

# ----------------------------
# Configuration Loader
# ----------------------------
def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

# ----------------------------
# PostgreSQL Database Functions
# ----------------------------
def create_db_connection(db_config):
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        dbname=db_config["db_name"],
        user=db_config["db_user"],
        password=db_config["db_password"],
        host=db_config["db_host"],
        port=db_config.get("db_port", "5432")
    )

def insert_messages(conn, messages):
    """Insert scraped Telegram messages into the existing `scraped_messages` table."""
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

# ----------------------------
# Telegram Scraping Functions
# ----------------------------
async def fetch_messages_from_channel(client, channel_identifier, limit=100):
    """Fetch messages from a specific Telegram channel."""
    messages = []
    try:
        entity = await client.get_entity(channel_identifier)
    except Exception as e:
        print(f"Error getting entity for {channel_identifier}: {e}")
        return messages

    async for message in client.iter_messages(entity, limit=limit):
        if message.message:
            event_date = message.date.strftime("%Y-%m-%d")
            event_time = message.date.strftime("%H:%M:%S")
            messages.append({
                'security_area': 'Unknown',  # Placeholder, update if possible
                'region': 'Unknown',  # Placeholder, update if needed
                'city_town_area': 'Unknown',  # Placeholder
                'event_date': event_date,
                'event_time': event_time,
                'source_message_original': message.message,
                'source_message_translated': '',  # Placeholder for future translation
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
    return messages

async def fetch_all_messages(config):
    """Fetch messages from all channels specified in the config."""
    telegram_config = config["telegram"]
    client = TelegramClient('session', telegram_config["api_id"], telegram_config["api_hash"])
    await client.start()

    all_messages = []
    for channel in telegram_config["channels"]:
        print(f"Fetching messages from channel: {channel}")
        msgs = await fetch_messages_from_channel(client, channel, limit=100)
        all_messages.extend(msgs)

    await client.disconnect()
    return all_messages

# ----------------------------
# Main Function
# ----------------------------
async def main():
    # Load configuration from file.
    config = load_config("config.json")

    # Connect to PostgreSQL (without creating a table).
    conn = create_db_connection(config["postgres"])

    # Fetch messages from Telegram.
    messages = await fetch_all_messages(config)

    # Insert messages into the database.
    insert_messages(conn, messages)
    conn.close()

# ----------------------------
# Entry Point
# ----------------------------
if __name__ == '__main__':
    asyncio.run(main())
