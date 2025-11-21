# book_scrapy/pipelines.py
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import re
import sqlite3
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class BookScrapyPipeline:
    # Tasso di cambio fisso £ → € (puoi aggiornarlo quando vuoi)
    exchange_rate = 1.16

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # 1. Converti in minuscolo (in modo sicuro, gestendo valori None)
        lowercase_keys = ['categoria_prodotto', 'rating', 'recensioni', 'tipo_prodotto']
        for key in lowercase_keys:
            value = adapter.get(key)
            if value and isinstance(value, str):
                adapter[key] = value.strip().lower()

        # 2. Converti prezzo da sterline a euro (rimuove £ e converte in float)
        prezzo_raw = adapter.get('prezzo')
        if prezzo_raw and isinstance(prezzo_raw, str):
            try:
                prezzo_sterline = float(re.sub(r'[^\d.]', '', prezzo_raw))
                adapter['prezzo'] = round(prezzo_sterline * self.exchange_rate, 2)
            except ValueError:
                spider.logger.warning(f"Impossibile convertire il prezzo: {prezzo_raw}")
                adapter['prezzo'] = 0.0
        else:
            adapter['prezzo'] = 0.0

        # 3. Estrai quantità disponibile (es. "In stock (22 available)" → 22)
        availability = adapter.get('disponibilita', '')
        if availability and '(' in availability:
            try:
                num_str = availability.split('(')[1].split()[0]
                adapter['disponibilita'] = int(num_str)
            except (IndexError, ValueError):
                adapter['disponibilita'] = 0
        else:
            adapter['disponibilita'] = 0

        # 4. Converte rating testuale → numero intero
        rating_text = adapter.get('rating', '').lower()
        rating_map = {
            'zero': 0, 'one': 1, 'two': 2,
            'three': 3, 'four': 4, 'five': 5
        }
        adapter['rating'] = rating_map.get(rating_text, 0)

        # 5. Converte numero recensioni in intero (alcune pagine hanno "0")
        recensioni_raw = adapter.get('recensioni', '0')
        try:
            adapter['recensioni'] = int(recensioni_raw) if recensioni_raw else 0
        except (ValueError, TypeError):
            adapter['recensioni'] = 0

        return item  # SEMPRE restituire l'item!


class SaveSqlLitePipeline:
    def __init__(self):
        self.conn = sqlite3.connect('DB_libri.db')
        self.cur = self.conn.cursor()

        # Creazione tabella (se non esiste)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS libri (
                titolo TEXT,
                prezzo REAL,
                rating INTEGER,
                tipo_prodotto TEXT,
                categoria_prodotto TEXT,
                recensioni INTEGER,
                disponibilita INTEGER
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        try:
            self.cur.execute("""
                INSERT INTO libri 
                (titolo, prezzo, rating, tipo_prodotto, categoria_prodotto, recensioni, disponibilita)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get('titolo'),
                item.get('prezzo'),
                item.get('rating'),
                item.get('tipo_prodotto'),
                item.get('categoria_prodotto'),
                item.get('recensioni'),
                item.get('disponibilita')
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            spider.logger.error(f"Errore SQLite: {e}")
            raise DropItem(f"Errore DB: {e}")

        return item  # Importantissimo: restituire l'item!

    def close_spider(self, spider):
        self.conn.close()