# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import sqlite3
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class BookScrapyPipeline:

    # attributo statico di classe
    exchange_rate = 1.16

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Converto le maiuscole in minuscole
        lowercase_keys = ['categoria_prodotto', 'rating', 'recensioni', 'tipo_prodotto']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

         # Coverto sterline in euro togliendo il simbolo e salvando come float
        prezzo_in_sterline = adapter.get('prezzo')
        if prezzo_in_sterline:
            # Rimuovi il simbolo di sterlina e converti a float
            prezzo_in_sterline = float(re.sub(r'[£,]', '', prezzo_in_sterline))
            # Converti in euro
            prezzo_in_euro = prezzo_in_sterline * self.exchange_rate
            # Salva il valore convertito nell'item come float
            adapter['prezzo'] = round(prezzo_in_euro, 2)

        # estrapolazione del numero di copie disponibili
        availability_string = adapter.get('disponibilita')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['disponibilita'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['disponibilita'] = int(availability_array[0])

        #converto il rating in un numero intero
        stars_text_value = adapter.get('rating')
        if stars_text_value == "zero":
            adapter['rating'] = 0
        elif stars_text_value == "one":
            adapter['rating'] = 1
        elif stars_text_value == "two":
            adapter['rating'] = 2
        elif stars_text_value == "three":
            adapter['rating'] = 3
        elif stars_text_value == "four":
            adapter['rating'] = 4
        elif stars_text_value == "five":
            adapter['rating'] = 5

        return item
    
class SaveSqlLitePipeline:

    def __init__(self):
        self.conn = sqlite3.connect('DB_libri.db')
        self.c = self.conn.cursor()

        self.c.execute("""CREATE TABLE IF NOT EXISTS libri (
		titolo text,
		prezzo real,
		rating integer,
		tipo_prodotto text,
		categoria_prodotto text,
		recensioni integer,
        disponibilita integer               
		)""")
        self.conn.commit()

    def process_item(self, item, spider):
        # Inserimento dei dati nel database
        try:
            self.c.execute("""
            INSERT INTO libri (titolo, prezzo, rating, tipo_prodotto, categoria_prodotto, recensioni, disponibilita)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get('titolo'),               # titolo
                item.get('prezzo'),               # prezzo
                item.get('rating'),               # rating
                item.get('tipo_prodotto'),        # tipo_prodotto
                item.get('categoria_prodotto'),   # categoria_prodotto
                item.get('recensioni'),           # recensioni
                item.get('disponibilita')         # disponibilita
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            # Se c'è un errore, lancia una DropItem exception
            raise DropItem(f"Errore durante l'inserimento dell'item nel database: {e}")

        return item  # Restituisci l'item per continuare la pipeline

    def close_spider(self, spider):
        # Chiude la connessione al database quando lo spider si chiude
        self.conn.close()




    





