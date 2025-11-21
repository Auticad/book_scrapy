# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BookScrapyItem(scrapy.Item):
    titolo = scrapy.Field()
    prezzo = scrapy.Field()
    rating = scrapy.Field()
    tipo_prodotto = scrapy.Field()
    categoria_prodotto = scrapy.Field()
    disponibilita = scrapy.Field()
    recensioni = scrapy.Field()
    
