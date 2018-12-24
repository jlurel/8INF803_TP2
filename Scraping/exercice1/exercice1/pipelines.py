# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import pymongo
from scrapy.exceptions import DropItem


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['name'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['name'])
            return item

class DropIfEmptyFieldPipeline(object):

    def process_item(self, item, spider):

        # to test if only "job_id" is empty,
        # change to:
        # if not(item["job_id"]):
        if not(item["name"]):
            raise DropItem()
        elif item["name"] == " ":
            raise DropItem()
        elif not(item["spells"]):
            raise DropItem()
        else:
            return item


class MongoPipeline(object):
    collection_name = 'monsters'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        # recupere les informations qu'on a defini dans settings.py
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        # initialisation du spider avec connexion a la base de donnees
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        # ajout de l'item dans la base de donnees
        self.db[self.collection_name].insert(dict(item))
        logging.info("Monster ajouté dans la base de données")
        return item