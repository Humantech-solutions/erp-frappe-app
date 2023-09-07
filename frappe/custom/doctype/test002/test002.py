# Copyright (c) 2023, Frappe Technologies and contributors
# For license information, please see license.txt

# import frappe

from frappe.model.document import Document
from pymongo import MongoClient
from confluent_kafka import Producer
import json

CONNECTION_STRING = "mongodb+srv://api-user:e0gzMJct8pbUtJdl@cluster0.uwiy0um.mongodb.net/auth?retryWrites=true&w=majority"

def get_collections():
	client = MongoClient(CONNECTION_STRING)
	db = client["Custom"]
	collection = db["test002"]
	return collection



class test002(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		title: DF.Data | None
	# end: auto-generated types

	def load_from_db(self):
		docs = get_collections()
		d = docs.find_one({"name": self.name})
		super(Document, self).__init__(d)

	
	@staticmethod
	def get_list(args):
		docs = get_collections()
		d_list = []
		for doc in docs.find():
			d_list.append({**doc,
				"_id": str(doc["_id"])
				})
		return d_list

	def db_update(self):
		docs = get_collections()
		docs.update_one({"name": self.name}, {"$set": self.get_valid_dict()})

	def db_insert(self, *args, **kwargs):
		docs = get_collections()
		d = self.get_valid_dict()
		d.modified = self.modified
		docs.insert_one(d)

	def delete(self):
		collections = get_collections()
		collections.delete_one({"name": self.name})

	@staticmethod
	def get_count(args):
		docs = get_collections()
		count = docs.count_documents({})
		return count

	@staticmethod
	def get_stats(args):
		pass

	def on_update(self):
		topic = "events"
		producer_config = {"bootstrap.servers": "164.52.218.247:9092"}
		producer = Producer(producer_config)
		producer.poll(1)
		print("sending to topic: ", topic)
		producer.produce(topic, key="key", value=json.dumps({'docType':"test002",'doc':self.get_valid_dict()}).encode("utf-8"))
		producer.flush()

