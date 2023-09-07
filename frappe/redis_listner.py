import frappe
import json
import redis

r = redis.Redis(host="localhost", port=11001, db=0)
pubsub = r.pubsub()
pubsub.subscribe("vehicle")
for message in pubsub.listen():
	if message['type'] == 'message':
		print(message['data'].decode("utf-8"))
		data = json.loads(message['data'].decode("utf-8"))
		doc = frappe.get_doc({
			'doctype': 'Contracts',
			'vehicle_id': data['vehicle_id'],
			'status':data['status'],
		})
		doc.insert()
		print("Document updated")