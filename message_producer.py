import threading
import uuid
import random
import string
import time
import hashlib
import base64
import json

class MessageProducer():
	
	def __init__(self, queue_controller, frequency=1.0, log_filename='produced_messages.log'):
		self.frequency = frequency # messages produced per second
		self.log_file = open(log_filename, 'w')
		self.queue_controller = queue_controller
		self.logger = queue_controller.logger
		self.total_produced = 0 # total count of messages produced

		# schedule message generation
		threading.Timer(1.0 / self.frequency, self.generate_message).start()
		# schedule loggin
		threading.Timer(1.0 / self.logger.frequency, self.log).start()
	
	def __del__(self):
		self.log_file.close()
		
	def generate_message(self):
		# unique message id
		message_id = str(uuid.uuid4())

		# generate random message content (uppercase ASCII string of 50 characters)  
		message_content = ''.join(random.choices(string.ascii_uppercase, k=50))

		message = {
			'id': message_id,
			'content': message_content 
		}

		# log message content in JSON format separated by newline
		self.log_file.write(json.dumps(message))
		self.log_file.write('\n')
		self.log_file.flush()

		encoded_message = self.encode_message(message)

		self.queue_controller.send_message(encoded_message)
		self.total_produced += 1

		threading.Timer(1.0 / self.frequency, self.generate_message).start()

	def encode_message(self, message):
		# encoding format can be extended to transfer only strings
		# ...e.g. by separating payload and signature with a "."
		# therefore, the base64 encoding is not neccesary at the moment but allows for extension
		payload = base64.b64encode(json.dumps(message).encode('ascii'))
		signature = hashlib.sha256(payload).hexdigest()

		return {'payload': payload, 'signature': signature}

	def log(self):
		self.logger.log_produced_messages(self.total_produced)
		threading.Timer(1.0 / self.logger.frequency, self.log).start()