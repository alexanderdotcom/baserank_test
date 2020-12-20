import threading
import base64
import hashlib
import json

class MessageReceiver():
	def __init__(self, queue_controller, frequency=1.0, log_filename='received_messages.log'):
		self.queue_controller = queue_controller
		self.log_file = open(log_filename, 'w')
		self.frequency = frequency # number of messages attempted to receive per second
		self.total_received = 0
		self.logger = self.queue_controller.logger
		
		# schedule receiving messages
		threading.Timer(1.0 / self.frequency, self.get_messages).start()
		# schedule logging
		threading.Timer(1.0 / self.logger.frequency, self.log).start()

	def __del__(self):
		self.log_file.close()
		
	def decode_message(self, message): 
		# check if message is corrupted
		corrupted = hashlib.sha256(message['payload']).hexdigest() != message['signature']

		decoded_message = json.loads(base64.b64decode(message['payload']))
		
		return decoded_message, corrupted
	
	def get_messages(self):
		encoded_messages = self.queue_controller.get_messages()
		
		for encoded_message in encoded_messages:
			message, corrupted = self.decode_message(encoded_message)

			# corrupted messages are lost
			if not corrupted:
				self.total_received += 1
				self.log_file.write(json.dumps(message))
				self.log_file.write('\n')
		
		# clear buffer
		self.log_file.flush()

		threading.Timer(1.0 / self.frequency, self.get_messages).start()

	def log(self):
		self.logger.log_received_messages(self.total_received)
		threading.Timer(1.0 / self.logger.frequency, self.log).start()