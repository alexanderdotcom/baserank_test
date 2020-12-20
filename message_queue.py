import queue
import time
import uuid
import threading

class MessageQueue(queue.Queue):
	def __init__(self, logger, throughput=1.0):
		super().__init__()
		self.id = str(uuid.uuid4())
		self.throughput = throughput # number of allowed "get" or "put" per second
		self.last_used = 0.0
		self.logger = logger
		self.previous_length = 0 # length at last log
		self.length = 0 # current length

		# schedule logging
		threading.Timer(1.0 / self.logger.frequency, self.log).start()
		
	def ready(self):
		# ready if the queue was last used at least "throughput" seconds ago
		return self.last_used <= time.time() - self.throughput
	
	def get(self):
		self.length -= 1
		self.last_used = time.time()
		return super().get()

	def put(self, message):
		self.length += 1
		self.last_used = time.time()
		super().put(message)

	def log(self):
		self.logger.log_queue_info(self.id, {
			'length': self.length,
			'difference': self.length - self.previous_length
		})

		self.previous_length = self.length

		threading.Timer(1.0 / self.logger.frequency, self.log).start()