import threading
import time

class MessageLogger():
    def __init__(self, frequency=1.0):
        self.frequency = frequency
        self.generated_total = 0 # total number of generated messages
        self.generated_new = 0  # number of messages generated since last log
        self.received_total = 0 # total number of received messages
        self.received_new = 0 # number of messages received since last log
        self.queue_info = {} # information for each queue instance

        # schedule logging
        threading.Timer(1.0 / self.frequency, self.log).start()
        
    # called by message producer
    def log_produced_messages(self, updated_total):
    	self.generated_new = updated_total - self.generated_total
    	self.generated_total = updated_total

    # called by message receiver
    def log_received_messages(self, updated_total):
    	self.received_new = updated_total - self.received_total
    	self.received_total = updated_total

    # called by queue instances
    def log_queue_info(self, queue_id, queue_info):
    	self.queue_info[queue_id] = queue_info

    def log(self):
    	print({
    		'time': time.time(),
    		'generated': {
    			'new': self.generated_new,
    			'total': self.generated_total
    		},
    		'received': {
    			'new': self.received_new,
    			'total': self.received_total
    		},
    		'queued': self.queue_info
    	})

    	self.generated_new = 0
    	self.received_new = 0

    	threading.Timer(1.0 / self.frequency, self.log).start()