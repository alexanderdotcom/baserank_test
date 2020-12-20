from message_queue import MessageQueue
import random

'''
This class represents a helper component
It stores the queue instances and (directly) scales if neccessary

'''

class MessageQueueController():

	def __init__(self, logger):
		self.logger = logger
		# create a single queue at initialisation
		self.queues = self.queues = [MessageQueue(self.logger)]

	def send_message(self, encoded_message):
		# shuffle indices to balance load on instances
		queue_indices = list(range(len(self.queues)))
		random.shuffle(queue_indices)

		for queue_index in queue_indices:
			if self.queues[queue_index].ready(): # check if capacity overflown
				self.queues[queue_index].put(encoded_message)
				return True # message sent successfully
		
		# if overflow, create new queue and append message to it
		new_queue = MessageQueue(self.logger)
		new_queue.put(encoded_message)

		self.queues.append(new_queue)

		return False # overflow

	def get_messages(self):
		messages = []

		for queue in self.queues:
			while not queue.empty() and queue.ready():
				messages.append(queue.get())
				
		return messages

	