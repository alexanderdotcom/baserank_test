from message_producer import MessageProducer
from message_receiver import MessageReceiver
from message_logger import MessageLogger
from message_queue_controller import MessageQueueController
import sys

'''
Expect JSON logs separated by newlines
Log format is:

{
	time: TIMESTAMP,
	generated: {
		new: MESSAGES GENERATED SINCE LAST LOG,
		total: TOTAL GENERATED MESSAGES
	},
	received: {
		new: MESSAGES RECEIVED SINCE LAST LOG,
		total: TOTAL RECEIVED MESSAGES
	},
	queued: {
		INSTANCE ID: {
			length: TOTAL QUEUE LENGTH
			difference: CHANGE IN QUEUE LENGTH SINCE LAST LOG
		},
		...
	}
}

'''

if __name__ == '__main__':
	producer_frequency = int(sys.argv[1]) if len(sys.argv) > 1 else 1.0
	receiver_frequency = 1.0

	logger = MessageLogger(frequency=1) # log one per second
	queue_controller = MessageQueueController(logger)
	producer = MessageProducer(queue_controller, frequency = producer_frequency)
	receiver = MessageReceiver(queue_controller, frequency = receiver_frequency)