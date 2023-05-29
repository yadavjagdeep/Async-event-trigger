from confluent_kafka import Consumer, KafkaError
from src.utility.logging_util import logger
from abc import ABC, abstractmethod
from graceful_death import GracefulDeath


class BaseKafkaConsumer(ABC):
    """Abstract base class for Kafka consumer."""

    def __init__(self, consumer_config: dict, topic_names: list, poll_timeout=None):
        """
        Initializes the BaseKafkaConsumer.
        Args:
        - consumer_config (dict): A dictionary of configurations to initialize the Kafka consumer.
        - topic_names (list): A list of topic names to consume from.
        - poll_timeout (int): The timeout in seconds for polling messages from Kafka consumer.
        """
        self.run_consumer = None
        self.consumer_config = consumer_config
        self.topic_names = topic_names
        self.poll_timeout = poll_timeout
        self.consumer = self.getConsumer()
        self.graceful_death = GracefulDeath()
        self._startConsumer()  # start consumer

    def _startConsumer(self):
        """
        - private method should not be triggered from outside this class
        - Starts the Kafka consumer and continuously polls for messages
        """

        self.subscribeTopic(self.topic_names)
        logger.log_info(f"Kafka consumer started for topics  = {self.topic_names}")
        self.run_consumer = True
        while self.run_consumer:
            try:
                message = self.pullMessage()
                if message is None:
                    if self.shouldContinue():
                        continue
                    else:
                        self.setNullMessageBehaviour()
                if message.error():
                    self.handleMessageError(message)
                self.processMessage(message)
                self.commitOffset()

                if self.graceful_death.received_term_signal:
                    self.stop_kafka()
            except Exception as e:
                logger.log_error(f"ERROR KAFKA CONSUMER: {str(e)}")
                if self.graceful_death.received_term_signal:
                    self.stop_kafka()
                    logger.log_error("Closed Kafka consumer")

    def commitOffset(self):
        """Commits the message offset to Kafka broker"""

        if self.consumer_config.get("enable.auto.commit", False) is False:
            try:
                self.consumer.commit(asynchronous=False)
            except Exception as e:
                logger.log_error(f"ERROR Kafka offset commit, {str(e)}")

    def handleMessageError(self, message):
        """Handles any error encountered during message consumption"""

        if message.error().code() == KafkaError._PARTITION_EOF:  # This exception is raised to indicate, consumer that it has reached the end of a partition and that there are no more messages to read
            logger.error(f"ERROR KAFKA PARTITION Error,partition = {message.partition()}, offset = {message.offset()}")
        else:
            raise Exception(f"Error encountered in msg consume from kafka {str(message.error())}")

    def subscribeTopic(self, topic_names: list):
        """
        Subscribes to Kafka topics
        - override in child class to set custom behavior
        """
        return self.consumer.subscribe(topic_names)

    def getConsumer(self):
        """
        Creates a new instance of Kafka consumer
        - override in child class to set custom behavior
        """
        return Consumer(self.consumer_config)

    def pullMessage(self):
        """
        Polls for new message from Kafka broker
        - to use self.consumer.consumer() or set custom timing override
        """
        return self.consumer.poll(self.poll_timeout if self.poll_timeout else 1.0)

    def shouldContinue(self):
        return True

    def stop_kafka(self):
        """  Stops the Kafka consumer """
        if self.consumer:
            self.consumer.close()
        self.run_consumer = False

    def setNullMessageBehaviour(self):
        """ In case of null message if not needed to continue override this to set behaviour"""
        raise Exception("Implement to set nullMessageBehaviour")

    @abstractmethod
    def processMessage(self, message):
        """Abstract method to process consumed messages. Must be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement this method")
