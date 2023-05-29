import os
from base_kafka_consumer import BaseKafkaConsumer
from utility.logging_util import logger


class TriggerApis(BaseKafkaConsumer):

    def __init__(self):
        super().__init__(
            self.__consumer_config(),
            self.__get_kafka_topics(),
            self.__get_poll_timeout()
        )

    def processMessage(self, message):
        logger.info(f"Inside class = {self.__class__.__name__} and message = {message}")

    @classmethod
    def __consumer_config(cls):
        _config = {

        }
        return _config

    @classmethod
    def __get_kafka_topics(cls):
        topics = os.environ["KAFKA_TOPICS"].split(',')  # comma separated topics
        return topics

    @classmethod
    def __get_poll_timeout(cls):
        poll_timeout = os.environ.get("POLL_TIMEOUT", None)
        return poll_timeout
