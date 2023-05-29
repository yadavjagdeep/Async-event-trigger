from confluent_kafka import Producer
from utility.logging_util import logger


class BaseKafkaProducer:

    def __init__(self, producer_config: dict):
        self.producer = self.__get_producer(producer_config)

    @classmethod
    def __get_producer(cls, producer_config: dict):
        return Producer(producer_config)

    def write_data_to_kafka(self, key, data, kafka_topic):
        logger.info(f"Pushing data to kafka, key = {key}, topic = {kafka_topic}, message = {data}")
        self.producer.produce(kafka_topic, data, key, callback=lambda err, msg: self.__on_delivery(err, msg, key))

    @staticmethod
    def __on_delivery(error, message, key):
        if error:
            logger.error(f"Failed to push data to kafka for key = {key}, error = {error}")
        else:
            logger.info(f"kafka push successes for key = {key}, message = {message.value()}")
