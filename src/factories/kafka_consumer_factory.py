from managers.kafka_consumer_manager.trigger_api_consumer import TriggerApis


class KafkaConsumerFactory:

    @staticmethod
    def get_consumer(consumer_name: str):
        if consumer_name == "trigger_apis":
            return TriggerApis
        else:
            raise Exception("Invalid kafka consumer !!!")
