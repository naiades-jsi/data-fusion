from kafka import KafkaProducer
import json
import logging

# logger initialization
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

class DataFusionOutput():

    def __init__(self) -> None:
        pass

    def send(self, feature_vector, location) -> bool:
        """
        Send a feature vector to the specific output.

        Parameters
        ----------
        feature_vector : dict
            Feature vector to send.
        location : str
            Location of the feature vector.

        Returns
        -------
        bool
            True if the feature vector was sent successfully, False otherwise.

        """
        pass

class KafkaDataFusionOutput(DataFusionOutput):

    def __init__(self, config) -> None:
        """
        Initialize the Kafka data fusion output.

        Parameters
        ----------
        config : dict
            Configuration of the Kafka data fusion output.

        Returns
        -------
        None.
        """

        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers = config["bootstrap_servers"],
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, feature_vector, location) -> bool:
        """
        Send a feature vector to the specific output.

        Parameters
        ----------
        feature_vector : dict
            Feature vector to send.
        location : str
            Location of the feature vector.

        Returns
        -------
        bool
            True if the feature vector was sent successfully, False otherwise.

        """

        # send the feature vector to the Kafka topic
        output_topic = f"features_{location}"
        future = self.producer.send(output_topic, feature_vector)

        try:
            record_metadata = future.get(timeout=10)
            LOGGER.info("Feature vector sent to Kafka topic %s", output_topic)
        except Exception as producer_exception:
            LOGGER.error("Producer exception: %s", str(producer_exception))

        return True