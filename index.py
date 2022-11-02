# -*- coding: utf-8 -*-
"""
Generic data fusion component.

This component is a generic data fusion component that can be used to
perform data fusion on time series data. It is based on the ability
of a timeseries database and python to produce enriched time series.
"""

# import libraries
import logging
import json
import argparse
import time
import datetime
import schedule
import numpy as np

# import data fusion specific librarires
from src.fusion.stream_fusion import batchFusion

# logger initialization
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

# data fusion version
__version__ = "1.0.0"

class TimeseriesDataFusionInstance():
    """
    Generic class for time series data fusion.
    """
    config: dict = None
    outputs: dict = None

    def __init__(self, config_path) -> None:
        """
        Initialize the data fusion instance.

        Parameters
        ----------
        config_path : str
            Path to the configuration file.

        Returns
        -------
        None.
        """

        LOGGER.info("Reading configuration file: %s", config_path)

        # load config
        with open(config_path, "r") as jsonfile:
            self.config = json.load(jsonfile)

        # TODO: create outputs

        LOGGER.info("Starting data fusion instance for %s", self.config["name"])

    def is_valid_feature_vector(self, feature_vector) -> bool:
        """
        Check if the feature vector is valid.

        Parameters
        ----------
        feature_vector : dict
            Feature vector to check.

        Returns
        -------
        bool
            True if the feature vector is valid, False otherwise.
        """

        # check if the feature vector is valid
        if feature_vector is None:
            return False

        # check if the feature vector is empty
        if not feature_vector:
            return False

        # check if the feature vector contains only NaNs
        if np.isnan(feature_vector).all():
            return False

        # check if the feature vector contains only Infs
        if np.isinf(feature_vector).all():
            return False

        return True

    def run_batch_fusion_once(self) -> None:
        """
        Run the data fusion instance once.

        Parameters
        ----------
        config : dict
            Configuration dictionary.

        Returns
        -------
        None.
        """

        for fusion in self.config["fusions"]:
            # obtain last successful timestamp

            # initiate the batch fusion
            batch_fusion = batchFusion(fusion)

            update_outputs = True
            try:
                feature_vector, timestamps = batch_fusion.buildFeatureVectors()
            except Exception as no_vector_exception:
                LOGGER.error("Error while building feature vectors: %s", str(no_vector_exception))
                update_outputs = False

            # if feature vector was successfully built, then update outputs
            if update_outputs:
                # iterate through possible outputs
                for output in self.config["outputs"]:

                    # iterate through the generated feature vectors
                    for j in range(timestamps.shape[0]):
                        # generating timestamp and timestamp in readable form
                        generated_ts = int(timestamps[j].astype('uint64')/1000000)
                        ts_string = datetime.datetime.utcfromtimestamp(generated_ts / 1000).strftime("%Y-%m-%dT%H:%M:%S")

                        # generating ouput location
                        location = fusion["id"]
                        # generating output message
                        output = {"timestamp": generated_ts, "ftr_vector": list(feature_vector[j])}

                        # check if feature vectors are valid
                        if self.is_valid_feature_vector(feature_vector):
                            # send feature vector to specified output
                            self.outputs[output].send_feature_vector(location, output)
                        else:
                            LOGGER.warning("Feature vector for [%s] at [%s] is not valid: %s", ts_string, location, json.dumps(feature_vector))

                        # populate all the outputs with the new feature vectors

    def run(self) -> None:
        """
        Run the data fusion instance.

        Returns
        -------
        None.
        """

        # create hourly scheduler
        schedule.every().hour.do(self.run_batch_fusion_once)
        self.run_batch_fusion_once()
        LOGGER.info('Component started successfully.')

        # checking scheduler
        while True:
            schedule.run_pending()
            time.sleep(1)

def main() -> None:
    """
    Main function.

    Returns
    -------
    None.
    """

    # extract command line parameters
    parser = argparse.ArgumentParser()
    # required parameters
    parser.add_argument("--config", help="Path to the configuration file", required=True)
    args = parser.parse_args()

    LOGGER.info("Starting data fusion component v.%s", __version__)

    # initialize data fusion instance
    data_fusion_instance = TimeseriesDataFusionInstance(args.config)

    # start data fusion
    data_fusion_instance.run()

# run main
if __name__ == "__main__":
    main()
