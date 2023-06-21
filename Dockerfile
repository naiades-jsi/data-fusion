# BUILDING: docker build -t <container_name> .
# RUNNING: docker run <container_name> <python_program_path> <config_file_path>
# e.g. docker run --network="host" -p 8668:8668 lstm_alicante
FROM ubuntu:20.04
RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev
COPY ./requirements.txt /requirements.txt
WORKDIR /
RUN pip3 install -r requirements.txt
COPY . /

# e3ailab/df_alicante_forecasting_ircai
# CMD ["python3", "index.NAIADES.alicante_forecasting.py"]

# e3ailab/df_alicante_forecasting_w_ircai
# CMD ["python3", "index.NAIADES.alicante_forecasting_w.py"]

# e3ailab/df_alicante_features_raw_ircai
# CMD ["python3", "index.NAIADES.alicante_features_raw.py"]

# e3ailab/df_carouge_sm_ircai
CMD ["python3", "index.NAIADES.carouge_sm.py"]

# e3ailab/df_braila_forecasting_ircai
# CMD ["python3", "index.NAIADES.braila_forecasting.py"]

# e3ailab/df_braila_anomaly_flow_ircai
# CMD ["python3", "index.NAIADES.braila_anomaly_flow.py"]

# e3ailab/df_braila_leakage_approximate_ircai
# CMD ["python3", "index.NAIADES.leakage_pressure_updated.py"]

# OBSOLETE: e3ailab/df_alicante_level_frequency_ircai
# CMD ["python3", "index.NAIADES.alicante_level_frequency.py"]

# OBSOLETE: e3ailab/df_braila_anomaly_pressure_ircai
# CMD ["python3", "index.NAIADES.braila_anomaly_pressure.py"]
