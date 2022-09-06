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
CMD ["python3", "index.NAIADES.alicante_forecasting.py"]

# e3ailab/df_alicante_features_raw_ircai
# CMD ["python3", "index.NAIADES.alicante_features_raw.py"]

# e3ailab/df_alicante_features_level_ircai -- is this really needed
# CMD ["python3", "index.NAIADES.alicante_level.py"]

# e3ailab/df_alicante_level_freq_ircai -- is this really needed
# CMD ["python3", "index.NAIADES.alicante_level_freq.py"]

# e3ailab/df_carouge_sm_ircai
# CMD ["python3", "index.NAIADES.carouge_sm.py"]