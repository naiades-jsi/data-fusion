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
# CMD ["python3", "index.NAIADES.alicante_forecasting.py"]
# CMD ["python3", "index.NAIADES.alicante_features_raw.py"]
# CMD ["python3", "index.NAIADES.alicante_level.py"]
# CMD ["python3", "index.NAIADES.alicante_level_freq.py"]
CMD ["python3", "index.NAIADES.carouge_sm.py"]