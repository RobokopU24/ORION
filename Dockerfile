# A docker container with neo4j, java and python for Data Services
FROM neo4j:4.4.10

RUN apt-get update  \
    && apt-get -y install python3 \
    && apt-get -y install python-is-python3 \
    && apt-get -y install python3-pip \
    && apt-get -y install git \
    && apt-get -y install vim

COPY ./requirements.txt /Data_services/requirements.txt

RUN pip3 install -r /Data_services/requirements.txt

COPY . /Data_services/.

RUN chmod -R 777 /Data_services


ENV PYTHONPATH "$PYTHONPATH:/Data_services"