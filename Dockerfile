# A docker container with neo4j, java and python for Data Services
FROM neo4j:4.4.34

RUN apt-get update  \
    && apt-get -y install python3 \
    && apt-get -y install python-is-python3 \
    && apt-get -y install python3-pip \
    && apt-get -y install git \
    && apt-get -y install vim

COPY ./requirements.txt /ORION/requirements.txt

RUN pip3 install -r /ORION/requirements.txt

COPY . /ORION/.

RUN chmod -R 777 /ORION

ENV PYTHONPATH "$PYTHONPATH:/ORION"