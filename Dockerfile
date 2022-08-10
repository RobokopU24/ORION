# A docker container with neo4j, java and python for Data Services
FROM neo4j:4.3.16

COPY ./requirements.txt /Data_services/requirements.txt

RUN apt update
RUN apt-get -y install rsync
RUN apt-get -y install python3
RUN apt-get -y install python-is-python3
RUN apt-get -y install python3-pip
RUN apt-get -y install git

RUN pip3 install -r /Data_services/requirements.txt

COPY . /Data_services/.

ENV PYTHONPATH "/Data_services"

CMD ["neo4j"]