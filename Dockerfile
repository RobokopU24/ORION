# A docker container with java and python for Data Services

FROM openjdk:slim
COPY --from=python:3.9 / /

COPY ./requirements.txt /Data_services/requirements.txt

RUN pip install -r /Data_services/requirements.txt

COPY . /Data_services/.

ENV PYTHONPATH "${PYTHONPATH}:/Data_services"

CMD ["python", "/Data_services/Common/load_manager.py"]