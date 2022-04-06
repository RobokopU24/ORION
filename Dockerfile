# A docker container with java and python for Data Services

FROM openjdk:slim
COPY --from=python:3.9 / /

ARG UID=1000
ARG GID=1000
RUN groupadd -o -g $GID ds_user
RUN useradd -m -u $UID -g $GID -s /bin/bash ds_user

ENV USER=ds_user

COPY ./requirements.txt /Data_services/requirements.txt

RUN pip install -r /Data_services/requirements.txt

COPY . /Data_services/.

ENV PYTHONPATH "/Data_services"

CMD ["python", "/Data_services/Common/build_manager.py"]