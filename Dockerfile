# A docker container with java and python for Data Services

FROM openjdk:slim
COPY --from=python:3.9 / /

COPY ./requirements.txt /Data_services/requirements.txt

RUN pip install -r /Data_services/requirements.txt

ARG UID=1000
ARG GID=1000
ARG DS_USER=ds_user
RUN groupadd -f --gid $GID $DS_USER
RUN useradd -o --uid $UID --gid $GID -m $DS_USER

USER $DS_USER

COPY . /Data_services/.

ENV PYTHONPATH "/Data_services"

CMD ["python", "/Data_services/Common/build_manager.py"]