# PHAROS
Retrieve and parse PHAROS data. Write it to KGX files to load into a graph database.

Get latest.sql.gz from here: http://juniper.health.unm.edu/tcrd/download/

Run the following:
docker compose -f docker-compose-pharos.yaml up --build
zcat latest.sql.gz | mysql --host=127.0.0.1 --port=3306 -uds-user -pds-pass -D PHAROS