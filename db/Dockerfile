FROM postgres

ADD ./create_database.sql /docker-entrypoint-initdb.d/
ADD ./insert_dummy_data.sql /docker-entrypoint-initdb.d/
