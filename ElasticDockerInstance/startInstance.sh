#!/bin/bash

sudo docker-compose up -d

sudo chmod 777 ../ElasticDockerInstance

sudo docker exec -it movieElasticDB exit

sudo docker cp movieElasticDB:/usr/share/elasticsearch/config/certs/http_ca.crt .



