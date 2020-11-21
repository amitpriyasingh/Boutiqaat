#!/bin/sh

$(aws ecr get-login --no-include-email --region eu-west-1)
docker build --rm -t boutiqaat-etl .
docker tag boutiqaat-etl:latest 652586300051.dkr.ecr.eu-west-1.amazonaws.com/boutiqaat-etl:latest
docker push 652586300051.dkr.ecr.eu-west-1.amazonaws.com/boutiqaat-etl:latest
