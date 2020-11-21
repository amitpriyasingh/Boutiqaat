#!/bin/sh


$(aws ecr get-login --no-include-email --region eu-west-1)
docker build --rm -t btq-airflow .
docker tag btq-airflow:latest 652586300051.dkr.ecr.eu-west-1.amazonaws.com/btq-airflow:latest
docker push 652586300051.dkr.ecr.eu-west-1.amazonaws.com/btq-airflow:latest
aws ecs update-service --cluster bi-airflow-celery --service worker --task-definition airflow-worker:9 --force-new-deployment
aws ecs update-service --cluster bi-airflow-celery --service scheduler --task-definition airflow-scheduler:8 --force-new-deployment
aws ecs update-service --cluster bi-airflow-celery --service webserver --task-definition airflow-webserver:14 --force-new-deployment

