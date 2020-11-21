#!/usr/bin/env bash

: "${REDIS_HOST:="redis"}"
: "${REDIS_PORT:="6379"}"
: "${REDIS_PASSWORD:=""}"

: "${POSTGRES_HOST:="postgres"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_USER:="airflow"}"
: "${POSTGRES_PASSWORD:="airflow"}"
: "${POSTGRES_DB:="airflow"}"

# Defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Celery}Executor}"

AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"

AIRFLOW__CORE__REMOTE_LOGGING=True
AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER=s3://btq-etl/airflow/logs
AIRFLOW__CORE__REMOTE_LOG_CONN_ID=logs_s3

export \
  AIRFLOW_HOME \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__CORE__REMOTE_LOGGING \
  AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER \
  AIRFLOW__CORE__REMOTE_LOG_CONN_ID \
  AIRFLOW__SMTP__SMTP_HOST \
  AIRFLOW__SMTP__SMTP_MAIL_FROM \
  AIRFLOW__SMTP__SENDGRID_API_KEY \
  AIRFLOW__SMTP__SMTP_PASSWORD \
  AIRFLOW__SMTP__SMTP_PORT \
  AIRFLOW__SMTP__SMTP_USER \
  AWS_ACCESS_KEY_ID \
  AWS_SECRET_ACCESS_KEY \
  AWS_DEFAULT_REGION


# Load DAGs examples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

AIRFLOW__CELERY__BROKER_URL="redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1"


case "$1" in
  webserver)
    if [ -e "/requirements.txt" ]; then
      pip3 install --user --upgrade -r /requirements.txt
    fi

    # if [ -e "/create_admin.py" ]; then
    #   python3 /create_admin.py
    # fi

    #airflow resetdb
    airflow initdb
    #airflow upgradedb
    #airflow create_user -r Admin -u admin -e admin@example.com -f admin -l user -p test
    cp /webserver_config.py /usr/local/airflow/webserver_config.py
    exec airflow webserver
    ;;
  scheduler|flower|version)

    exec airflow "$@"
    ;;
  worker)
    if [ -e "/requirements.txt" ]; then
      pip3 install --user --upgrade -r /requirements.txt
    fi

    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
