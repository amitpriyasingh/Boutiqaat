FROM gusto/sqooperduper

RUN apk add --update \
     bash \
     curl \
     git \
     zip \
     python3 \
     coreutils \
     build-base \
     py-pip \
     python-dev \
     libffi-dev \
     openssl-dev \
     openssh \
     postgresql \
     postgresql-contrib \
     mysql-client \
     && rm -rf /var/cache/apk/*

RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.1-1_amd64.apk && \
     curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/mssql-tools_17.5.2.1-1_amd64.apk
RUN apk add --allow-untrusted msodbcsql17_17.5.2.1-1_amd64.apk && \
     apk add --allow-untrusted mssql-tools_17.5.2.1-1_amd64.apk

RUN pip3 install --upgrade jinja2 s3cmd awscli
RUN pip3 install git+https://github.com/omkay/pybashutil@master

#Install gcloud tool
RUN curl -o \
      google-cloud-sdk.tar.gz https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-272.0.0-linux-x86_64.tar.gz && \
      tar -xzf google-cloud-sdk.tar.gz && \
      rm google-cloud-sdk.tar.gz
RUN google-cloud-sdk/install.sh --path-update=true --bash-completion=true --rc-path=/.bashrc

RUN curl -o \
     amazon-redshift-jdbc-41.jar https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.32.1056/RedshiftJDBC41-no-awssdk-1.2.32.1056.jar && \
     mv amazon-redshift-jdbc-41.jar /usr/local/sqoop/lib

RUN echo -e '#!/bin/bash\nsource /credentials/var.env\npsql "host=${REDSHIFT_HOST} user=${REDSHIFT_USER} dbname=${REDSHIFT_DB} port=${REDSHIFT_PORT} password=${REDSHIFT_PASSWORD}" "$@"' > /usr/bin/redshift && \
    chmod +x /usr/bin/redshift

RUN echo -e '#!/bin/bash\n source /credentials/var.env\n/opt/mssql-tools/bin/sqlcmd -S ${MSSQL_CLI_SERVER} -U ${MSSQL_CLI_USER} -P ${MSSQL_CLI_PASSWORD}  -d ${MSSQL_CLI_DATABASE}  "$@"' > /usr/bin/navcli && \
     chmod +x /usr/bin/navcli

RUN export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
ENV AWS_SHARED_CREDENTIALS_FILE /credentials/aws_credentials
ENV GOOGLE_APPLICATION_CREDENTIALS /credentials/credentials.json
RUN touch /root/.bigqueryrc
RUN ln -s /credentials/aws_credentials /root/.boto

COPY . /src
RUN chmod +x /src/launch.sh
WORKDIR /src

ENTRYPOINT ["sh","/src/launch.sh"]