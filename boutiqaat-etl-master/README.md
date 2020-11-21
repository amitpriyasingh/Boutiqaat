# Boutiqaat Data Pipelines ETL

## Local development
### 1- Setup
All you need for setup is to have a docker installed on your machine once that in place you can then build the docker image by running the following command 

```docker build -t boutiqaat-etl .```

### 2- Running commands

once the image has been built you can run the following command to run the docker image with the command required for running the rule, note that you need to mount credentials in order to run sqoop sync commands to the path `/credentials/` withtin the container.

```docker run -v /tmp/credentials:/credentials boutiqaat-etl make -C testfolder testrule```

where `-C` is used to run commands from Makefile within a folder in the working directory so `testfolder` is the folder name and `testrule` is the rule name

### 3- Credentials
the credentials required here is the following
* sqoop import option file which connects through jdbc driver
* s3cmd AWS s3 credentials file that has S3 access key and S3 secret key
* env variables that has S3 access key and S3 secret key to run `redshift` command

