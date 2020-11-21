Environment info:

Description: Ubuntu 16.04 

Release: 16.04.3 LTS (GNU/Linux 4.4.0-1055-aws x86_64)

Python Version: Python 2.7.12

Pip Version: pip 10.0.1 

Setuptools Version: setuptools 39.0.1




# BI Tool implemented on Apache Superset 

To deploy:

#Clone this repository

Within the project folder

#Install Dependencies.

sudo apt-get install build-essential libssl-dev libffi-dev python-dev python-pip libsasl2-dev libldap2-dev libmysqlclient-dev

pip install virtualenv

#Create a virtual environment.

python2 -m virtualenv venv

#Activate venv.

. ./venv/bin/activate

#Install required packages.

pip install -r requirements.txt

#Make a file superset_config.py for local configuration details
Add the line with MySQL username, password and host.
SQLALCHEMY_DATABASE_URI = 'mysql://username:password@localhost/superset'

For Increasing Webserver Timeout add the following lines in the superset_config.py file.
SUPERSET_WEBSERVER_TIMEOUT = 300 
SQLLAB_TIMEOUT = 300

For async queries -
SQLLAB_ASYNC_TIME_LIMIT_SEC = 60 * 60 * 6



#Create Admin for first time

fabmanager create-admin --app superset

#Initialize the database

python bi-app.py db upgrade

#Create default roles and permissions

python bi-app.py init

#Run the webserver in background

python bi-app.py runserver &



# Reset Password of a User
From within the project folder run the following command.

source reset_password.sh


#To Restart Server after Reboot

Schedule in crontab like:

@reboot ~/bi-app.run

<bi-app.run> :

cd ~/repos/bi-dev

. venv/bin/activate

nohup python bi-app.py runserver 2>bi-app.log 1>bi-app.out &

#Check MySQL service is up. If not restart server.




#Data Dictionary
https://projectbq8-my.sharepoint.com/personal/h_padode_boutiqaat_com/_layouts/15/Doc.aspx?sourcedoc=%7B021f0601-cf6e-491c-a31b-f4a005f254ee%7D&action=default&slrid=83b66e9e-c0cc-5000-c986-390d790060ab



