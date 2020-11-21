#                           Tested On Mozila & Chrome.
# event-app

Environment info:

Description: Ubuntu 16.04

Release: 16.04.3 LTS (GNU/Linux 4.4.0-1055-aws x86_64)

Python Version: Python 3.5.2

Pip Version: pip 10.0.1

Setuptools Version: setuptools 39.1.0

MySQL Version: Ver 14.14 Distrib 5.7.21


To deploy:

#Clone this repository

Within the project folder

pip3 install virtualenv

#Create a virtual environment.

python3 -m virtualenv venv

#Activate venv.

. ./venv/bin/activate

#Install required packages.

pip install -r requirements.txt

# Add Database Credentials

Provide database credential in event_management/credential.py file. 

Refer to credential-sample.py for format.

#Required Tables to be populated in same DB defined in credential.py

ERP_SKU, ERP_CELEBRITY, magento_celeb_prod

#Before deployment Run (One time only)

cd event_management/

python manage.py makemigrations

python manage.py migrate

python manage.py createsuperuser

cd ..

# For Deployment

source emt.sh



Documentation for Managing Users /Groups

https://djangobook.com/managing-users-admin/


