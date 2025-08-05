# airflow-etl-design

### Run on Local

#### Docker local setup architecture

<img width="825" height="498" alt="Screenshot 2025-08-05 at 2 03 30 PM" src="https://github.com/user-attachments/assets/8795ab12-8a2e-4144-a926-59fe7e917914" />


#### Pre-requisites
- Install docker desktop
- Install python (Recommended version >= 3.12)
- Clone this github repo and cd inside it

#### Commands
Create virtualenv and source it. The purpose of this virtual env can be used by the editor
to help with code completions.
```
virtualenv <your_dir>/airflow-etl-design
source <your_dir>/airflow-etl-design/bin/activate
```
Setup airflow user id for use by docker compose so that files and folders created insider docker containers are not with root ownership.
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
Build local docker image
```
docker-compose build --no-cache
```
Start all the docker containers
```
docker-compose up
```

#### Dashboard
Airflow Dashboard - http://localhost:8080/

##### Login Credentials for local

Username: airflow | Password: airflow
