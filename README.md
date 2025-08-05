# airflow-etl-design

### Description

Implementing a scalable, fault-tolerant data pipeline for processing telemetry and configuration data from thousands of
IoT devices deployed across agricultural zones.

### System Design

<img width="1033" height="588" alt="Screenshot 2025-08-05 at 2 44 53 PM" src="https://github.com/user-attachments/assets/4130f4e3-e00d-424c-b148-25882b87f6f3" />


### Directory Structure
```
├── config/
│   ├── airflow_local_settings.py
│   └── airflow.cfg
├── dags/
│   └── iot/
│       ├── conftest.py
│       ├── device_metadata.py
│       ├── ingest/
│       │   ├── tasks.py
│       │   ├── tests/
│       │   │   ├── data_utils.py
│       │   │   ├── test_device_metadata.py
│       │   │   └── test_telemetry.py
│       │   └── utils.py
│       ├── load/
│       │   ├── tasks.py
│       │   └── tests/
│       │       └── test_device_metadata.py
│       ├── telemetry.py
│       ├── transform/
│       │   ├── tasks.py
│       │   └── tests/
│       └── validate/
│           ├── data_models.py
│           ├── exceptions.py
│           ├── tasks.py
│           └── tests/
│               └── test_device_metadata.py
├── docker-compose.yaml
├── Dockerfile
├── LICENSE
├── logs/
├── plugins/
├── README.md
├── requirements.txt
├── ruff.toml
└── scripts/
    ├── __init__.py
    ├── localstack_init.sh*
    └── testdata_init.py
```

### Run on Local

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

Login - Username: airflow | Password: airflow

Flower Dashboard (Monitoring) - http://localhost:5555/
