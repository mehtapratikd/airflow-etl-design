FROM apache/airflow:3.0.3

# env
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"

# Copy required files and folders
COPY requirements.txt /opt/airflow

# Install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
