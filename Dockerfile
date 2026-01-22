FROM apache/airflow:3.1.1
COPY requirements.txt .
RUN pip install -r requirements.txt 