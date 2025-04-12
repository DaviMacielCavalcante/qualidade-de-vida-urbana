FROM apache/airflow:2.10.5 AS projeto_pi
RUN pip install uv 
COPY requirements.txt .
RUN uv pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt