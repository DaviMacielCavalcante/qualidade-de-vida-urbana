FROM apache/airflow:3.0.2 AS projeto_pi
RUN pip install uv 
COPY requirements.txt .
RUN uv pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt