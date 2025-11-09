FROM apache/airflow:3.1.1
RUN pip install uv 
COPY pyproject.toml .
RUN uv sync