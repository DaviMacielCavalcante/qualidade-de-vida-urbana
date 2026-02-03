from datetime import datetime as dt, timedelta
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, TaskGroup, Variable
from aws.secrets import get_parameter

@dag(
    start_date=dt(2025, 1, 6), 
    schedule=None, 
    catchup=False, 
    description='ETL for starting Weather Sync', 
    is_paused_upon_creation=False,
    tags=['setup', 'inmet'])
def app_setup():
    
    @task 
    def get_historical_inmet():
        import zipfile
        import requests
        import boto3
        from pyarrow import csv
        from pyarrow import parquet as pq
        from io import BytesIO
        from pipe.task_utils import most_recent
        
        years = list(range(2020,2026))

        metadata_inmet = {}

        s3_client = boto3.client(
                        's3',
                        aws_access_key_id = get_parameter("/tcc/dev/airflow_aws_s3_key_id"),
                        aws_secret_access_key = get_parameter("/tcc/dev/airflow_s3_secret"),
                        region_name = get_parameter("/tcc/dev/aws_region")
                    )


        bucket_name = get_parameter("/tcc/dev/aws_s3_bucket_bronze")

        for year in years:
            response = requests.get(f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip")
            zip_in_buffer = BytesIO(response.content)
            
            with zipfile.ZipFile(zip_in_buffer, "r") as zf:
                
                files = zf.namelist()
                castanhal_files = list(filter(lambda file: "A202" in file, files))
                most_recent_castanhal_file = most_recent(castanhal_files)
                castanhal_file_bytes = zf.read(most_recent_castanhal_file)
                rows = castanhal_file_bytes.decode('latin-1').split('\n')[:8]
                
                # Pra cada linha, separa por ":;" e pega a chave e valor
                for row in rows:
                    chunk = row.split(':;')
                    metadata_inmet[chunk[0]] = chunk[1]
                
                castanhal_file = BytesIO(zf.read(most_recent_castanhal_file))
                read_options = csv.ReadOptions(skip_rows=8, encoding="latin-1")
                parse_options = csv.ParseOptions(delimiter=";")
                castanhal_csv = csv.read_csv(castanhal_file, read_options=read_options, parse_options=parse_options)
                
            metadata = {
                b'layer': b'bronze',
                b'destiny': b'silver',
                b'fonte_nome': b'INMET',
                b'fonte_estacao': metadata_inmet['ESTACAO'].encode(),
                b'fonte_codigo': metadata_inmet['CODIGO (WMO)'].encode(),
                b'fonte_latitude': metadata_inmet['LATITUDE'].encode(),
                b'fonte_longitude': metadata_inmet['LONGITUDE'].encode(),
                b'fonte_altitude': metadata_inmet['ALTITUDE'].encode(),
                b'fonte_datetime_insert': dt.now().isoformat().encode()
            }

            table = castanhal_csv.replace_schema_metadata(metadata)


            now = dt.now()

            path_parquet_s3 = (
                f"{metadata[b'fonte_nome'].decode().lower()}/historical/{metadata[b'fonte_estacao'].decode().lower()}/"
                f"{metadata[b'fonte_estacao'].decode().lower()}-{year}-{now.isoformat()}.parquet"
            )

            buffer = BytesIO()

            pq.write_table(
                table,
                buffer,
                compression="snappy"
            )

            buffer.seek(0)

            s3_client.put_object(
                Body=buffer.getvalue(),
                Bucket=bucket_name,
                Key=path_parquet_s3    
            )
                
    @task
    def get_gap_inmet():
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as ec
        from selenium.webdriver.chrome.service import Service 
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager
        from io import BytesIO
        import time 
        import pyarrow as pa 
        import pyarrow.parquet as pq
        import boto3
        
        default_date = dt(dt.now().year, 1, 1).strftime("%Y-%m-%d")
        last_date_processed = dt.strptime(Variable.get("INMET_LAST_FETCH", default=default_date), "%Y-%m-%d")
        
        yesterday = dt.now() - timedelta(days=1)
        
        if last_date_processed <= yesterday:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")

            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            
            try:
                while last_date_processed <= yesterday:
                    driver.get("https://tempo.inmet.gov.br/TabelaEstacoes/")

                    menu_icon = WebDriverWait(driver, 10).until(
                        ec.element_to_be_clickable((By.CLASS_NAME, "bars"))
                    )

                    menu_icon.click()

                    time.sleep(2)

                    botao_estacao_automatica = WebDriverWait(driver, 10).until(
                        ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Automáticas')]"))
                    )

                    botao_estacao_automatica.click()

                    time.sleep(1)
                    
                    dropdown = "div.ui.search.selection.dropdown"

                    dropdowns = driver.find_elements(By.CSS_SELECTOR, dropdown)

                    dropdown_estado = dropdowns[1]  
                    dropdown_estado.click()
                        
                    opcao_para = WebDriverWait(driver, 10).until(
                        ec.element_to_be_clickable((By.XPATH, "//span[text()='Pará']"))
                    )

                    opcao_para.click()
                    time.sleep(2)

                    dropdowns_atualizados = driver.find_elements(By.CSS_SELECTOR, dropdown)

                    dropdown_estacao = dropdowns_atualizados[2] 
                    dropdown_estacao.click()

                    opcao_castanhal = WebDriverWait(driver, 10).until(
                        ec.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'CASTANHAL') and contains(text(), 'A202')]"))
                    )

                    opcao_castanhal.click()

                    campos_data = driver.find_elements(By.CSS_SELECTOR, "input[type='date']")

                    campo_data_inicio = campos_data[0]
                    campo_data_inicio.click()

                    data_fim_dt = min(last_date_processed + timedelta(days=180), dt.now())

                    data_inicio = last_date_processed.strftime("%d/%m/%Y")
                    data_fim = data_fim_dt.strftime("%d/%m/%Y")

                    campo_data_inicio.clear()
                    campo_data_inicio.send_keys(data_inicio)

                    time.sleep(5)

                    campo_data_fim = campos_data[1]
                    campo_data_fim.click()

                    campo_data_fim.clear()
                    campo_data_fim.send_keys(data_fim)

                    time.sleep(5)

                    btn_gerar_tabela = driver.find_element(By.XPATH, "//button[contains(text(), 'Gerar Tabela')]")

                    btn_gerar_tabela.click()

                    time.sleep(2)

                    tabela = WebDriverWait(driver, 10).until(
                        ec.presence_of_element_located((By.TAG_NAME, "table"))
                    )

                    # Dentro do <thead>, tem 2 <tr>
                    thead = tabela.find_element(By.TAG_NAME, "thead")
                    linhas_header = thead.find_elements(By.TAG_NAME, "tr")

                    # Primeira linha de headers
                    linha1 = linhas_header[0]
                    ths_linha1 = linha1.find_elements(By.TAG_NAME, "th")

                    # Segunda linha de headers
                    linha2 = linhas_header[1]
                    ths_linha2 = linha2.find_elements(By.TAG_NAME, "th")

                    mapeamento_headers = {
                        "Temperatura (°C)": "temp_c",
                        "Umidade (%)": "umid_perc",
                        "Pto. Orvalho (°C)": "pto_orvalho_c",  
                        "Pressão (hPa)": "press_hpa",
                        "Vento": "vento",
                        "Radiação": "rad",  
                        "Chuva": "chuva",
                        "Data": "data",
                        "Hora": "hora"
                    }

                    mapeamento_headers2 = {
                        "Inst.": "inst",
                        "Máx.": "max",   
                        "Mín.": "min",   
                        "Vel. (m/s)": "vel_ms",  
                        "Dir. (°)": "dir",   
                        "Raj. (m/s)": "raj",  
                        "Kj/m²": "kjm2",       
                        "mm": "mm",       
                        "UTC": "utc"  
                    }

                    headers_combinados = []
                    index_linha2 = 0 

                    tbody = tabela.find_element(By.TAG_NAME, "tbody")
                    linhas = tbody.find_elements(By.TAG_NAME, "tr")

                    inmet_table = []

                    for th_linha1 in ths_linha1:
                        nome_grupo = th_linha1.text
                        colspan = th_linha1.get_attribute('colspan')
                        nome_grupo = mapeamento_headers.get(nome_grupo, nome_grupo)
                        
                        # Se colspan é None, tratar como 1
                        if colspan is None:
                            colspan = 1
                        else:
                            colspan = int(colspan)
                        
                        # Pegar N headers da linha 2 (onde N = colspan)
                        for _ in range(colspan):
                            nome_detalhe = ths_linha2[index_linha2].text 
                            
                            nome_detalhe = mapeamento_headers2.get(nome_detalhe, nome_detalhe) 
                            
                            if nome_detalhe.strip() == "":
                                nome_final = nome_grupo
                            else:
                                nome_final = nome_grupo + "_" + nome_detalhe
                            
                            headers_combinados.append(nome_final)
                            index_linha2 += 1

                    for linha in linhas:
                        celulas = linha.find_elements(By.TAG_NAME, "td")
                        valores = [celula.text for celula in celulas]
                        inmet_table.append(valores)
                                
                        
                    inmet_campos_schema = [(header, pa.string()) for header in headers_combinados]
                    schema = pa.schema(inmet_campos_schema)
                        
                        
                    dados_sem_metadados_colunar = {
                        header: [linha[i] for linha in inmet_table]
                        for i, header in enumerate(headers_combinados)
                    }

                    table = pa.Table.from_pydict(dados_sem_metadados_colunar, schema=schema)

                    metadata = {
                        b'layer': b'bronze',
                        b'destiny': b'silver',
                        b'fonte_nome': b'INMET',
                        b'fonte_estacao': b'CASTANHAL',
                        b'fonte_codigo': b'A202',
                        b'fonte_datetime_insert': dt.now().isoformat().encode()
                    }

                    table = table.replace_schema_metadata(metadata)

                    agora = dt.now()
                    path_parquet_s3 = (
                        f"{metadata[b'fonte_nome'].decode().lower()}/gap/{metadata[b'fonte_estacao'].decode().lower()}/"
                        f"{metadata[b'fonte_estacao'].decode().lower()}-{agora.isoformat()}.parquet"
                    )

                    s3_client = boto3.client(
                        's3',
                        aws_access_key_id = get_parameter("/tcc/dev/airflow_aws_s3_key_id"),
                        aws_secret_access_key = get_parameter("/tcc/dev/airflow_s3_secret"),
                        region_name = get_parameter("/tcc/dev/aws_region")
                    )

                    bucket_name = get_parameter("/tcc/dev/aws_s3_bucket_bronze")
                    buffer = BytesIO()

                    pq.write_table(
                        table,
                        buffer,
                        compression="snappy"
                    )

                    buffer.seek(0)

                    s3_client.put_object(
                        Body=buffer.getvalue(),
                        Bucket=bucket_name,
                        Key=path_parquet_s3    
                    )
                        
                    Variable.set("INMET_LAST_FETCH", data_fim_dt.strftime("%Y-%m-%d"))
                    
                    return True    
            finally:
                driver.quit() 
        else:
            return True

    
    trigger_last_hour_dag = TriggerDagRunOperator(
        task_id="trigger_hourly_dag",
        trigger_dag_id="inmet_hourly",
        wait_for_completion=False
    )
    
    @task
    def unpause_hourly_dag():
        import requests
        
        base_url = Variable.get("BASE_URL", default="http://airflow-apiserver:8080")
        username = Variable.get("ADMIN_USER")
        password = Variable.get("ADMIN_PASSWORD")
        
        # Pega o token
        token_response = requests.post(
            f"{base_url}/auth/token",
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"}
        )
        
        if token_response.status_code != 201:
            raise Exception(f"Falha ao autenticar: {token_response.text}")
        
        token = token_response.json().get("access_token")
        
        # Ativa a DAG
        response = requests.patch(
            f"{base_url}/api/v2/dags/inmet_hourly?update_mask=is_paused",
            json={"is_paused": False},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}"
            }
        )
        
        if response.status_code == 200:
            return True
        else:
            raise Exception(f"Falha ao ativar DAG: {response.text}")
    
    get_historical_inmet() >> get_gap_inmet()>> unpause_hourly_dag() >> trigger_last_hour_dag
    
@dag(
    start_date=dt(2025, 1, 6),
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['air_quality']
)
def inmet_hourly():
        
    @task 
    def get_last_hour_inmet():
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as ec
        from selenium.webdriver.chrome.service import Service 
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager
        from io import BytesIO
        import time 
        import pyarrow as pa 
        import pyarrow.parquet as pq
        import boto3
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        driver.get("https://tempo.inmet.gov.br/TabelaEstacoes/")

        menu_icon = WebDriverWait(driver, 10).until(
            ec.element_to_be_clickable((By.CLASS_NAME, "bars"))
        )

        menu_icon.click()

        time.sleep(2)


        botao_estacao_automatica = WebDriverWait(driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Automáticas')]"))
        )
        
        dropdown = "div.ui.search.selection.dropdown"

        botao_estacao_automatica.click()

        time.sleep(1)

        dropdowns = driver.find_elements(By.CSS_SELECTOR, dropdown)

        dropdown_estado = dropdowns[1]  
        dropdown_estado.click()
            
        opcao_para = WebDriverWait(driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//span[text()='Pará']"))
        )

        opcao_para.click()
        time.sleep(2)

        dropdowns_atualizados = driver.find_elements(By.CSS_SELECTOR, dropdown)

        dropdown_estacao = dropdowns_atualizados[2] 
        dropdown_estacao.click()

        opcao_castanhal = WebDriverWait(driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'CASTANHAL') and contains(text(), 'A202')]"))
        )

        opcao_castanhal.click()

        campos_data = driver.find_elements(By.CSS_SELECTOR, "input[type='date']")

        campo_data_inicio = campos_data[0]
        campo_data_inicio.click()

        data_hoje = dt.now().strftime("%Y-%m-%d")

        driver.execute_script("arguments[0].value = arguments[1];", campo_data_inicio, data_hoje)


        campo_data_fim = campos_data[1]
        campo_data_fim.click()

        driver.execute_script("arguments[0].value = arguments[1];", campo_data_fim, data_hoje)

        btn_gerar_tabela = driver.find_element(By.XPATH, "//button[contains(text(), 'Gerar Tabela')]")

        btn_gerar_tabela.click()

        time.sleep(2)

        tabela = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.TAG_NAME, "table"))
        )

        # Dentro do <thead>, tem 2 <tr>
        thead = tabela.find_element(By.TAG_NAME, "thead")
        linhas_header = thead.find_elements(By.TAG_NAME, "tr")

        # Primeira linha de headers
        linha1 = linhas_header[0]
        ths_linha1 = linha1.find_elements(By.TAG_NAME, "th")

        # Segunda linha de headers
        linha2 = linhas_header[1]
        ths_linha2 = linha2.find_elements(By.TAG_NAME, "th")

        mapeamento_headers = {
            "Temperatura (°C)": "temp_c",
            "Umidade (%)": "umid_perc",
            "Pto. Orvalho (°C)": "pto_orvalho_c",  
            "Pressão (hPa)": "press_hpa",
            "Vento": "vento",
            "Radiação": "rad",  
            "Chuva": "chuva",
            "Data": "data",
            "Hora": "hora"
        }

        mapeamento_headers2 = {
            "Inst.": "inst",
            "Máx.": "max",   
            "Mín.": "min",   
            "Vel. (m/s)": "vel_ms",  
            "Dir. (°)": "dir",   
            "Raj. (m/s)": "raj",  
            "Kj/m²": "kjm2",       
            "mm": "mm",       
            "UTC": "utc"  
        }

        headers_combinados = []
        index_linha2 = 0 

        for th_linha1 in ths_linha1:
            nome_grupo = th_linha1.text
            colspan = th_linha1.get_attribute('colspan')
            nome_grupo = mapeamento_headers.get(nome_grupo, nome_grupo)
            
            # Se colspan é None, tratar como 1
            if colspan is None:
                colspan = 1
            else:
                colspan = int(colspan)
            
            # Pegar N headers da linha 2 (onde N = colspan)
            for _ in range(colspan):
                nome_detalhe = ths_linha2[index_linha2].text 
                
                nome_detalhe = mapeamento_headers2.get(nome_detalhe, nome_detalhe) 
                
                if nome_detalhe.strip() == "":
                    nome_final = nome_grupo
                else:
                    nome_final = nome_grupo + "_" + nome_detalhe
                
                headers_combinados.append(nome_final)
                index_linha2 += 1

        tbody = tabela.find_element(By.TAG_NAME, "tbody")
        linhas = tbody.find_elements(By.TAG_NAME, "tr")

        hora_atual = dt.now().strftime("%H00")

        for linha in linhas:
            celulas = linha.find_elements(By.TAG_NAME, "td")
            hora = celulas[1].text
            if hora_atual == hora:
                valores = [celula.text for celula in celulas]
                print(valores)
                
        dados_sem_metadados = dict(zip(headers_combinados, valores))
        
        inmet_campos_schema = [(header, pa.string()) for header in headers_combinados]
        schema = pa.schema(inmet_campos_schema)
        schema
        
        dados_sem_metadados_colunar = {k: [v] for k,v in dados_sem_metadados.items()}
        table = pa.Table.from_pydict(dados_sem_metadados_colunar, schema=schema)

        metadata = {
            b'layer': b'bronze',
            b'destiny': b'silver',
            b'fonte_nome': b'INMET',
            b'fonte_estacao': b'CASTANHAL',
            b'fonte_codigo': b'A202',
            b'fonte_datetime_insert': dt.now().isoformat().encode()
        }

        table = table.replace_schema_metadata(metadata)
        
        agora = dt.now()
        path_parquet_s3 = (
            f"{metadata[b'fonte_nome'].decode().lower()}/{metadata[b'fonte_estacao'].decode().lower()}/"
            f"year={agora.year}/month={agora.month:02d}/day={agora.day:02d}/"
            f"{metadata[b'fonte_estacao'].decode().lower()}-{agora.isoformat()}.parquet"
        )
        path_parquet_s3
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id = get_parameter("/tcc/dev/airflow_aws_s3_key_id"),
            aws_secret_access_key = get_parameter("/tcc/dev/airflow_s3_secret"),
            region_name = get_parameter("/tcc/dev/aws_region")
        )

        bucket_name = get_parameter("/tcc/dev/aws_s3_bucket_bronze")
        
        buffer = BytesIO()

        pq.write_table(
            table,
            buffer,
            compression="snappy"
        )

        buffer.seek(0)

        s3_client.put_object(
            Body=buffer.getvalue(),
            Bucket=bucket_name,
            Key=path_parquet_s3    
        )
        
        return True
    
    get_last_hour_inmet()
        
app_setup()
inmet_hourly()