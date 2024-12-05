import requests
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime, timezone
import logging

# Configurações Gerais
API_URL = "http://your-url/example"
CSV_FILENAME = "data.csv"
THREAD_COUNT = 80  # Número de threads para envio paralelo

# Configuração do Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_uuid():
    return str(uuid.uuid4())

def get_formatted_timestamp():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def read_documents_from_csv(filename):
    rows = []
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                rows.append(row)
        logger.info(f"{len(rows)} registros lidos do arquivo {filename}")
    except FileNotFoundError:
        logger.error(f"Arquivo {filename} não encontrado.")
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo CSV: {e}")
    return rows

def generate_payload(row):
    try:
        document = row['document'].strip().zfill(11)
        payload = {
        "event": "topic_name",
        "document": document,
        "decisionId": 12345,
        "infoId": "info_id_value",
        "subInfoId": "sub_info_id_value",
        "xValue": 1.23,
        "yValue": 4.56,
        "zValue": 7.89,
        "floatNumber": 10.11,
        "string": "string_value",
        "correlationId": generate_uuid(),
        "timestamp": get_formatted_timestamp()
        }
        return payload
    except KeyError as e:
        logger.error(f"Chave ausente no CSV: {e}")
    except ValueError as e:
        logger.error(f"Erro de conversão de tipo: {e}")
    except Exception as e:
        logger.error(f"Erro ao gerar payload: {e}")
    return None

def send_request(session, payload):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    try:
        response = session.post(API_URL, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            logger.info(f"Mensagem enviada com sucesso para documento: {payload['body']['document']}")
            return True
        else:
            logger.error(f"Falha ao enviar mensagem para documento: {payload['body']['document']}. Status Code: {response.status_code}, Resposta: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de requisição para documento: {payload['body']['document']}. Erro: {e}")
        return False

def send_messages_in_parallel(rows, thread_count):
    start_time = time.time()
    success_count = 0
    processed_documents = set()  # Conjunto para armazenar documentos já processados

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        with requests.Session() as session:
            tasks = []
            for row in rows:
                document = row['document'].strip().zfill(11)
                if document not in processed_documents:
                    payload = generate_payload(row)
                    if payload:
                        tasks.append(executor.submit(send_request, session, payload))
                        processed_documents.add(document)
                else:
                    logger.info(f"Documento duplicado encontrado, ignorando: {document}")
            for future in as_completed(tasks):
                if future.result():
                    success_count += 1

    elapsed_time = time.time() - start_time
    logger.info(f"{success_count}/{len(processed_documents)} mensagens enviadas com sucesso em {elapsed_time:.2f} segundos")

if __name__ == '__main__':
    rows = read_documents_from_csv(CSV_FILENAME)
    if rows:
        send_messages_in_parallel(rows, THREAD_COUNT)
    else:
        logger.error("Nenhuma mensagem foi enviada devido a erros na leitura do CSV.")