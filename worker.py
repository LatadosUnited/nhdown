# worker.py
import redis
import json
import time
import threading
import socket
import os
import requests
from bs4 import BeautifulSoup
import io
import zipfile
import base64
import re
from tqdm import tqdm

# --- Configurações ---
REDIS_HOST = '147.185.221.22'
REDIS_PORT = 40943
TASK_QUEUE_KEY = 'gallery_tasks:queue'
RESULTS_QUEUE_KEY = 'gallery_tasks:results'
PROCESSING_HASH_KEY = 'gallery_tasks:processing'
SHUTDOWN_CHANNEL = 'gallery_tasks:shutdown'
WORKER_HEARTBEAT_KEY_PREFIX = 'worker_heartbeat:'
SECONDS_BETWEEN_TASKS = 2
HEARTBEAT_INTERVAL = 15
HEARTBEAT_EXPIRATION = 30

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
shutdown_event = threading.Event()

def send_heartbeat(worker_id):
    """Envia um heartbeat para o Redis em intervalos regulares."""
    heartbeat_key = f"{WORKER_HEARTBEAT_KEY_PREFIX}{worker_id}"
    while not shutdown_event.is_set():
        try:
            r.set(heartbeat_key, "online", ex=HEARTBEAT_EXPIRATION)
        except redis.exceptions.ConnectionError:
            break
        time.sleep(HEARTBEAT_INTERVAL)

def listen_for_shutdown():
    """Thread que escuta o canal Pub/Sub por um sinal de parada."""
    pubsub = r.pubsub()
    pubsub.subscribe(SHUTDOWN_CHANNEL)
    for message in pubsub.listen():
        if message['type'] == 'message' and message['data'] == 'stop':
            shutdown_event.set()
            break

def sanitize_filename(name):
    """Remove caracteres inválidos de um texto para que possa ser usado como nome de arquivo."""
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = name.strip()
    return name[:150]

def download_and_package_gallery(gallery_id_str):
    """Baixa as imagens de uma galeria com verificações de HTML para maior robustez."""
    gallery_id = int(gallery_id_str)
    task_id = gallery_id_str
    
    tqdm.write(f"[Worker] Iniciando tarefa: {gallery_id}") 
    try:
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        response = session.get(f'https://nhentai.net/g/{gallery_id}/', timeout=10)
        response.raise_for_status() # Lança um erro para códigos como 404
        soup = BeautifulSoup(response.text, 'html.parser')
        
        title_tag = soup.find('h1', class_='title')
        sanitized_title = sanitize_filename(title_tag.get_text(strip=True)) if title_tag else "titulo_nao_encontrado"
        final_zip_name = f"[{gallery_id}] {sanitized_title}.zip"
        
        # ALTERADO: Verificação robusta do contêiner de miniaturas
        thumbs_container = soup.find('div', class_='thumbs')
        if not thumbs_container:
            raise ValueError(f"Contêiner de miniaturas não encontrado para a galeria {gallery_id}. Pode ter sido deletada.")
        
        links_paginas = thumbs_container.find_all('a', class_='gallerythumb')
        downloaded_files = []
        
        for link in tqdm(links_paginas, desc=f"Baixando galeria {gallery_id}", unit="pág", ncols=100, leave=False):
            pagina_response = session.get(f"https://nhentai.net{link['href']}", timeout=10)
            pagina_soup = BeautifulSoup(pagina_response.text, 'html.parser')
            
            # ALTERADO: Verificação robusta do contêiner de imagem
            image_container = pagina_soup.find('section', id='image-container')
            if not image_container:
                tqdm.write(f"  -> Aviso: Contêiner de imagem não encontrado na página, pulando.")
                continue # Pula para a próxima página

            img_tag = image_container.find('img')
            img_url = img_tag.get('src') if img_tag else None
            
            if img_url:
                img_response = session.get(img_url, timeout=15)
                if img_response.status_code == 200:
                    extensao = img_url.split('.')[-1]
                    nome_arquivo = f"{len(downloaded_files) + 1:03d}.{extensao}"
                    downloaded_files.append({'name': nome_arquivo, 'content': img_response.content})

                    if len(downloaded_files) % 10 == 0:
                        r.hset(PROCESSING_HASH_KEY, task_id, time.time())
        
        if not downloaded_files:
            raise ValueError('Nenhuma imagem foi baixada com sucesso.')
            
        tqdm.write(f"[Worker] Empacotando {len(downloaded_files)} arquivos para a tarefa {gallery_id}...")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_info in downloaded_files:
                zf.writestr(file_info['name'], file_info['content'])
        
        zip_content_b64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')
        return {'id': gallery_id, 'status': 'success', 'file_name': final_zip_name, 'content_b64': zip_content_b64}
    
    except Exception as e:
        tqdm.write(f"❌ [Worker] Erro ao processar {gallery_id}: {e}")
        return {'id': gallery_id, 'status': 'error', 'message': str(e)}

def main():
    """Função principal do worker."""
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    print(f"--- Worker '{worker_id}' iniciado ---")
    
    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(worker_id,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    shutdown_listener_thread = threading.Thread(target=listen_for_shutdown)
    shutdown_listener_thread.daemon = True
    shutdown_listener_thread.start()

    try:
        while not shutdown_event.is_set():
            print(f"[Worker] Aguardando nova tarefa...")
            result = r.brpop(TASK_QUEUE_KEY, timeout=5)
            
            if result:
                task_id = result[1]
                
                r.hset(PROCESSING_HASH_KEY, task_id, time.time())
                result_data = download_and_package_gallery(task_id)
                r.rpush(RESULTS_QUEUE_KEY, json.dumps(result_data))
                r.hdel(PROCESSING_HASH_KEY, task_id)

                if result_data['status'] == 'success':
                    tqdm.write(f"✅ [Worker] Tarefa {task_id} concluída com sucesso. Aguardando {SECONDS_BETWEEN_TASKS}s...")
                else:
                    tqdm.write(f"❌ [Worker] Tarefa {task_id} falhou. Aguardando {SECONDS_BETWEEN_TASKS}s...")
                
                time.sleep(SECONDS_BETWEEN_TASKS)

    except redis.exceptions.ConnectionError as e:
        print(f"❌ [Worker] Não foi possível conectar ao Redis: {e}. Encerrando.")
    finally:
        shutdown_event.set()
        print(f"--- Worker '{worker_id}' encerrado ---")

if __name__ == '__main__':
    main()
