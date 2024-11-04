import csv
from dotenv import load_dotenv
import time
import json
import os
import subprocess
from tqdm import tqdm

def is_segmented():
    return os.environ["SEGMENT"] == '1'

def get_segment_size():
    print('Segment size:', os.environ["SEGMENT_SIZE"])
    return os.environ["SEGMENT_SIZE"]

def get_highlight():
    print('Highlight:', os.environ["HIGHLIGHT"])
    return os.environ["HIGHLIGHT"]


def get_execution_mode():
    return os.environ["EXECUTION_MODE"]

""" def stop_opensearch():
    container_name = os.environ["OPENSEARCH_CONTAINER_NAME"]
    command = ["podman", "rm", "--force", "--ignore", container_name]
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)

        print("Stop OpenSearch command output\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Stop OpenSearch command error\n", e.stderr) """

def stop_opensearch():
    container_name = os.environ["OPENSEARCH_CONTAINER_NAME"]
    
    # Primeiro, tente parar o container com um timeout
    stop_command = ["podman", "stop", "--timeout", "30", container_name]
    remove_command = ["podman", "rm", "--force", "--ignore", container_name]

    try:
        # Tenta parar o container
        stop_result = subprocess.run(stop_command, check=True, text=True, capture_output=True)
        print("Stop OpenSearch command output\n", stop_result.stdout)

        # Remove o container após parar com sucesso
        remove_result = subprocess.run(remove_command, check=True, text=True, capture_output=True)
        print("Remove OpenSearch container output\n", remove_result.stdout)

    except subprocess.CalledProcessError as e:
        print("Error stopping or removing OpenSearch container\n", e.stderr)

        # Verifica se o container ainda está rodando após tentativa de stop
        ps_command = ["podman", "ps", "--filter", f"name={container_name}"]
        ps_result = subprocess.run(ps_command, check=True, text=True, capture_output=True)
        if container_name in ps_result.stdout:
            print(f"Container {container_name} is still running. Forcing removal...")
            
            # Força a remoção do container
            try:
                force_result = subprocess.run(remove_command, check=True, text=True, capture_output=True)
                print("Force remove OpenSearch container output\n", force_result.stdout)
            except subprocess.CalledProcessError as force_e:
                print("Force remove command error\n", force_e.stderr)
        else:
            print(f"Container {container_name} is not running anymore.")

def restart_opensearch():

    container_name = os.environ["OPENSEARCH_CONTAINER_NAME"]
    pod_name = os.environ["POD_NAME"]
    
    # -e OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g 
    command = [
    "podman", "run", "-d", "--rm", "-ti",
    "--name", container_name,
    "--pod", pod_name,
    "--env", "OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g ",
    "--env", "discovery.type=single-node",
    "--env", "plugins.security.ssl.http.enabled=false",
    "docker.io/opensearchproject/opensearch:2.9.0"
    ]
    
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)

        print("Restart OpenSearch command output\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Restart OpenSearch command error\n", e.stderr)

def text_pipeline(segment, segment_size):
    
    current_directory = os.getcwd()
    pod_name = os.environ["POD_NAME"]
    img_namespace = os.environ["IMAGE_NAMESPACE"]
    img_name = os.environ["IMAGE_NAME"]
    img_tag = os.environ["IMAGE_TAG"]

    copy_command = ["cp", "contrib/sample.env", "envvars"]

    if segment:
        print('Processando documentos segmentados')
        podman_command = [
        "podman", "run", "--rm", "-ti",
        "--volume", f"{current_directory}:/mnt/code:rw",
        "--pod", pod_name,
        "-e", f"SEGMENT=1",  # Adiciona SEGMENT como variável de ambiente
        "-e", f"SEGMENT_SIZE={segment_size}",  # Adiciona SEGMENT_SIZE como variável de ambiente
        "--env", "PYTHONPATH=/mnt/code",
        "--env-file", "envvars",
        f"{img_namespace}/{img_name}:{img_tag}",
        "python", "main"
        ]
    else:
        print('Processando documentos nao segmentados')
        podman_command = [
        "podman", "run", "--rm", "-ti",
        "--volume", f"{current_directory}:/mnt/code:rw",
        "--pod", pod_name,
        "-e", f"SEGMENT=0",  # Adiciona SEGMENT como variável de ambiente
        "--env", "PYTHONPATH=/mnt/code",
        "--env-file", "envvars",
        f"{img_namespace}/{img_name}:{img_tag}",
        "python", "main"
        ]
    
    try:
        subprocess.run(copy_command, check=True)
        result = subprocess.run(podman_command, check=True, text=True, capture_output=True)
        print('Text pipeline output\n', result.stdout)
    except subprocess.CalledProcessError as e:
        print("Text pipeline command error\n", e.stderr)


def get_opensearch_data():
    # container_name = os.environ["OPENSEARCH_CONTAINER_NAME"]
    command = [
        "curl", "-k", "-u", "admin:admin",
        "-X", "GET", f"http://localhost:9200/_cat/indices/querido-diario?v&h=docs.count,store.size",
    ]
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        # print("Index size output\n", result.stdout)
        # Divide a saída em linhas
        lines = result.stdout.splitlines()

        # A segunda linha contém os dados (a primeira linha contém os cabeçalhos)
        if len(lines) > 1:
            data = lines[1].split()

            # Pega os índices das colunas de interesse
            docs_count = data[0]  # docs.count
            store_size = data[1]  # store.size

            # print(f{"docs.count":  docs_count, "store.size": store_size})
            # return {"docs.count":  docs_count, "store.size": store_size}
            return docs_count, store_size
        return None
    except subprocess.CalledProcessError as e:
        print("Error getting opensearch data\n", e.stderr)
        return None

""" def term_search(term):

    command = [
        "curl", "-k", "-u", "admin:admin",
        "-X", "GET", "http://localhost:9200/querido-diario/_search?pretty",
        "-H", "Content-Type: application/json",
        "-d", f'{{"query": {{"match": {{"source_text": "{term}"}}}}}}'
    ]
    
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        # print("General search output\n", result.stdout)
        # return result.stdout

        # Parsing the result.stdout as JSON
        result_json = json.loads(result.stdout)
        
        # Getting the value of "took"
        took_value = result_json.get("took", None)
        
        # print("Search output\n", result.stdout)
        print(f"Value of 'took': {took_value}")
        return took_value
    except subprocess.CalledProcessError as e:
        print("Search error\n", e.stderr)
        return None """

def term_search(term):
    command = [
        "curl", "-k", "-u", "admin:admin",
        "-X", "GET", "http://localhost:9200/querido-diario/_search?pretty",
        "-H", "Content-Type: application/json",
        "-d", f'{{"query": {{"match": {{"source_text": "{term}"}}}}}}'
    ]
    
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        
        # Parsing the result.stdout as JSON
        result_json = json.loads(result.stdout)
        
        # Getting the value of "took"
        took_value = result_json.get("took", None)
        
        # Getting the value of "hits.total.value"
        hits_total_value = result_json.get("hits", {}).get("total", {}).get("value", None)
        
        print(f"Value of 'took': {took_value}")
        print(f"Total hits value: {hits_total_value}")
        
        return took_value, hits_total_value
    except subprocess.CalledProcessError as e:
        print("Search error\n", e.stderr)
        return None, None

def term_search_with_highlight(term, fragment_size):

    """ command = [
        "curl", "-k", "-u", "admin:admin",
        "-X", "GET", "http://localhost:9200/querido-diario/_search?pretty",
        "-H", "Content-Type: application/json",
        "-d", f'{{"query": {{"match": {{"source_text": "{term}"}}}}}}'
    ] """
    
    command = [
    "curl", "-k", "-u", "admin:admin",
    "-X", "GET", "http://localhost:9200/querido-diario/_search?pretty",
    "-H", "Content-Type: application/json",
    "-d", f'''{{
        "query": {{
            "match": {{
                "source_text": "{term}"
            }}
        }},
        "highlight": {{
            "fields": {{
                "source_text": {{
                    "fragment_size": {fragment_size},
                    "number_of_fragments": 1
                }}
            }}
        }}
    }}'''
    ]
    
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        # print("General search output\n", result.stdout)
        # return result.stdout

        # Parsing the result.stdout as JSON
        result_json = json.loads(result.stdout)
        
        # Getting the value of "took"
        took_value = result_json.get("took", None)
        
        # print("Search output\n", result.stdout)
        print(f"Value of 'took' with highlight: {took_value}")
        return took_value
    except subprocess.CalledProcessError as e:
        print("Search error\n", e.stderr)
        return None

def save_to_csv(data, output_file):
    with open(output_file, mode='a', newline='\n') as file:
        writer = csv.writer(file)
        writer.writerow([data['is_segmented'],data['segment_size'], \
                        data['index_size'], data['document_count'], \
                        data['general_search_term'], data['general_search_time'],
                        data['general_search_docs_count'],
                        data['specific_search_term'], data['specific_search_time'], \
                        data['specific_search_docs_count'],
                        data['with_highlight'], data['highlight_term'], \
                        data['fragment_size'], data['highlight_time']])

def wait_with_progress(duration):
    # Número total de passos na barra de progresso
    steps = 60
    interval = duration / steps
    
    print("Aguardando", duration, "segundos...")
    
    # Cria uma barra de progresso usando tqdm
    for _ in tqdm(range(steps), desc="Progresso", unit="s"):
        time.sleep(interval)
    
    print("\nTempo concluído!")

# stop_opensearch()
# restart_opensearch()
# wait_with_progress(30)

if __name__ == '__main__':
    load_dotenv('envvars')
    # stop_opensearch()
    # restart_opensearch()
    # wait_with_progress(60)

    full_document_dict = {}
    # """ full_document_data """
    # csv_name = './municipios/rs_horizontina/full_document_data.csv'
    # full_document_dict['is_segmented'] = False
    # full_document_dict['segment_size'] = None
    # fragment_sizes = [500, 1000, 2000]

    """ 500_document_data """
    # full_document_dict['is_segmented'] = True
    # full_document_dict['segment_size'] = 500
    # csv_name = './municipios/rs_horizontina/500_document_data.csv'
    # fragment_sizes = [500]
    
    """ 1000_document_data """
    # full_document_dict['is_segmented'] = True
    # full_document_dict['segment_size'] = 1000
    # csv_name = './municipios/rs_horizontina/1000_document_data.csv'
    # fragment_sizes = [1000]
    
    """ 2000_document_data """
    full_document_dict['is_segmented'] = True
    full_document_dict['segment_size'] = 2000
    csv_name = './municipios/teste/2000_document_data.csv'
    fragment_sizes = [2000]
    
    docs_count, store_size = get_opensearch_data()
    print(f'Docs count {docs_count} store size {store_size}')
    full_document_dict['index_size'] = store_size
    full_document_dict['document_count'] = docs_count

    
    general_term = ['prefeitura', 'diário', 'lei', 'termo', 'decreto']
    specific_term = [ 'Lei de Acesso à Informação', 'LAI',
                     'Lei Geral de Proteção de Dados Pessoais', 'LGPD', 
                     'Política Nacional de Resíduos Sólidos', 'PNRS']
    
    full_document_dict['specific_search_term'] = None
    full_document_dict['specific_search_time'] = None
    full_document_dict['specific_search_docs_count'] = None
    full_document_dict['with_highlight'] = None
    full_document_dict['highlight_term'] = None
    full_document_dict['fragment_size'] = None
    full_document_dict['highlight_time'] = None
    for general in general_term:
        full_document_dict['general_search_term'] = general
        for i in range(100):
            general_search_time, general_search_docs_count = term_search(general)
            full_document_dict['general_search_time'] = general_search_time
            full_document_dict['general_search_docs_count'] = general_search_docs_count
            save_to_csv(full_document_dict, csv_name)

    
    highlight_terms = [general_term[0], specific_term[0]]
    full_document_dict['general_search_term'] = None
    full_document_dict['general_search_time'] = None
    full_document_dict['general_search_docs_count'] = None
    full_document_dict['specific_search_term'] = None
    full_document_dict['specific_search_time'] = None
    for highlight in highlight_terms:
        full_document_dict['highlight_term'] = highlight
        full_document_dict['with_highlight'] = True
        for fragment_size in fragment_sizes:
            for i in range(100):
                full_document_dict['fragment_size'] = fragment_size
                highlight_time = term_search_with_highlight(highlight, fragment_size)
                full_document_dict['highlight_time'] = highlight_time
                save_to_csv(full_document_dict, csv_name)
            
    full_document_dict['general_search_term'] = None
    full_document_dict['general_search_time'] = None
    full_document_dict['with_highlight'] = None
    full_document_dict['highlight_term'] = None
    full_document_dict['fragment_size'] = None
    full_document_dict['highlight_time'] = None
    for specific in specific_term:
        full_document_dict['specific_search_term'] = specific
        for i in range(100):
            specific_search_time, specific_search_docs_count = term_search(specific)
            full_document_dict['specific_search_time'] = specific_search_time        
            full_document_dict['specific_search_docs_count'] = specific_search_docs_count        
            save_to_csv(full_document_dict, csv_name)
