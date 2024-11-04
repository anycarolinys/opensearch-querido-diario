import logging
import tempfile
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union
from segmentation import get_segmenter

from .interfaces import (
    DatabaseInterface,
    IndexInterface,
    StorageInterface,
    TextExtractorInterface,
)

""" PROCESSED_FILES_LOG = "processed_gazettes.log"

def log_processed_file(file_checksum: str):
    
    # Registra o arquivo processado no log.
   
    with open(PROCESSED_FILES_LOG, "a") as log_file:
        log_file.write(f"{file_checksum}\n")


def load_processed_files() -> set:
    
    # Carrega a lista de arquivos já processados a partir do arquivo de log.
   
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, "r") as log_file:
        return set(line.strip() for line in log_file) """

# PROCESSED_FILES_LOG = os.path.join(os.getcwd(), "processed_files.log")
# PROCESSED_FILES_LOG = "./home/anycarolinys/tcc/querido-diario-data-processing/processed_files.log"
# Obter o caminho da pasta onde o script atual está localizado
script_dir = os.path.dirname(os.path.realpath(__file__))

# Criar o caminho do arquivo "processed_files.log" dentro dessa pasta
PROCESSED_FILES_LOG = os.path.join(script_dir, "processed_files.log")

def log_processed_file(file_checksum: str):
    """
    Registra o arquivo processado no log.
    """
    # print(PROCESSED_FILES_LOG)
    with open(PROCESSED_FILES_LOG, "a") as log_file:
        log_file.write(f"{file_checksum}\n")


def load_processed_files() -> set:
    """
    Carrega a lista de arquivos já processados a partir do arquivo de log.
    """
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, "r") as log_file:
        return set(line.strip() for line in log_file)
    
def extract_text_from_gazettes_in_batches(
    gazettes: Iterable[Dict[str, Any]],
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
    batch_size: int
) -> List[str]:
    """
    Processa os arquivos de gazetas em batches, parando após processar o número de arquivos especificado (batch_size).
    """
    logging.info("Iniciando extração de texto em batches")
    processed_files = load_processed_files()  # Carrega arquivos já processados
    ids = []
    batch = []

    for gazette in gazettes:
        file_checksum = gazette["file_checksum"]

        # Verifica se o arquivo já foi processado
        if file_checksum in processed_files:
            logging.info(f"Gazette já processada: {file_checksum}, pulando.")
            continue

        # Adiciona gazette ao batch
        batch.append(gazette)

        # Quando atingir o tamanho do batch, processa e encerra
        if len(batch) >= batch_size:
            logging.info(f"Batch de tamanho {batch_size} atingido. Processando batch.")
            ids.extend(process_batch(batch, territories, database, storage, index, text_extractor))
            return ids  # Encerra o processamento ao atingir o batch size

    # Caso o batch não tenha atingido o tamanho e não haja mais gazettes, processa o que tiver
    if batch:
        logging.info(f"Processando batch incompleto de tamanho {len(batch)}")
        ids.extend(process_batch(batch, territories, database, storage, index, text_extractor))

    return ids  # Retorna os ids processados


def process_batch(
    batch: List[Dict[str, Any]],
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface
) -> List[str]:
    """
    Processa um batch de gazetas.
    """
    ids = []
    for gazette in batch:
        try:
            document_ids = new_try_process_gazette_filev2(
                gazette, territories, database, storage, index, text_extractor
            )
        except Exception as e:
            logging.warning(
                f"Não foi possível processar a gazeta: {gazette['file_path']}. Causa: {e}"
            )
            logging.exception(e)
        else:
            ids.extend(document_ids)
            log_processed_file(gazette["file_checksum"])  # Log após processar com sucesso

    return ids



def extract_text_from_gazettes(
    gazettes: Iterable[Dict[str, Any]],
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> List[str]:
    
    # Extracts the text from a list of gazettes
   
    logging.info("Starting text extraction from gazettes")

    ids = []
    for gazette in gazettes:
        try:
            document_ids = new_try_process_gazette_filev2(
                gazette, territories, database, storage, index, text_extractor
            )
        except Exception as e:
            logging.warning(
                f"Could not process gazette: {gazette['file_path']}. Cause: {e}"
            )
            logging.exception(e)
        else:
            ids.extend(document_ids)

    return ids


def new_try_process_gazette_filev2(
    gazette: Dict,
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> List[str]:
    
    # Do all the work to extract the content from the gazette files
   
    logging.debug(f"Processing gazette {gazette['file_path']}")
    gazette_file = download_gazette_file(gazette, storage)
    
    # Extrair o texto da gazeta
    full_text = try_to_extract_content(gazette_file, text_extractor)
    gazette["url"] = define_file_url(gazette["file_path"])
    gazette_txt_path = define_gazette_txt_path(gazette)
    gazette["file_raw_txt"] = define_file_url(gazette_txt_path)
    
    # Upload do texto completo
    upload_raw_text(gazette_txt_path, full_text, storage)
    delete_gazette_files(gazette_file)

    gazette_filename = Path(gazette["file_path"]).stem
    segment_size = int(os.environ['SEGMENT_SIZE'])
    document_ids = []
    if gazette_type_is_aggregated(gazette):
        # Processar documentos agregados
        segmenter = get_segmenter(gazette["territory_id"], territories)
        territory_segments = segmenter.get_gazette_segments(gazette)
        
        if get_segment_option():
            print(f'Texto segmentado em {segment_size} caracteres')
            for segment in territory_segments:
                # Segmenta o texto unificado em partes menores de 2000 caracteres
                segments = segment_text(segment["source_text"], segment_size=segment_size)
                
                for i, segment_part in enumerate(segments):
                    segment_data = segment.copy()
                    segment_data["source_text"] = segment_part  # Adiciona o trecho de texto segmentado
                    segment_data["segment_index"] = i  # Índice do segmento
                    segment_data["file_checksum"] = f"{segment['file_checksum']}_{i}"  # Checksum único para cada segmento
                    segment_data["file_path"] = f"{gazette_filename}_{i}" 
                    
                    segment_txt_path = define_segment_txt_path(segment_data)
                    upload_raw_text(segment_txt_path, segment_part, storage)
                    
                    index.index_document(segment_data, document_id=segment_data["file_checksum"])
                    document_ids.append(segment_data["file_checksum"])
        else:
            print(f'Texto completo')
            for segment in territory_segments:
                segment_txt_path = define_segment_txt_path(segment)
                segment["file_raw_txt"] = define_file_url(segment_txt_path)
                upload_raw_text(segment_txt_path, segment["source_text"], storage)
                index.index_document(segment, document_id=segment["file_checksum"])
                document_ids.append(segment["file_checksum"])     
    else:
        if get_segment_option():
            print(f'Texto segmentado em {segment_size} caracteres')
            # Processar documentos não agregados com segmentação de texto
            segments = segment_text(full_text,  segment_size=segment_size)
            document_ids = upload_segmented_text(segments, gazette, index, storage, document_ids)
        else:
            print(f'Texto completo')
            gazette["source_text"] = full_text
            index.index_document(gazette, document_id=gazette["file_checksum"])
            document_ids.append(gazette["file_checksum"])

    set_gazette_as_processed(gazette, database)
    return document_ids

def upload_segmented_text(
    segments: List[str],
    gazette: Dict,
    index: IndexInterface,
    storage: StorageInterface,
    document_ids: List[str]) -> List[str]:

    gazette_filename = Path(gazette["file_path"]).stem

    for i, segment in enumerate(segments):
            segment_data = gazette.copy()
            segment_data["source_text"] = segment  # Adiciona o trecho de texto
            segment_data["segment_index"] = i  # Índice do segmento
            segment_data["file_checksum"] = f"{gazette['file_checksum']}_{i}"  # Checksum único para cada segmento
            segment_data["file_path"] = f"{gazette_filename}_{i}"
            
            segment_txt_path = define_segment_txt_path(segment_data)
            upload_raw_text(segment_txt_path, segment, storage)
            
            index.index_document(segment_data, document_id=segment_data["file_checksum"])
            document_ids.append(segment_data["file_checksum"])
    return document_ids

def get_segment_option():
    print('get_segment_option', (os.environ["SEGMENT"] == '1'))
    return os.environ["SEGMENT"] == '1'

def segment_text(text: str, segment_size: int) -> List[str]:
    """
    Segmenta o texto em partes menores baseadas em tamanho de caractere ou número de palavras.
    """
    return [text[i:i + segment_size] for i in range(0, len(text), segment_size)]

def gazette_type_is_aggregated(gazette: Dict):
    """
    Checks if gazette contains publications by more than one city.

    Currently, this is being done by verifying if the territory_id finishes in "00000".
    This is a special code we are using for gazettes from associations of cities from a
    state.

    E.g. If cities from Alagoas have their territory_id's starting with "27", an
    association file will be given territory_id "270000" and will be detected.
    """
    return str(gazette["territory_id"][-5:]).strip() == "00000"


def upload_raw_text(path: Union[str, Path], content: str, storage: StorageInterface):
    """
    Upload gazette raw text file
    """
    storage.upload_content(path, content)
    logging.debug(f"Raw text uploaded {path}")


def define_gazette_txt_path(gazette: Dict):
    """
    Defines the gazette txt path in the storage
    """
    return str(Path(gazette["file_path"]).with_suffix(".txt").as_posix())


def define_segment_txt_path(segment: Dict):
    """
    Defines the segment txt path in the storage
    """
    # return f"{segment['territory_id']}/{segment['date']}/{segment['file_checksum']}.txt"
    return f"{segment['territory_id']}/{segment['date']}/{segment['file_path']}.txt"


def define_file_url(path: str):
    """
    Joins the storage endpoint with the path to form the URL
    """
    file_endpoint = get_file_endpoint()
    return f"{file_endpoint}/{path}"


def get_file_endpoint() -> str:
    """
    Get the endpoint where the gazette files can be downloaded.
    """
    return os.environ["QUERIDO_DIARIO_FILES_ENDPOINT"]


def try_to_extract_content(
    gazette_file: str, text_extractor: TextExtractorInterface
) -> str:
    """
    Calls the function to extract the content from the gazette file. If it fails
    remove the gazette file and raise an exception
    """
    try:
        return text_extractor.extract_text(gazette_file)
    except Exception as e:
        os.remove(gazette_file)
        raise e


def delete_gazette_files(gazette_file: str) -> None:
    """
    Removes the files used to process the gazette content.
    """
    os.remove(gazette_file)


def download_gazette_file(gazette: Dict, storage: StorageInterface) -> str:
    """
    Download the file from the object storage and write it down in the local
    disk to allow the text extraction
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        gazette_file_key = get_gazette_file_key_used_in_storage(gazette)
        storage.get_file(gazette_file_key, tmpfile)
        return tmpfile.name


def get_gazette_file_key_used_in_storage(gazette: Dict) -> str:
    """
    Get the file key used to store the gazette in the object storage
    """
    return gazette["file_path"]


def set_gazette_as_processed(gazette: Dict, database: DatabaseInterface) -> None:
    command = """
        UPDATE gazettes
        SET processed = True
        WHERE id = %(id)s
        AND file_checksum = %(file_checksum)s
    ;
    """
    id = gazette["id"]
    checksum = gazette["file_checksum"]
    data = {"id": id, "file_checksum": checksum}
    logging.debug(f"Marking {id}({checksum}) as processed")
    database.update(command, data)
