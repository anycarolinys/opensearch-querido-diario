from typing import Dict, Iterable, List, Union
import os

import opensearchpy

from tasks import IndexInterface
from tenacity import retry, stop_after_attempt, wait_fixed



class OpenSearchInterface(IndexInterface):
    # def __init__(self, hosts: List, user: str, password: str, timeout: int = 30, default_index: str = ""):
    def __init__(self, hosts: List, user: str, password: str, timeout: int = 120, default_index: str = ""):
        # self._search_engine = opensearchpy.OpenSearch(hosts=hosts, http_auth=(user, password))
        # self._search_engine = opensearchpy.OpenSearch(hosts=hosts, http_auth=(user, password),timeout=60)
        # Número de tentativas de reconexão retry_on_timeout=True  
        # Tenta reconectar em caso de timeout
        self._search_engine = opensearchpy.OpenSearch( hosts=hosts, http_auth=(user, password), timeout=60, max_retries=3)  

        # self._timeout = timeout
        self._timeout = 120
        self._default_index = default_index

    def index_exists(self, index_name: str) -> bool:
        """ 
        Verifica se um índice existe no OpenSearch

        Parâmetros:
            index_name: Nome do índice a ser verificado.
        Retorno: 
            True se o índice existir, caso contrário, False. 
        """
        return self._search_engine.indices.exists(index=index_name)

    def is_valid_index_name(self, index_name: str) -> bool:
        """ 
        Verifica se um nome de índice é válido.
        
        Parâmetros:
            index_name: Nome do índice a ser validado.
        Retorno: 
            True se o nome do índice for uma string não vazia, caso contrário, False.
        """
        return isinstance(index_name, str) and len(index_name) > 0

    """ Obtém o nome do índice, verificando se é válido.
        Parâmetros:
            index_name: Nome do índice a ser obtido.
        Retorno: 
            O nome do índice se for válido; caso contrário, retorna o índice padrão.
        Exceção: 
            Lança uma exceção se nenhum índice for definido. """
    def get_index_name(self, index_name: str) -> str:
        if self.is_valid_index_name(index_name):
            return index_name
        if self._default_index == "":
            raise Exception("Index name not defined")
        return self._default_index

    """ Cria um índice no OpenSearch.
        Parâmetros:
            index_name: Nome do índice a ser criado (padrão: "").
            body: Configurações e mapeamentos do índice (padrão: {}).
        Ação: 
            Cria o índice se ele não existir. """
    def create_index(self, index_name: str = "", body: Dict = {}) -> None:
        index_name = self.get_index_name(index_name)
        if self.index_exists(index_name):
            return
        self._search_engine.indices.create(
            index=index_name,
            body=body,
            # timeout=self._timeout,
            timeout=120,
        )

    """ Atualiza um índice no OpenSearch.
        Parâmetros: 
            index_name: Nome do índice a ser atualizado (padrão: "").
        Ação: 
            Atualiza o índice se ele existir. """
    def refresh_index(self, index_name: str = "") -> None:
        index_name = self.get_index_name(index_name)
        if self.index_exists(index_name):
            return
        self._search_engine.indices.refresh(
            index=index_name,
        )

    """ Descrição: Indexa (insere) um documento no OpenSearch.
        Parâmetros:
            document: Dicionário representando o documento a ser indexado.
            document_id: ID opcional do documento.
            index: Nome do índice onde o documento será indexado (padrão: "").
            refresh: Se deve atualizar o índice após a operação (padrão: False).
        Ação: 
            Indexa o documento no índice especificado. """
    """ def index_document(
        self,
        document: Dict,
        document_id: Union[str, None] = None,
        index: str = "",
        refresh: bool = False,
    ) -> None:
        index = self.get_index_name(index)
        # self._search_engine.index(index=index, body=document, id=document_id, refresh=refresh)
        self._search_engine.index(index=index, body=document, id=document_id, refresh=refresh,  timeout=60) """

    
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(10))
    def index_document(
        self, 
        document: Dict, 
        document_id: str, 
        index: str = "", 
        refresh: bool = False
    ) -> None:
        index = self.get_index_name(index)
        if not self._search_engine.exists(index=index, id=document_id):
            self._search_engine.index(index=index, body=document, id=document_id, refresh=refresh, timeout=60)


    """ Realiza uma busca no OpenSearch.
        Parâmetros:
            query: Dicionário representando a consulta.
            index: Nome do índice a ser pesquisado (padrão: "").
        Retorno: 
            Resultado da busca como um dicionário. """
    def search(self, query: Dict, index: str = "") -> Dict:
        index = self.get_index_name(index)
        # result = self._search_engine.search(index=index, body=query, request_timeout=60)
        result = self._search_engine.search(index=index, body=query, request_timeout=120)
        return result

    """ Analisa um texto usando o analisador do índice especificado.
        Parâmetros:
            text: Texto a ser analisado.
            field: Campo do índice a ser utilizado na análise.
            index: Nome do índice a ser utilizado (padrão: "").
        Retorno: 
            Resultado da análise como um dicionário. """
    def analyze(self, text: str, field: str, index: str = "") -> Dict:
        index = self.get_index_name(index)
        result = self._search_engine.indices.analyze(body={"text": text, "field":field}, index=index)
        return result


    """ Realiza uma busca paginada no OpenSearch.
        Parâmetros:
            query: Dicionário representando a consulta.
            index: Nome do índice a ser pesquisado (padrão: "").
            keep_alive: Tempo para manter a busca aberta (padrão: "5m").
        Retorno: 
            Um iterador que retorna os resultados da busca em páginas.
        Ação: 
            Realiza a busca inicial e continua retornando páginas de resultados até que não haja mais resultados. """
    def paginated_search(
        self, query: Dict, index: str = "", keep_alive: str = "5m"
    ) -> Iterable[Dict]:
        index = self.get_index_name(index)
        result = self._search_engine.search(
            index=index, body=query, scroll=keep_alive, request_timeout=120
        )

        if len(result["hits"]["hits"]) == 0:
            return

        scroll_id = None
        while len(result["hits"]["hits"]) > 0:
            yield result

            if scroll_id is not None and scroll_id != result["_scroll_id"]:
                self._search_engine.clear_scroll(scroll_id=scroll_id)

            scroll_id = result["_scroll_id"]
            result = self._search_engine.scroll(
                scroll_id=scroll_id, scroll=keep_alive, request_timeout=120
            )

        self._search_engine.clear_scroll(scroll_id=scroll_id)

""" Obtém o host do OpenSearch das variáveis de ambiente.
    Retorno: 
        Host do OpenSearch. """
def get_opensearch_host():
    return os.environ["OPENSEARCH_HOST"]

""" Obtém do OpenSearch das variáveis de ambiente.
    Retorno: 
        Host do OpenSearch. """
def get_opensearch_index():
    return os.environ["OPENSEARCH_INDEX"]

def get_opensearch_user():
    return os.environ["OPENSEARCH_USER"]

def get_opensearch_password():
    return os.environ["OPENSEARCH_PASSWORD"]

def create_index_interface() -> IndexInterface:
    hosts = get_opensearch_host()
    if not isinstance(hosts, str) or len(hosts) == 0:
        raise Exception("Missing index hosts")
    default_index_name = get_opensearch_index()
    if not isinstance(default_index_name, str) or len(default_index_name) == 0:
        raise Exception("Invalid index name")
    return OpenSearchInterface([hosts], get_opensearch_user(), get_opensearch_password(), default_index=default_index_name)
