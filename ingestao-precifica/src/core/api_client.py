# src/core/api_client.py

import requests
import time
import logging
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PrecificaAPIClient:
    def __init__(self, config):
        self.base_url = config.get('API', 'BASE_URL')
        self.client_key = config.get('API', 'CLIENT_KEY')
        self.secret_key = config.get('API', 'SECRET_KEY')
        self.plataforma = config.get('API', 'PLATAFORMA')
        self.domain = self._normalize_domain(config.get('API', 'DOMINIO'))
        self.headers = {'Accept': 'application/vnd.api+json', 'Content-Type': 'application/vnd.api+json'}
        self._auth_token = None
        self._token_expiration_time = 1
        self.rate_limit_delay = 1.2
        self.auth_lock = Lock()

    def _normalize_domain(self, domain_raw: str) -> str:
        parsed = urlparse(domain_raw)
        domain = parsed.netloc if parsed.netloc else parsed.path
        return domain.rstrip('/')

    def _get_auth_token(self):
        if self._auth_token and time.time() < self._token_expiration_time - 10:
            return
        with self.auth_lock:
            if self._auth_token and time.time() < self._token_expiration_time - 10:
                return
            logging.info("Token expirado ou inexistente. Solicitando um novo token...")
            auth_url = f"{self.base_url}/authentication"
            auth_headers = {'client_key': self.client_key, 'secret_key': self.secret_key, **self.headers}
            try:
                r = requests.get(auth_url, headers=auth_headers, timeout=30)
                r.raise_for_status()
                token = r.json().get('data', {}).get('token')
                if not token: raise ValueError("Token não encontrado na resposta.")
                self._auth_token = token
                self._token_expiration_time = time.time() + 40
                logging.info("Token de autenticação obtido com sucesso.")
            except requests.exceptions.ConnectTimeout as e:
                logging.error(f"Timeout de conexão ao tentar conectar com {auth_url}")
                logging.error("Possíveis causas:")
                logging.error("1. VPC sem Cloud NAT configurado (se usando --vpc-egress all-traffic)")
                logging.error("2. Firewall bloqueando tráfego de saída")
                logging.error("3. Problema de DNS na VPC")
                logging.error(f"Erro completo: {e}")
                raise
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao obter token de {auth_url}: {e}")
                logging.error(f"Tipo de erro: {type(e).__name__}")
                raise

    def _make_request(self, method: str, endpoint: str, params: dict = None, retries: int = 2):
        time.sleep(self.rate_limit_delay)
        self._get_auth_token()
        url = f"{self.base_url}/{endpoint}"
        req_headers = {'Authorization': f'Bearer {self._auth_token}', **self.headers}
        try:
            response = requests.request(method, url, headers=req_headers, params=params, timeout=30)
            if response.status_code == 429:
                delay = int(response.headers.get('X-Ratelimit-Delay-Sec', 2))
                logging.warning(f"Rate limit atingido (429). Aguardando {delay}s.")
                time.sleep(delay)
                return self._make_request(method, endpoint, params)
            if response.status_code == 401 and retries > 0:
                self._auth_token = None
                return self._make_request(method, endpoint, params, retries - 1)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro na requisição para '{endpoint}': {e}")
            return None

    def _fetch_single_page(self, page: int):
        """Busca uma única página de produtos."""
        endpoint = f"platform/{self.plataforma}/{self.domain}/scan/products?page={page}"
        return self._make_request('GET', endpoint)

    def fetch_all_products_concurrently(self, max_workers=8) -> list:
        """
        Busca todos os produtos da API de paginação de forma concorrente e rápida.
        Esta é a única chamada de busca de dados necessária.
        """
        logging.info("Iniciando busca em massa otimizada de produtos...")
        first_page_data = self._fetch_single_page(1)
        if not (first_page_data and isinstance(first_page_data.get('data'), dict)):
            logging.error("Não foi possível obter dados da primeira página ou o formato é inesperado.")
            return []
        
        total = int(first_page_data['data'].get('total', 0))
        limit = int(first_page_data['data'].get('limit', 1))
        if limit == 0: return []
        total_pages = (total + limit - 1) // limit
        logging.info(f"Total de {total_pages} páginas a serem buscadas.")
        
        all_products = first_page_data['data'].get('scan', [])
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            pages = range(2, total_pages + 1)
            futures = {executor.submit(self._fetch_single_page, p): p for p in pages}
            for future in as_completed(futures):
                res = future.result()
                if res and res.get('data', {}).get('scan'):
                    all_products.extend(res['data']['scan'])
        
        logging.info(f"Busca em massa finalizada. {len(all_products)} produtos brutos coletados.")
        return all_products