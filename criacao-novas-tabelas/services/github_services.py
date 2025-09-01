# Arquivo: github_services.py
"""
Módulo para encapsular todas as interações com a API do GitHub.
Fornece uma classe cliente para gerenciar o estado (token, repo) e as ações.
"""
import base64
import requests
from typing import Optional

class GitHubAPI:
    """Um cliente para interagir com a API REST do GitHub v3."""

    def __init__(self, token: str, user: str, repo: str):
        """
        Inicializa o cliente da API.

        Args:
            token: Personal Access Token (PAT) para autenticação.
            user: O nome do usuário ou organização do GitHub.
            repo: O nome do repositório.
        """
        self.token = token
        self.base_api_url = f"https://api.github.com/repos/{user}/{repo}"
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def _make_request(self, method: str, endpoint: str, json_data: Optional[dict] = None) -> dict:
        """
        Método privado para fazer requisições à API.

        Args:
            method: Método HTTP (GET, POST, PUT, DELETE).
            endpoint: O endpoint da API (ex: '/git/refs/heads/main').
            json_data: O payload JSON para a requisição.

        Returns:
            A resposta JSON da API.
        """
        url = self.base_api_url + endpoint
        response = requests.request(method, url, headers=self.headers, json=json_data)
        response.raise_for_status()
        return {} if response.status_code == 204 else response.json()

    def create_branch(self, base_branch: str, new_branch_name: str):
        """
        Cria uma nova branch no repositório a partir de uma branch base.
        """
        print(f"Criando a branch '{new_branch_name}' a partir de '{base_branch}'...")
        ref_data = self._make_request("GET", f"/git/refs/heads/{base_branch}")
        base_sha = ref_data['object']['sha']
        
        payload = {"ref": f"refs/heads/{new_branch_name}", "sha": base_sha}
        self._make_request("POST", "/git/refs", json_data=payload)
        print(f"Branch '{new_branch_name}' criada com sucesso.")

    def get_file_content(self, file_path: str, branch: str) -> tuple[str, str]:
        """
        Obtém o conteúdo e o SHA de um arquivo em uma branch específica.
        """
        response = self._make_request("GET", f"/contents/{file_path}?ref={branch}")
        content = base64.b64decode(response['content']).decode('utf-8')
        return content, response['sha']

    def update_file(self, file_path: str, branch: str, new_content: str, sha: str, message: str):
        """
        Atualiza um arquivo existente em uma branch.
        """
        payload = {
            "message": message,
            "content": base64.b64encode(new_content.encode('utf-8')).decode('utf-8'),
            "sha": sha,
            "branch": branch,
        }
        self._make_request("PUT", f"/contents/{file_path}", json_data=payload)
        print(f"Arquivo '{file_path}' atualizado com sucesso.")

    def create_file(self, file_path: str, branch: str, content: str, message: str):
        """
        Cria um novo arquivo em uma branch. O GitHub cria os diretórios necessários.
        """
        payload = {
            "message": message,
            "content": base64.b64encode(content.encode('utf-8')).decode('utf-8'),
            "branch": branch,
        }
        self._make_request("PUT", f"/contents/{file_path}", json_data=payload)
        print(f"Arquivo '{file_path}' criado com sucesso.")

    def create_or_update_file(self, file_path: str, branch: str, new_content: str, commit_message: str):
        """
        Cria um novo arquivo ou atualiza um existente, substituindo o conteúdo.
        Verifica se há mudanças antes de commitar.
        """
        try:
            # Tenta ler o arquivo para obter o SHA (necessário para atualização)
            original_content, sha = self.get_file_content(file_path, branch)
            
            # Compara o conteúdo para evitar commits desnecessários
            if original_content.strip() == new_content.strip():
                print(f"O conteúdo de '{file_path}' já está atualizado. Nenhum commit necessário.")
                return
            
            # Se o conteúdo for diferente, atualiza
            print(f"Atualizando o arquivo '{file_path}'...")
            self.update_file(file_path, branch, new_content, sha, commit_message)
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Se o arquivo não existe, cria um novo
                print(f"Criando o novo arquivo '{file_path}'...")
                self.create_file(file_path, branch, new_content, commit_message)
            else:
                raise # Propaga outros erros HTTP

    def upsert_sqlx_file(self, file_path: str, branch: str, new_content: str, commit_message: str):
        """
        Cria um novo arquivo .sqlx ou atualiza um existente se o conteúdo for diferente.

        Args:
            file_path: O caminho completo para o arquivo no repositório.
            branch: O nome da branch onde a operação será realizada.
            new_content: O conteúdo completo que o arquivo deve ter.
            commit_message: A mensagem de commit a ser usada.
        """
        try:
            # 1. Tenta ler o arquivo para ver se ele existe
            original_content, sha = self.get_file_content(file_path, branch)
            
            # 2. Se o arquivo existe, compara o conteúdo
            if original_content.strip() == new_content.strip():
                print(f"O conteúdo de '{file_path}' já está atualizado. Nenhum commit necessário.")
                return # Sai da função sem fazer nada
            else:
                # 3. Se o conteúdo for diferente, atualiza o arquivo
                print(f"O conteúdo de '{file_path}' está desatualizado. Atualizando...")
                self.update_file(file_path, branch, new_content, sha, commit_message)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # 4. Se o arquivo não existe (404), cria um novo
                print(f"O arquivo '{file_path}' não existe. Criando...")
                self.create_file(file_path, branch, new_content, commit_message)
            else:
                # 5. Se for qualquer outro erro, propaga a exceção
                raise