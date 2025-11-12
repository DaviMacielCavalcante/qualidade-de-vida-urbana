"""
Testes de autenticação (rotas /auth/*).
"""

from .models.test_user import User


def test_database_fixture_works(test_db):
    """
    Valida que a fixture test_db funciona corretamente.
    """
    
    user = User(
        name="Test User",
        email="test@test.com",
        password="hashed_password",
        phone_number="11999999999",
        role="user",
        signatureStatus="active"
    )
    
    test_db.add(user)
    test_db.commit()
    

    found = test_db.query(User).filter(User.email == "test@test.com").first()
    
    assert found is not None
    assert found.email == "test@test.com"
    assert found.name == "Test User"


def test_client_fixture_works(client):
    """
    Valida que a fixture client funciona.
    """    

    response = client.get("/")

    assert response.status_code in [200, 404]

def test_user_fixture_creates_user(test_user, test_db):
    """
    Valida que test_user cria usuário no banco.
    """

    from tests.models.test_user import User

    assert "user" in test_user
    assert "email" in test_user
    assert "password" in test_user

    assert test_user["email"] == "teste@teste.com"
    assert test_user["role"] == "user"

    found = test_db.query(User).filter(User.email == test_user["email"]).first()

    assert found is not None
    assert found.id is not None

def test_user_fixture_creates_admin(test_admin, test_db):
    """
    Valida que test_user cria usuário no banco.
    """

    from tests.models.test_user import User

    assert "user" in test_admin
    assert "email" in test_admin
    assert "password" in test_admin

    assert test_admin["email"] == "admin@admin.com"
    assert test_admin["role"] == "admin"

    found = test_db.query(User).filter(User.email == test_admin["email"]).first()

    assert found is not None
    assert found.id is not None

def test_authenticated_client_is_logged(authenticated_client):
    """
    Valida que authenticated_client está logado.
    """

    response = authenticated_client.get("/auth/me")

    assert response.status_code == 200
    assert response.json()["email"] == "teste@teste.com"
    assert response.json()["role"] == "user"

def test_authenticated_admin_is_logged(authenticated_admin):
    """
    Valida que authenticated_admin está logado.
    """

    response = authenticated_admin.get("/auth/me")

    assert response.status_code == 200
    assert response.json()["email"] == "admin@admin.com"
    assert response.json()["role"] == "admin"    

def test_admin_can_access_protected_route(authenticated_admin):
    """
    Valida que admin consegue acessar rotas protegidas.
    """
    response = authenticated_admin.get("/users/all")
    
    assert response.status_code == 200


def test_user_cannot_access_admin_route(authenticated_client):
    """
    Valida que user comum NÃO consegue acessar rotas de admin.
    """
    response = authenticated_client.get("/users/all")

    assert response.status_code == 403

def test_login_success(client, test_user):
    """
    Cenário: Usuário faz login com credenciais válidas
    
    Dado: Um usuário existe no banco
    Quando: Faz POST /auth/login com email e senha corretos
    Então: 
        - Status 200
        - Cookie access_token é setado
        - Resposta contém mensagem de sucesso
    """
    
    response = client.post("/auth/login", data={
        "username": test_user["email"],  
        "password": test_user["password"],  
        "grant_type": "password"
    })
    
    assert response.status_code == 200
    assert "access_token" in response.cookies  
    assert response.json() == {"message": "Login successful"}

def test_login_invalid_email(client, test_user):
    """
    Cenário: Tentar login com email que não existe
    
    Dado: Email não está cadastrado
    Quando: Faz POST /auth/login
    Então:
        - Status 401
        - Sem cookie
        - Mensagem de erro
    """
    
    response = client.post("/auth/login", data={
        "username": "nao_existo@naoexisto.com",  # Email que NÃO existe
        "password": "qualquer_senha",
        "grant_type": "password"
    })
    
    assert response.status_code == 401
    assert "access_token" not in response.cookies

def test_login_wrong_password(client, test_user):
    """
    Cenário: Tentar login com senha incorreta
    
    Dado: Usuário existe no banco
    Quando: Faz POST /auth/login com senha errada
    Então:
        - Status 422
        - Sem cookie
        - Mensagem de erro
    """
    
    response = client.post("/auth/login", data={
        "username": test_user["email"],  
        "password": ".", 
        "grant_type": "password"
    })
    
    assert response.status_code == 401
    assert "access_token" not in response.cookies

def test_register_duplicate_email(client, test_user):
    """
    Tentar registrar com email já usado deve retornar 400.
    """
    duplicate_data = {
        "id": "999",
        "email": test_user["email"],  
        "password": "outra_senha",
        "name": "Outro Nome",
        "phone_number": "11888888888",
        "role": "user",
        "signatureStatus": "active",
        "notifications": "yes"
    }
    
    response = client.post("/auth/register", json=duplicate_data)
    
    assert response.status_code == 400  
    assert "access_token" not in response.cookies

def test_me_not_authenticated(client, test_user):
    """
    Cenário: Acessar /auth/me sem estar logado
    
    Dado: Não fez login (sem cookie)
    Quando: GET /auth/me
    Então:
        - Status 401 (Unauthorized)
        - Mensagem de erro
    """

    response = client.get("/auth/me")
    
    assert response.status_code == 401

def test_logout(authenticated_client):
    """
    Cenário: Fazer logout
    
    Dado: Usuário está logado (authenticated_client já tem cookie)
    Quando: POST /auth/logout
    Então:
        - Status 200
        - Cookie é removido ou invalidado
        - Não consegue mais acessar rotas protegidas
    """

    me_before = authenticated_client.get("/auth/me")
    assert me_before.status_code == 200  # Estava logado

    response = authenticated_client.post("/auth/logout")
    
    assert response.status_code == 200

    me_after = authenticated_client.get("/auth/me")
    assert me_after.status_code == 401  # Não está mais logado