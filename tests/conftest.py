"""
Fixtures compartilhadas para todos os testes.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models.test_user import TestBase, User


@pytest.fixture(scope="function")
def test_db():
    """
    Cria banco SQLite em memória para cada teste.
    
    Cada teste começa com banco limpo (scope="function").
    """

    engine = create_engine("sqlite:///:memory:")
    
    TestBase.metadata.create_all(bind=engine)
    
    testing_session_local = sessionmaker(bind=engine)
    
    db = testing_session_local()
    
    yield db
    
    db.close()
    TestBase.metadata.drop_all(bind=engine)

@pytest.fixture
def client(test_db):
    """
    TestClient que usa banco de teste.
    
    Sobrescreve get_db para usar test_db (SQLite) 
    ao invés do banco de produção (PostgreSQL).
    """

    from fastapi.testclient import TestClient
    from main import app
    from app.database import get_db

    def overrirde_get_db():
        try:
            yield test_db
        finally:
            pass

    app.dependency_overrides[get_db] = overrirde_get_db

    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides.clear()

@pytest.fixture
def test_user(test_db):
    """
    Cria usuário comum no banco de teste.
    
    Returns:
        dict: {
            "user": User object (ORM),
            "email": string,
            "password": string (texto, não hash!),
            "name": string,
            "role": string
        }
    """    

    from tests.models.test_user import User
    from app.repositories.user_repository import get_pwd_hash

    user_data = {
        "email": "teste@teste.com",        
        "password": "teste1234",       
        "name": "teste",
        "phone_number": "5591999999999",
        "role": "user",           
        "signatureStatus": "active"
    }

    user = User(
        email = user_data["email"],
        password = get_pwd_hash(user_data["password"]),
        name=user_data["name"],
        phone_number = user_data["phone_number"],
        role=user_data["role"],
        signatureStatus=user_data["signatureStatus"]
    )

    test_db.add(user)
    test_db.commit()
    test_db.refresh(user)

    return {
        "user": user,
        "email": user_data["email"],      
        "password": user_data["password"],
        "name": user_data["name"],       
        "role": user_data["role"]         
    }

@pytest.fixture
def test_admin(test_db):
    """
    Cria usuário administrador no banco de teste.
    
    Returns:
        dict: {
            "user": User object (ORM),
            "email": string,
            "password": string (texto, não hash!),
            "name": string,
            "role": string
        }
    """    

    from tests.models.test_user import User
    from app.repositories.user_repository import get_pwd_hash

    user_data = {
        "email": "admin@admin.com",        
        "password": "adm1234",       
        "name": "admin",
        "phone_number": "5591999999999",
        "role": "admin",           
        "signatureStatus": "active"
    }

    user = User(
        email = user_data["email"],
        password = get_pwd_hash(user_data["password"]),
        name=user_data["name"],
        phone_number = user_data["phone_number"],
        role=user_data["role"],
        signatureStatus=user_data["signatureStatus"]
    )

    test_db.add(user)
    test_db.commit()
    test_db.refresh(user)

    return {
        "user": user,
        "email": user_data["email"],      
        "password": user_data["password"],
        "name": user_data["name"],        
        "role": user_data["role"]         
    }





# Próximas fixtures virão aqui:
# - client (TestClient)
# - test_user (usuário já criado no banco)
# - test_admin (admin já criado no banco)
# - authenticated_client (client logado)
# etc.