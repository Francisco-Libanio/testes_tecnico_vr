import pytest
from main import app  # importa sua app Flask

@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client

def test_producer_post(client):
    payload = {
        "conteudoMensagem": "Olá mundo",
        "tipoNotificacao": "SMS"
    }
    response = client.post("/send", json=payload)
    assert response.status_code == 202
