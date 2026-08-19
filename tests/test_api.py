from fastapi.testclient import TestClient
from scheduler.api import app



client = TestClient(app)


def test_health_check():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_create_job():
    response = client.post( "/jobs", json={
            "type": "matrix_multiply", "payload": { "size": 100 }, "priority": 5, },)
    assert response.status_code == 200
    job = response.json()
    assert job["type"] == "matrix_multiply"
    assert job["payload"]["size"] == 100
    assert job["priority"] == 5
    assert job["status"] == "queued"
    assert job["worker_id"] is None
    assert job["retry_count"] == 0

def test_create_job_rejects_invalid_priority():
    response = client.post(
        "/jobs",
        json={
            "type": "matrix_multiply",
            "payload": {"size": 100},
            "priority": 50,
        },
    )

    assert response.status_code == 422