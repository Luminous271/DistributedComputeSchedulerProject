from fastapi import FastAPI

app = FastAPI(
    title = "Distributed Compute Scheduler",
    version = "0.1.0"
)

@app.get("/health") 
def health_check() :
    return {"status": "healthy"}