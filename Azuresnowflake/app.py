from fastapi import FastAPI
from pydantic import BaseModel
import subprocess
import uvicorn

app = FastAPI()

class DBTCommand(BaseModel):
    command: str

def execute_dbt(cmd_list):
    result = subprocess.run(
        cmd_list,
        capture_output=True,
        text=True
    )
    return {
        "command": " ".join(cmd_list),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode
    }

@app.get("/")
def root():
    return {"message": "DBT FastAPI Service Running!"}

# -------------------
# Prebuilt dbt commands
# -------------------
@app.get("/dbt/run")
def run():
    return execute_dbt(["dbt", "run"])

@app.get("/dbt/test")
def test():
    return execute_dbt(["dbt", "test"])

@app.get("/dbt/debug")
def debug():
    return execute_dbt(["dbt", "debug"])

@app.get("/dbt/build")
def build():
    return execute_dbt(["dbt", "build"])

@app.get("/dbt/seed")
def seed():
    return execute_dbt(["dbt", "seed"])

@app.get("/dbt/snapshot")
def snapshot():
    return execute_dbt(["dbt", "snapshot"])

@app.get("/dbt/full-refresh")
def full_refresh():
    return execute_dbt(["dbt", "run", "--full-refresh"])

@app.get("/dbt/select/{model}")
def run_specific_model(model: str):
    return execute_dbt(["dbt", "run", "--select", model])

# -------------------
# Generic executor
# -------------------
@app.post("/dbt/execute")
def execute_custom_dbt(cmd: DBTCommand):
    command_list = cmd.command.split()
    return execute_dbt(command_list)

# Only for local (not used in Docker)
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
