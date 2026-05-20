from fastapi import FastAPI, HTTPException
import paramiko
import json
import re
import os
import asyncio
import ssl
import threading
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from contextlib import asynccontextmanager


KAFKA_BROKER = os.getenv("KAFKA_BROKER")
CONSUME_TOPIC = os.getenv("CONSUME_TOPIC")
PRODUCE_TOPIC = os.getenv("PRODUCE_TOPIC")
GROUP_ID = os.getenv("GROUP_ID")

CA_CERT = "cobol_certs/ca.crt"
USER_CERT = "cobol_certs/user.crt"
USER_KEY = "cobol_certs/user.key"
CA_CERT = "cluster_certs/ca.crt"

ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=CA_CERT)
ssl_context.load_cert_chain(certfile=USER_CERT, keyfile=USER_KEY)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_REQUIRED

# --- SETTINGS FOR SSH (PUB400) ---
SSH_HOST = os.getenv("SSH_HOST", "pub400.com")
SSH_PORT = int(os.getenv("SSH_PORT"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")

ssh_client = None
ssh_lock = threading.Lock()

def create_ssh_client() -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        SSH_HOST,
        port=SSH_PORT,
        username=SSH_USER,
        password=SSH_PASS,
        timeout=10
    )
    client.get_transport().set_keepalive(60)
    print(" [SSH] Connected to Mainframe!")
    return client

def get_ssh_client() -> paramiko.SSHClient:
    global ssh_client
    with ssh_lock:
        transport = ssh_client.get_transport() if ssh_client else None
        if transport is None or not transport.is_active():
            print(" [SSH] Connection lost, reconnecting...")
            ssh_client = create_ssh_client()
    return ssh_client

# --- ΛΟΓΙΚΗ ΕΚΤΕΛΕΣΗΣ (SSH COBOL) ---
def run_cobol_ssh(account_id: str):
    client = get_ssh_client()
    cmd = f"/usr/bin/qsh -c \"system \\\"CALL PGM(RMAT981/READER) PARM('{account_id}')\\\" 2>&1\""

    with ssh_lock:
        stdin, stdout, stderr = client.exec_command(cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()

    full_output = f"{result} {error}".strip()
    print(f" [DEBUG] SSH command executed successfully. Received {len(full_output)} bytes.")
    match = re.search(r'\{.*\}', full_output, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    else:
        raise Exception(f"No JSON found. SSH Output was: {full_output[:100]}")

# --- BACKGROUND TASK: KAFKA CONSUMER & PRODUCER ---
async def kafka_consumer_loop():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        security_protocol="SSL",
        ssl_context=ssl_context,
        auto_offset_reset='earliest'
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol="SSL",
        ssl_context=ssl_context
    )

    while True:
        try:
            await consumer.start()
            await producer.start()
            print(f" [KAFKA] Connected to {KAFKA_BROKER}")
            break
        except Exception as e:
            print(f" [KAFKA] Waiting for broker... ({e})")
            await asyncio.sleep(5)

    try:
        # Η κύρια λούπα που ακούει για μηνύματα
        async for msg in consumer:
            try:
                account_id = msg.value.decode('utf-8')
                print(f" [KAFKA IN] Received: {account_id}")
                
                loop = asyncio.get_running_loop()
                cobol_result = await loop.run_in_executor(None, run_cobol_ssh, account_id)

                balance = cobol_result.get("bal", "0000000000")
                final_message = f"{account_id}+{balance}"
                final_log = {
                    "account_id": account_id,
                    "balance": "********",  # Hide actual balance in logs for security,
                    "status": "PROCESSED"
                }
                await producer.send_and_wait(PRODUCE_TOPIC, final_message.encode('utf-8'))
                print(f" [KAFKA OUT] Published: {final_log}")

            except Exception as e:
                # Αν σκάσει ΕΝΑ μήνυμα, το πιάνουμε εδώ για να ΜΗΝ σταματήσει η λούπα
                print(f" [ERROR] Processing message failed: {e}")
                print(f" [DEBUG] Final log for failed message: {final_log}")
    except asyncio.CancelledError:
        # Εδώ ερχόμαστε ΜΟΝΟ όταν το Kubernetes κάνει kill το pod
        print("[SHUTDOWN] Received stop signal. Stopping Kafka consumer loop...")
    except Exception as e:
        print(f"[FATAL ERROR] Unexpected error in Kafka loop: {e}")
    finally:
        # Κλείνουμε τον Kafka με ασφάλεια
        print("[SHUTDOWN] Closing Kafka connections...")
        await consumer.stop()
        await producer.stop()
        print("[SHUTDOWN] Graceful shutdown complete.")

# --- FASTAPI LIFESPAN & ENDPOINTS ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global ssh_client
    # Αρχικοποίηση SSH όταν ξεκινάει η εφαρμογή
    ssh_client = create_ssh_client()
    
    task = asyncio.create_task(kafka_consumer_loop())
    yield
    
    # Διαδικασία τερματισμού
    task.cancel()
    try:
        await task  # Περιμένουμε να τερματίσει ομαλά ο Kafka
    except asyncio.CancelledError:
        print("[SHUTDOWN] Kafka background task cancelled successfully.")
        
    # Κλείνουμε και το SSH
    if ssh_client:
        ssh_client.close()
        print("[SHUTDOWN] SSH Connection closed.")

app = FastAPI(lifespan=lifespan, title="Legacy SSH Wrapper API")

@app.get("/balance/{account_id}")
async def get_balance(account_id: str):
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, run_cobol_ssh, account_id)
        return {"status": "SUCCESS", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))