from fastapi import FastAPI, HTTPException
import paramiko
import json
import re
import os
import asyncio
import ssl
import threading
import time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from contextlib import asynccontextmanager

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
CONSUME_TOPIC = os.getenv("CONSUME_TOPIC")
PRODUCE_TOPIC = os.getenv("PRODUCE_TOPIC")
GROUP_ID = os.getenv("GROUP_ID")

CA_CERT = "cluster_certs/ca.crt"
USER_CERT = "cobol_certs/user.crt"
USER_KEY = "cobol_certs/user.key"

ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=CA_CERT)
ssl_context.load_cert_chain(certfile=USER_CERT, keyfile=USER_KEY)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_REQUIRED

# --- SETTINGS FOR SSH (PUB400) ---
SSH_HOST = os.getenv("SSH_HOST", "pub400.com")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")

ssh_client = None
ssh_shell = None  # Communication channel (PTY)
ssh_lock = threading.Lock()

def create_ssh_shell():
    global ssh_client, ssh_shell
    print(" [SSH] Initiating persistent connection to Mainframe...")
    
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        SSH_HOST,
        port=SSH_PORT,
        username=SSH_USER,
        password=SSH_PASS,
        timeout=15
    )
    client.get_transport().set_keepalive(60)
    
    # 1. Open a persistent shell session (PTY) to keep the connection alive and avoid re-authentication overhead
    shell = client.invoke_shell()
    
    # 2. Wait for the initial banner of the AS/400 (PUB400) to be displayed
    time.sleep(2)
    if shell.recv_ready():
        shell.recv(9999)
        
    # 3. We enter the QShell and keep it open!
    shell.send("qsh\n")
    time.sleep(1.5) # We give the QSH time to load
    if shell.recv_ready():
        shell.recv(9999) # We clear the buffer

    print(" [SSH] Persistent QSH Shell is READY!")
    return client, shell

def get_active_shell():
    global ssh_client, ssh_shell
    with ssh_lock:
        transport = ssh_client.get_transport() if ssh_client else None
        # We check if the transport AND the PTY channel are alive
        if transport is None or not transport.is_active() or ssh_shell is None or ssh_shell.closed:
            print(" [SSH] Connection or Shell lost, reconnecting...")
            ssh_client, ssh_shell = create_ssh_shell()
    return ssh_shell

# --- LOGIC FOR EXECUTION (INTERACTIVE STREAM) ---
def run_cobol_ssh(account_id: str):
    shell = get_active_shell()
    
    # The termination marker remains as a "safety net" (fallback)
    marker = f"END_OF_{account_id}"
    cmd = f"system \"CALL PGM(RMAT981/READER) PARM('{account_id}')\"; echo '{marker}'\n"

    with ssh_lock:
        # 1. We clear the buffer from previous runs
        while shell.recv_ready():
            shell.recv(4096)
            
        # 2. We send the command to the Mainframe
        shell.send(cmd)
        
        output = ""
        while True:

            if shell.recv_ready():
                chunk = shell.recv(4096).decode('utf-8', errors='ignore')
                output += chunk
            else:
                time.sleep(0.01)
            match = re.search(r'\{[^{}]*"acc"\s*:\s*"' + account_id + r'"[^}]*\}', output)
            if match:
                try:
                    result = json.loads(match.group(0))
                    print(f" [DEBUG] Smart Parse Success. Stream bytes: {len(output)}")
                    return result
                except json.JSONDecodeError:
                    pass 

            
            if output.count(marker) >= 2:
                break
                
    
    raise Exception(f"Command completed but no valid JSON found for {account_id}. Raw Output: {output[-200:]}")
# --- BACKGROUND TASK: KAFKA CONSUMER & PRODUCER ---
async def kafka_consumer_loop():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        security_protocol="SSL",
        ssl_context=ssl_context,
        auto_offset_reset='earliest',
        max_poll_records=5, # We fetch in small batches to avoid timeout
        max_poll_interval_ms=300000
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
        async for msg in consumer:
            final_log = {}
            try:
                account_id = msg.value.decode('utf-8')
                print(f" [KAFKA IN] Received: {account_id}")
                
                loop = asyncio.get_running_loop()
                cobol_result = None
                success = False
                
                # --- RETRY (Max 3 ATTEMPTS) ---
                MAX_RETRIES = 3
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        #  COBOL
                        cobol_result = await loop.run_in_executor(None, run_cobol_ssh, account_id)
                        success = True
                        break 
                        
                    except Exception as e:
                        print(f" [WARNING] Attempt {attempt}/{MAX_RETRIES} failed for {account_id}. Reason: {e}")
                        if attempt < MAX_RETRIES:
                            print(f" [RETRY] Waiting 1 seconds before retrying...")
                            await asyncio.sleep(1) 
                
                
                if success:
                    balance = cobol_result.get("bal", "0000000000")
                    final_message = f"{account_id}+{balance}"
                    final_log = {"account_id": account_id, "balance": "********", "status": "PROCESSED"}
                    
                    await producer.send_and_wait(PRODUCE_TOPIC, final_message.encode('utf-8'))
                    print(f" [KAFKA OUT] Published: {final_log}")
                    
                else:
                    print(f" [ERROR] All {MAX_RETRIES} attempts failed for {account_id}. Sending Fallback.")
                    fallback_message = f"{account_id}+0000000000"
                    fallback_log = {"account_id": account_id, "balance": "0.00", "status": "SYSTEM_ERROR"}
                    
                    await producer.send_and_wait(PRODUCE_TOPIC, fallback_message.encode('utf-8'))
                    print(f" [KAFKA OUT] Published Fallback: {fallback_log}")
            except Exception as e:
                print(f" [FATAL ERROR] Failed to process incoming message completely: {e}")
    except asyncio.CancelledError:
        print("[SHUTDOWN] Received stop signal. Stopping Kafka consumer loop...")
    except Exception as e:
        print(f"[FATAL ERROR] Unexpected error in Kafka loop: {e}")
    finally:
        print("[SHUTDOWN] Closing Kafka connections...")
        await consumer.stop()
        await producer.stop()
        print("[SHUTDOWN] Graceful shutdown complete.")

# --- FASTAPI LIFESPAN & ENDPOINTS ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global ssh_client, ssh_shell
    try:
        ssh_client, ssh_shell = create_ssh_shell()  # ← Fix
    except Exception as e:
        print(f" [FATAL] Failed to initialize SSH Shell on startup: {e}")
    task = asyncio.create_task(kafka_consumer_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("[SHUTDOWN] Kafka background task cancelled successfully.")
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