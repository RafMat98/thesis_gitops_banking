FROM python:3.12-slim

RUN apt-get update && rm -rf /var/lib/apt/lists/*

RUN useradd -m bankuser
WORKDIR /home/bankuser/app

RUN pip install --no-cache-dir fastapi uvicorn aiokafka paramiko

COPY main.py .

RUN chown -R bankuser:bankuser /home/bankuser/app

USER bankuser
EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]