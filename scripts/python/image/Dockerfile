FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE 1
# Logging output to console immediately
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# Dependencies for the script
RUN pip install --no-cache-dir redis

# Copy the script into the container
COPY seed.py .

CMD ["python", "seed.py"]