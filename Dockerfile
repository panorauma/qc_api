FROM python:3.14.3-slim
WORKDIR app/

RUN apt-get update && apt-get install -y build-essential curl && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip

COPY ./checks_core ./checks_core
RUN pip install --no-cache-dir -r ./checks_core/requirements.txt

EXPOSE 8000
CMD ["python3","app.py"]