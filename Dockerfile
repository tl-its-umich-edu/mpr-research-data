FROM python:3.9-slim
RUN apt-get update && apt-get install -y procps
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/app/start.sh"]
