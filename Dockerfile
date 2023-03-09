FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
COPY . instagramstories
RUN mv instagramstories/main.py .
CMD ["python3", "main.py"]