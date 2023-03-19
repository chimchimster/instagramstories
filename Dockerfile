FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN apt-get update \
    && apt-get -y install tesseract-ocr \
    && apt-get -y install tesseract-ocr-rus \
    && apt-get -y install tesseract-ocr-kaz \
    && pip install -r requirements.txt --no-cache-dir
COPY . instagramstories
RUN mv instagramstories/main.py .
CMD ["python3", "main.py"]