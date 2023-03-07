FROM python:3.10-slim
WORKDIR usr/src/app
COPY requirements.txt .
RUN pip install -r requierments.txt --no-cache-dir
COPY . src
CMD ["python3", "instastoryparser/instaStoryParser.py"]