FROM python:3.11-slim

WORKDIR /usr/src/app

COPY src/requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./

RUN mkdir -p /usr/src/app/uploads && chmod 755 /usr/src/app/uploads

EXPOSE 5123

CMD ["python", "src/run.py"]