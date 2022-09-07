FROM python:3.8.13-slim

WORKDIR /app

COPY smd2013.geojson /app/data
COPY smd2023.geojson /app/data
COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY main.py /app/
CMD ["python", "main.py"]
