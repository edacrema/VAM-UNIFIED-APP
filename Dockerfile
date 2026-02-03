FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
COPY start.sh /start.sh

RUN sed -i 's/\r$//' /start.sh \
    && chmod +x /start.sh

EXPOSE 8080

CMD ["/start.sh"]
