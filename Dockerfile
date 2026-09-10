FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway ./gateway
COPY mock_upstream ./mock_upstream

# 同一镜像双角色：默认起网关；command 覆盖为 mock 起上游
EXPOSE 8080
ENTRYPOINT ["python", "-m"]
CMD ["gateway.app", "-c", "/etc/gateway/config.yaml"]
