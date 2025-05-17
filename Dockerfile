FROM python:3.12-slim

WORKDIR /app
COPY kvstore.py server.py /app/

# default shard count (override at runtime)
ENV SHARD_COUNT=4

EXPOSE 8000
CMD ["python", "server.py"]