FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt* setup.cfg .pre-commit-config.yaml ./
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir .
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python","email_bot.py"]
