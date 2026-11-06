FROM python:3.13-slim
RUN pip install --no-cache-dir flask==3.1.*
WORKDIR /site
COPY . .
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
