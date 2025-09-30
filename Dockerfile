FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  DEBUGPY=0

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
  && pip install --no-cache-dir debugpy

COPY . .

CMD ["bash","-lc","\
  if [ \"$DEBUGPY\" = \"1\" ]; then \
  python -m debugpy --listen 0.0.0.0:5678 -m streamlit run src/app.py --server.address=0.0.0.0 --server.port=8501; \
  else \
  streamlit run src/app.py --server.address=0.0.0.0 --server.port=8501; \
  fi \
  "]