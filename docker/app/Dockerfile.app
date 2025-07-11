# syntax=docker/dockerfile:1

################################
# BUILD + RUNTIME = JEDEN ETAP
################################
FROM python:3.12-slim

# ----- system + node -----
RUN apt-get update && \
    apt-get install -y curl gnupg git build-essential \
                       libblas-dev liblapack-dev libatlas-base-dev \
                       ninja-build nodejs npm && \
    rm -rf /var/lib/apt/lists/*

# ----- python deps -----
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install -r requirements.txt

# ----- backend -----
COPY . /workspace

# ----- frontend build -----
WORKDIR /workspace/frontend
RUN npm ci
RUN npm run build   # zbuduje dist – Vue/React/Svelte itp.

# do podglądu podczas dev uruchamiamy preview
EXPOSE 8000 5173
WORKDIR /workspace/backend

# run both Daphne and Vite preview
CMD ["bash","-lc", "\
  npm --prefix ../frontend run preview -- --host 0.0.0.0 --port 5173 & \
  daphne -b 0.0.0.0 -p 8000 sentiment_backend.asgi:application \
"]