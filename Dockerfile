FROM node:22-slim AS frontend
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

FROM rust:1.98-slim AS backend
WORKDIR /app
COPY --from=frontend /app/frontend/dist ./frontend/dist
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=backend /app/target/release/vpsiner /usr/local/bin/vpsiner
COPY --from=backend /app/frontend/dist /usr/local/share/vpsiner/static
EXPOSE 3000
ENV VPSINER_PORT=3000 VPSINER_DATA_PATH=/data VPSINER_DOCKER_HOST=unix:///var/run/docker.sock VPSINER_STATIC_DIR=/usr/local/share/vpsiner/static
VOLUME ["/data"]
CMD ["/usr/local/bin/vpsiner"]
