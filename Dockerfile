# Base images come from Docker Hardened Images (dhi.io).
# Pulling them requires `docker login dhi.io` with Docker Hub credentials.

FROM dhi.io/node:22-alpine3.22-dev AS frontend
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

FROM dhi.io/rust:1.98-alpine3.22-dev AS backend
ARG APP_VERSION=0.0.1
# C toolchain for the bundled SQLite and mimalloc builds. Rust's musl target
# links statically, so the runtime image needs no extra libraries.
RUN apk add --no-cache build-base
# Embeds the crate list in the binary, so Trivy can scan the Rust dependencies.
RUN cargo install cargo-auditable --locked --version 0.7.7
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Build the dependencies alone first: this layer stays cached until
# Cargo.toml or Cargo.lock changes, so a code change rebuilds only vpsiner.
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
	&& cargo auditable build --release --locked \
	&& rm -rf src
COPY src ./src
# `touch`: COPY keeps the original mtime, which can be older than the
# placeholder build above, so Cargo would skip rebuilding vpsiner.
RUN touch src/main.rs \
	&& sed -i "/^\[package\]/,/^version = /s/^version = \".*\"/version = \"${APP_VERSION}\"/" Cargo.toml \
	&& sed -i "/^name = \"vpsiner\"$/{n;s/^version = \".*\"/version = \"${APP_VERSION}\"/;}" Cargo.lock \
	&& cargo auditable build --release --locked \
	&& mkdir -p /out/data /out/config

# Minimal runtime: no shell or package manager.
FROM dhi.io/alpine-base:3.22
WORKDIR /app
COPY --from=backend /app/target/release/vpsiner /usr/local/bin/vpsiner
COPY --from=frontend /app/frontend/dist /usr/local/share/vpsiner/static
# Owned by the nonroot user so `--user 65532:65532` works with fresh named volumes.
COPY --from=backend --chown=65532:65532 /out/data /data
COPY --from=backend --chown=65532:65532 /out/config /config
EXPOSE 3000
ENV VPSINER_PORT=3000 VPSINER_DATA_PATH=/data VPSINER_CONFIG_PATH=/config VPSINER_STATIC_DIR=/usr/local/share/vpsiner/static
VOLUME ["/data", "/config"]
# DHI runtime images default to a nonroot user. VPSiner runs as root by default:
# access to the Docker socket is root-equivalent anyway, and root works with a
# mounted socket, existing root-owned data and rootless Docker without setup.
# See the README for running as nonroot.
# Numeric UID/GID: the DHI base has no `root` entry in /etc/passwd.
USER 0:0
ENTRYPOINT ["/usr/local/bin/vpsiner"]
CMD []
