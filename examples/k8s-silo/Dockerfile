# Runnable silo image for the Kubernetes e2e. No build step: the silo runs under
# vite-node (the repo's dev toolchain), so the whole workspace is copied and
# installed. Build from the repo root:
#   docker build -t tsva-k8s-silo:dev -f examples/k8s-silo/Dockerfile .
FROM node:22-slim
WORKDIR /app

RUN corepack enable

# Install dependencies first for layer caching, then the sources.
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json ./
COPY . .
RUN pnpm install --frozen-lockfile

EXPOSE 11111 8080 8081
CMD ["pnpm", "--filter", "@tsva/example-k8s-silo", "start"]
