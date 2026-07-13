# Stage 1: build
# Node 22: pnpm 10+ uses node:sqlite and requires Node >= 22.13. node:20 breaks
# the build with "No such built-in module: node:sqlite".
FROM node:22-alpine AS builder

# Install pnpm. Pin a specific major instead of @latest so a future pnpm release
# cannot silently break this build again by bumping its Node floor.
RUN corepack enable && corepack prepare pnpm@10.12.1 --activate

WORKDIR /app

# Copy package manifests first to leverage layer caching
COPY package.json pnpm-lock.yaml* ./

# Install all dependencies (including devDeps for build)
RUN pnpm install --frozen-lockfile

# Copy source
COPY . .

# Vite inlines VITE_* env vars at build time. They must be provided as Docker
# build args so they are available during `pnpm build`. They are intentionally
# scoped to this builder stage only — the nginx runner stage below never sees
# them, so the API key does not leak into the final image environment.
ARG VITE_API_BASE_URL
ARG VITE_API_KEY
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
ENV VITE_API_KEY=$VITE_API_KEY

# Build production bundle
RUN pnpm build

# Stage 2: serve
FROM nginx:alpine AS runner

# Copy custom nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
