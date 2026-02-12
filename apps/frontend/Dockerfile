FROM node:20-alpine AS base

FROM base AS builder
RUN apk add --no-cache libc6-compat
WORKDIR /app
RUN npm install -g turbo
COPY . .
RUN turbo prune --scope=@ai-trader/frontend --docker

FROM base AS installer
WORKDIR /app

COPY --from=builder /app/out/json/ .
COPY --from=builder /app/out/package-lock.json ./package-lock.json
COPY --from=builder /app/tsconfig.json ./tsconfig.json
RUN npm install

COPY --from=builder /app/out/full/ .

FROM installer AS builder-output
RUN npm run build --workspace=@ai-trader/frontend

FROM nginx:alpine
COPY --from=builder-output /app/apps/frontend/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
