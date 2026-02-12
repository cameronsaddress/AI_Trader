FROM node:20-alpine AS base

FROM base AS builder
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app
RUN npm install -g turbo
COPY . .
RUN turbo prune --scope=@ai-trader/backend --docker

FROM base AS installer
WORKDIR /app

# First install dependencies (as they change less often)
COPY --from=builder /app/out/json/ .
COPY --from=builder /app/out/package-lock.json ./package-lock.json
COPY --from=builder /app/tsconfig.json ./tsconfig.json
RUN npm install

# Build the project
COPY --from=builder /app/out/full/ .

FROM installer AS builder-output
RUN npm run build --workspace=@ai-trader/backend

FROM base AS runner
WORKDIR /app

# Don't run as root
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 expressjs
USER expressjs

COPY --from=builder-output /app/apps/backend/package.json .
COPY --from=builder-output /app/apps/backend/dist ./dist
COPY --from=installer /app/node_modules ./node_modules

EXPOSE 5114

CMD ["node", "dist/index.js"]
