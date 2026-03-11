# Stage 1: Build the React frontend
FROM node:20-alpine AS build

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html vite.config.ts tailwind.config.ts postcss.config.js tsconfig*.json ./
COPY src/ src/
COPY public/ public/
RUN npm run build

# Stage 2: Serve with nginx
FROM nginx:1.27-alpine

# Remove default config
RUN rm /etc/nginx/conf.d/default.conf

COPY docker/nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
