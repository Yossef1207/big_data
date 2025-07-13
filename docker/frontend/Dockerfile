FROM node:20-alpine

WORKDIR /app/frontend

COPY frontend/ ./

RUN npm install \
    npm ci --production=false && \
    npm run build

EXPOSE 5173
CMD ["npm", "run", "preview", "--", "--host", "0.0.0.0", "--port", "5173"]
