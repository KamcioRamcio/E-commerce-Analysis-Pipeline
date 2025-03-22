FROM node:20-alpine

WORKDIR /app
COPY docs/package*.json ./
RUN npm install
COPY docs/ ./

EXPOSE 3000
CMD ["npm", "run", "dev"]