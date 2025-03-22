FROM node:20-alpine

WORKDIR /app
COPY package*.json ./
RUN npm i
COPY . .
RUN npm i -g vercel

EXPOSE 8888
CMD ["npm", "run", "dev"]
