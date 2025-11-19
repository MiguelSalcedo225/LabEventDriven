# Use same base pattern as other services (node)
FROM node:18-alpine

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --only=production

COPY . .

EXPOSE 3008

CMD ["node", "index.js"]
