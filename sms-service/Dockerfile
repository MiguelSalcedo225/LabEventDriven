# Use same base pattern as other services (node)
FROM node:18-alpine

WORKDIR /usr/src/app

COPY package*.json ./
# Use npm install when package-lock.json is not present to avoid build failures
RUN npm install --only=production

COPY . .

EXPOSE 3008

CMD ["node", "index.js"]
