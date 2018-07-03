FROM node:alpine
RUN npm install -g http-server
VOLUME /var/www
ENTRYPOINT ["http-server", "/var/www", "--cors"]

