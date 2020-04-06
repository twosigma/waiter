FROM nginx:1.16-alpine

RUN apk add --no-cache bash curl
COPY fileserver-start /bin/
COPY s3-log-backup /bin/
COPY nginx.conf /root/nginx.conf.template
WORKDIR /root
RUN mkdir -p /srv/www && echo 'Hello!' > /srv/www/hello.txt
CMD ["fileserver-start"]
