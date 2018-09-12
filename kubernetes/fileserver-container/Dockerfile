FROM nginx:1.15-alpine

COPY fileserver-start /bin/
COPY nginx.conf /root/nginx.conf.template
RUN mkdir -p /srv/www && echo 'Hello!' > /srv/www/hello.txt
CMD ["fileserver-start"]
