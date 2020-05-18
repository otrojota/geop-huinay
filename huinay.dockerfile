# docker build -f huinay.dockerfile -t otrojota/geoportal:huinay-0.17 .
# docker push otrojota/geoportal:huinay-0.17
#
FROM otrojota/geoportal:gdal-nodejs
WORKDIR /opt/geoportal/geop-huinay
COPY . .
RUN apt-get update
RUN apt-get -y install git
RUN npm install 
EXPOSE 8186
CMD node index.js