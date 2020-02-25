FROM otrojota/geoportal:gdal-nodejs
WORKDIR /opt/geoportal/geop-huinay
COPY . .
RUN npm install 
EXPOSE 8186
CMD node index.js