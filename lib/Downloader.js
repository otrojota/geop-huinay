const mongoDB = require("./MongoDB");
const moment = require("moment-timezone");
const config = require("./Config").getConfig();
const request = require("request");

class Downloader {
    constructor() {        
    }
    static get instance() {
        if (!Downloader.singleton) Downloader.singleton = new Downloader();
        return Downloader.singleton;
    }

    init() {
        this.callDownload(1000);
    }
    callDownload(ms) {
        if (!ms) ms = 60000 * 15;
        if (this.timer) clearTimeout(this.timer);
        this.timer = setTimeout(_ => {
            this.timer = null;
            this.download()
        }, ms);
    }

    async download() {
        try {
            if (!mongoDB.connected) {
                console.error("[Downloader] MongoDB desconectado. Se aborta descarga.");
                return;
            }
            let promises = [];
            config.estaciones.forEach(e => {
                promises.push(this.downloadEstacion(e));
            });
            await Promise.all(promises);
        } catch(error) {
            console.error("Error en Downloader", error);
        } finally {
            this.callDownload();
        }
    }

    upsertOne(col, doc) {
        return new Promise((resolve, reject) => {
            col.updateOne({_id:doc._id}, {$set:doc}, {upsert:true}, err => {
                if (err) reject(err);
                else resolve();
            })
        })
    }
    async updateVariables(e, docs) {
        try {
            let col = await mongoDB.collection(e.codigo);
            let times = Object.keys(docs);
            for (let i=0; i<times.length; i++) {
                await this.upsertOne(col, docs[times[i]]);
            }
        } catch(error) {
            console.error("[HUINAY] Actualizando colección Mongo", error);
        }
    }
    downloadEstacion(e) {
        if (e.tipo == "hobolink") {
            return new Promise(resolve => {
                let t1 = moment.tz("UTC");
                let t0 = t1.clone();
                t0.subtract(2, "hours");
                
                let options = {
                    uri:"https://webservice.hobolink.com/restv2/data/json",
                    method:"POST",
                    json:{
                        authentication:{user:e.user, password:e.password, token:e.apiToken},
                        query:{
                            start_date_time:t0.format("YYYY-MM-DD HH:mm:ss"),                            
                            end_date_time:t1.format("YYYY-MM-DD HH:mm:ss"),
                            loggers:[e.nroSerie]
                        }
                    }
                }
                request(options, (err, res, body) => {
                    if (err) {
                        console.error("Error en request de estacion '" + e.codigo + "'", err);
                        resolve();
                        return;
                    } else if (res && res.statusCode != 200) {
                        console.error("Error en response de estacion '" + e.codigo + "' [" + res.statusCode + "]:" + res.statusText, err);
                        resolve();
                        return;
                    }
                    try {
                        //console.log("body", body);
                        let docs = {};
                        body.observationList.forEach(o => {
                            let variable = e.variables.find(v => v.sensor == o.sensor_sn);
                            if (variable) {
                                let time = moment.tz(o.timestamp, "UTC");
                                let doc = docs[time.valueOf()];
                                if (!doc) {
                                    doc = {_id:time.valueOf()};
                                    docs[time.valueOf()] = doc
                                }
                                if (variable.usarValorEscalado) {
                                    doc[variable.codigo] = o.scaled_value;
                                } else {
                                    doc[variable.codigo] = o.si_value;
                                }
                            }
                        })
                        //console.log("docs", docs);
                        this.updateVariables(e, docs)
                            .then(_ => resolve())
                            .catch(_ => resolve());
               
                    } catch(error) {
                        console.error("Error descargando estacion '" + e.codigo + "'", error);
                        resolve();
                    }
                });
            });  
        } else {
            throw "Tipo de estación '" + e.tipo + "' no manejado";
        }
    }
}

module.exports = Downloader.instance;