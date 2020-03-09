const {ProveedorCapas, CapaObjetosConDatos, CapaVectorial} = require("geop-base-proveedor-capas");
const config = require("./Config").getConfig();
const mongoDB = require("./MongoDB");

class ProveedorCapasHuinay extends ProveedorCapas {
    constructor(opciones) {
        super("huinay", opciones);
        this.addOrigen("huinay", "Fundación Huinay", "http://www.huinay.cl/site/sp/", "./img/huinay.svg");
        let capaEstacionesHuinay = new CapaObjetosConDatos("huinay", "ESTACIONES_HUINAY", "Estaciones HUINAY", "huinay", {
            temporal:false,
            datosDinamicos:false,
            menuEstaciones:true
        }, ["cl-estaciones"], "img/huinay.svg");
        config.estaciones.forEach(e => {
            let vars = [];
            e.variables.forEach(v => {
                vars.push({
                    codigo:v.codigo, 
                    nombre:v.nombre,
                    formatos:v.formatos,
                    icono:v.icono,
                    temporal:true, 
                    unidad:v.opciones.unidad,
                    decimales:v.opciones.decimales
                })
            });
            let extraConfig = {
                configAnalisis:{analizador:"serie-tiempo", analizadores:{
                    "serie-tiempo":{
                        variable:"huinay.ESTACIONES_HUINAY." + e.codigo + ".TEMP", nivelVariable:0,
                        tiempo:{tipo:"relativo", from:-2, to:4}
                    }
                }}
            }
            capaEstacionesHuinay.agregaPunto(e.codigo, e.nombre, e.lat, e.lng, "img/estacion-huinay.svg", vars, extraConfig);
        });
        this.addCapa(capaEstacionesHuinay);

        let capaCatastro2019 = new CapaVectorial("huinay", "CATASTRO_HUINAY_2019", "HUINAY - Catastro 2019", "huinay", {
            temporal:false,
            formatos:{
                geoJSON:true,
            },
            estilos:function(f) {
                let tipo = f.properties._tipo;
                if (tipo == "huinay") return {color:"#ff0000", weight:1}
                else if (tipo == "predio") return {color:"#bfb341", weight:1}
                else if (tipo == "escuela") return {color:"#00ff00", weight:1}
                else if (tipo == "rio") return {color:"#0000ff", fillcolor:"#0000ff", weight:2}
            }
    }, [], "img/huinay.svg");
        this.addCapa(capaCatastro2019);

        // cache
        this.featuresCatastro2019 = this.getFeaturesCatastro2019();
    }

    async resuelveConsulta(formato, args) {
        try {
            if (formato == "serieTiempo") {
                return await this.generaSerieTiempo(args);
            } else if (formato == "valorEnPunto") {
                return await this.generaValorEnPunto(args);
            } else if (formato == "geoJSON") {
                return await this.generaGeoJSON(args);
            } else throw "Formato " + formato + " no soportado";
        } catch(error) {
            throw error;
        }
    }

    async generaSerieTiempo(args) {
        try {
            let cod = args.codigoVariable;
            let p = cod.indexOf(".");
            if (p <= 0) throw "codigoVariable inválido '" + cod + "'. Se esperaba 'codigoCapa.codigoObjeto.codigoVariable";
            let codigoCapa = cod.substr(0, p);
            if (codigoCapa == "ESTACIONES_HUINAY") {
                return this.generaSerieTiempoEstacionesHuinay(args, cod.substr(p+1));
            } else throw "Código de Capa '" + codigoCapa + "' no manejado";
            
        } catch(error) {
            throw error;
        }
    }
    async generaSerieTiempoEstacionesHuinay(args, cod) {
        try {
            let p = cod.indexOf(".");
            if (p < 0) throw "Código de variable inválido '" + args.codigoVariable + "'. Se esperaba codigo-capa.codigo-objeto.codigo-variable";
            let codigoEstacion = cod.substr(0, p);
            let codigoVariable = cod.substr(p+1);
            let estacion = config.estaciones.find(e => e.codigo == codigoEstacion);
            if (!estacion) throw "No se encontró la estación '" + codigoEstacion + "'";
            let variable = estacion.variables.find(v => v.codigo == codigoVariable);
            if (!variable) throw "No se encontró el datasource '" + codigoDS + "' en la estación '" + codigoEstacion + "'";
            let t0 = args.time0;
            let t1 = args.time1;
            let col = await mongoDB.collection(codigoEstacion);
            let rows = await col.find({$and:[{_id:{$gte:t0}}, {_id:{$lte:t1}}]}).sort({_id:1}).toArray();
            if (!rows.length) throw "No hay Datos para el Período";
            let ret = {
                lat:args.lat, lng:args.lng, unit:variable.opciones.unidad,
                time0:rows[0]._id, time1:rows[rows.length - 1]._id, levelIndex:args.levelIndex,
                data:rows.map(r => ({
                    time:r._id, value:r[codigoVariable], atributos:{Tiempo:r._id}
                }))
            }
            return ret;
        } catch(error) {
            throw error;
        }
    }

    async generaValorEnPunto(args) {
        try {
            let cod = args.codigoVariable;
            let p = cod.indexOf(".");
            if (p <= 0) throw "codigoVariable inválido '" + cod + "'. Se esperaba 'codigoCapa.codigoObjeto.codigoVariable";
            let codigoCapa = cod.substr(0, p);
            if (codigoCapa == "ESTACIONES_HUINAY") {
                return this.generaValorEnPuntoEstacionesHuinay(args, cod.substr(p+1));
            } else throw "Código de Capa '" + codigoCapa + "' no manejado";
            
        } catch(error) {
            throw error;
        }
    }
    async generaValorEnPuntoEstacionesHuinay(args, cod) {
        try {
            let p = cod.indexOf(".");
            if (p < 0) throw "Código de variable inválido '" + args.codigoVariable + "'. Se esperaba codigo-capa.codigo-objeto.codigo-variable";
            let codigoEstacion = cod.substr(0, p);
            let codigoVariable = cod.substr(p+1);
            let estacion = config.estaciones.find(e => e.codigo == codigoEstacion);
            if (!estacion) throw "No se encontró la estación '" + codigoEstacion + "'";
            let variable = estacion.variables.find(v => v.codigo == codigoVariable);
            if (!variable) throw "No se encontró el datasource '" + codigoDS + "' en la estación '" + codigoEstacion + "'";
            let t0 = args.time - 3 * 60 * 60 * 1000;
            let t1 = args.time + 3 * 60 * 60 * 1000;
            let col = await mongoDB.collection(codigoEstacion);
            let rows = await col.find({$and:[{_id:{$gte:t0}}, {_id:{$lte:t1}}]}).toArray();
            let idx=-1, delta = undefined, value = undefined, time = undefined;
            rows.forEach((r, i) => {
                let d = Math.abs(r._id - args.time);
                if (r[codigoVariable] !== undefined) {
                    if (idx < 0 || d < delta) {
                        idx = i; delta = d; value = r[codigoVariable], time = r._id;
                    }
                }
            })
            let ret = {
                lat:args.lat, lng:args.lng, unit:variable.opciones.unidad,
                time:time, levelIndex:args.levelIndex, value:value, atributos:{Tiempo:time}
            }
            return ret;
        } catch(error) {
            throw error;
        }
    }

    async generaGeoJSON(args) {
        try {           
            if (args.codigoVariable == "CATASTRO_HUINAY_2019") {
                return this.featuresCatastro2019;
            } else throw "Código de Capa '" + args.codigoVariable + "' no manejado";            
        } catch(error) {
            throw error;
        }
    }

    getFeaturesCatastro2019() {
        let path = global.resDir + "/catastro2019.geojson";
        let features = JSON.parse(require("fs").readFileSync(path));
        features.name = "Huinay - Catastro 2019";
        features.vistaInicial = {zoom:10, lat:-42.379576, lng:-72.415347}
        let nRios = 0;
        features.features.forEach(f => {
            let props = f.properties;
            console.log("name", props.Name, props.NOMBRE_PREDIO, props.PROPIETARIO);
            if (props.NOMBRE_PREDIO.startsWith("Río ")) {
                props.id = "RIO_" + (++nRios);
                props._icono = "img/iconos/rio.svg";
                props._tipo = "rio";
                props._orden = 5;
                props._titulo = "Río: " + props.NOMBRE_PREDIO;
            } else if (props.NOMBRE_PREDIO.toLowerCase().indexOf("escuela") >= 0) {
                props.id = props.Name.substr(3);
                props._icono = "img/iconos/escuela.svg";
                props._tipo = "escuela";
                props._orden = 4;
                props._titulo = "Escuela: " + props.NOMBRE_PREDIO;
            } else if (props.PROPIETARIO.toLowerCase().indexOf("huinay") >= 0) {
                props.id = props.Name.substr(3);
                props._icono = "img/iconos/huinay.svg";
                props._tipo = "huinay";
                props._orden = 3;
                props._titulo = "Predio Huinay: " + props.NOMBRE_PREDIO;
            } else {
                props.id = props.Name.substr(3);
                props._icono = "img/iconos/predio.svg";
                props._tipo = "predio";
                props._orden = 2;
                props._titulo = "Predio: " + props.NOMBRE_PREDIO;
            }
        });
        features.features.sort((f1, f2) => (f1.properties._orden - f2.properties._orden));
        console.log("features", features);
        return features;
    }
}

module.exports = ProveedorCapasHuinay;