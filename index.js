global.confPath = __dirname + "/config.json";
global.resDir = __dirname + "/resources";
const config = require("./lib/Config").getConfig();
const ProveedorCapasHuinay = require("./lib/ProveedorCapasHuinay");

let downloader = false;
for (let i=2; i<process.argv.length; i++) {
    let arg = process.argv[i].toLowerCase();
    if (arg == "-d" || arg == "-download" || arg == "-downloader") downloader = true;
}
if (!downloader && process.env.DOWNLOADER) {
    downloader = true;
}

const proveedorCapas = new ProveedorCapasHuinay({
    puertoHTTP:config.webServer.http.port,
    directorioWeb:__dirname + "/www",
    directorioPublicacion:null
});
require("./lib/MongoDB").init()
    .then(_ => {
        if (downloader) {
            console.log("[HUINAY] Iniciando en modo Downloader");
            require("./lib/Downloader").init();
        }        
        proveedorCapas.start();
    })
    .catch(err => {
        console.error("Error Inicializando MongoDB", err);
        proveedorCapas.start();
    })

