import {getConfigNumber, getDebug, getTimeOut, getTopic, getTypeMessage, getNumberWord} from "./config/config.js";
import {connexion} from "./redpanda/consumer.js";

const configNumber = getConfigNumber()
const typeMessage = getTypeMessage()
const topic = getTopic()
const debug = getDebug()

async function start() {
    connexion(topic);
}

start()