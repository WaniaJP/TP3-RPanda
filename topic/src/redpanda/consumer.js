import {Kafka, logLevel} from "kafkajs";
import {getLocalBroker} from "../config/config.js";
import { createClient } from 'redis';

const isLocalBroker = getLocalBroker()
const redpanda = new Kafka({
    brokers: [
        isLocalBroker ? `${process.env.HOST_IP}:9092` : 'localhost:19092',
        'localhost:19092'],
});

const consumer = redpanda.consumer({ groupId: '...' });
const url = "redis://myredis";
const pwd = "redispwd";

let  client;
export const connexion = async (topic) => {
    await consumer.connect();
    await consumer.subscribe({topics : [topic]});
    //await consumer.subscribe({topics : [topic], fromBeginning: true});
    client = await getClient();
    await client.connect().then(()=> {
        console.log('yes ! ')
    })


    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            let messageJson = JSON.parse(message.value.toString());

            let motsInMessage = messageJson.message.split(" ");
            console.log(motsInMessage);

            await setValue(motsInMessage);
            /*
            console.log({
                date: getDateFromTimestamp(message.timestamp.toString()),
                utilisateur: messageJson.user.toString(),
                message: messageJson.message,
            })*/
        },
    })
}
export const disconnect = async () => {
    try {
        await consumer.disconnect();
    } catch (error) {
        console.error("Error:", error);
    }
}

export function getDateFromTimestamp(timestamp) {
    return new Date(parseInt(timestamp)).toLocaleString();
}

export const getClient = async() => {
    return createClient({
        host : url,
        username : 'default',
        password: pwd,
        port : "6379"
    });
}

export const setValue = async(words) => {

    for (const word of words){

        if( await client.exists(word) !== 1){
            await client.set(word, 0)
        }
        await client.incr(word);
    }
}