import {Kafka, logLevel} from "kafkajs";
import {getLocalBroker} from "../config/config.js";

const isLocalBroker = getLocalBroker()
const redpanda = new Kafka({
    brokers: [
        isLocalBroker ? `${process.env.HOST_IP}:9092` : 'localhost:19092',
        'localhost:19092'],
});

const consumer = redpanda.consumer({ groupId: '...' });
export const connexion = async (topic) => {
    await consumer.connect();
    await consumer.subscribe({topics : [topic], fromBeginning: true});

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            let messageJson = JSON.parse(message.value.toString());
            console.log({
                date: getDateFromTimestamp(message.timestamp.toString()),
                utilisateur: messageJson.user.toString(),
                message: messageJson.message.toString(),
            })
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