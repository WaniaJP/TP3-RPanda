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
            console.log({
                time: getDateFromTimestamp(message.timestamp.toString()),
                value: message.value.toString(),
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