import { Kafka } from "kafkajs";
import { v4 as UUID } from "uuid";

console.log("*** Producer starts... ***");

const kafka = new Kafka({
  clientId: "my-checking-client",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "producer-group" });

const run = async () => {
  // Producing
  await producer.connect();

  // Consuming from 'checkedresult'
  await consumer.connect();
  await consumer.subscribe({ topic: "checkedresult", fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("Checked ID result:", message.value.toString());
    },
  });

  setInterval(() => {
    queueMessage();
  }, 2500);
};

run().catch(console.error);

const idNumbers = [
  "311299-999X",
  "01070349999",
  "240588+9999",
  "NNN588+9999",
  "112233-9999",
  "300233-9999",
  "30233-9999",
];

function randomizeIntegerBetween(from, to) {
  return Math.floor(Math.random() * (to - from + 1)) + from;
}

async function queueMessage() {
  const uuidFraction = UUID().substring(0, 4); // First four hex characters of an UUID
  // UUID v4 are like this: '1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbdAbed'
  const success = await producer.send({
    topic: "tobechecked",
    messages: [
      {
        key: uuidFraction,
        value: Buffer.from(
          idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]
        ),
      },
    ],
  });

  if (success) {
    console.log(`Message ${uuidFraction} successfully to the stream`);
  } else {
    console.log("Problem writing to stream..");
  }
}
