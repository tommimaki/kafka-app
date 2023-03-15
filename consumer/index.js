import { Kafka } from "kafkajs";

console.log("*** Consumer starts... ***");

const kafka = new Kafka({
  clientId: "checker-server",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "kafka-checker-servers" });
const producer = kafka.producer();

// const isValidID = (id) => {
//   return id.length === 11 && !isNaN(parseInt(id[0]));
// };

const fakeDatabase = new Set([
  "311299-999X",
  "01070349999",
  "240588+9999",
  "NNN588+9999",
  "112233-9999",
  "300233-9999",
  "30233-9999",
]);

const isValidID = (id) => {
  // Basic checks: length and first character is a digit
  if (id.length !== 11 || isNaN(parseInt(id[0]))) {
    return false;
  }

  // Check against the fake database
  if (!fakeDatabase.has(id)) {
    return false;
  }

  return true;
};

const run = async () => {
  // Consuming
  await consumer.connect();
  await consumer.subscribe({ topic: "tobechecked", fromBeginning: true });

  // Producing
  await producer.connect();

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const id = message.value.toString();
      const validationResult = isValidID(id) ? "Valid ID" : "Invalid ID";

      // Send the result to the 'checkedresult' channel
      await producer.send({
        topic: "checkedresult",
        messages: [
          {
            key: message.key.toString(),
            value: validationResult,
          },
        ],
      });

      console.log("Checked ID:", id);
    },
  });
};

run().catch(console.error);
