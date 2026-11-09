// Topic 15: read river readings. Run it twice with the same group to share the work.
// Usage: node consumer.js [group]      e.g. node consumer.js group-a
import { kafka, TOPIC, disconnectOnExit } from './kafka.js';

const groupId = process.argv[2] || 'group-a';
const consumer = kafka.consumer({ groupId });
await consumer.connect();
await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
disconnectOnExit(consumer);
console.log(`Group "${groupId}" is listening on ${TOPIC}... (Ctrl+C to stop)`);

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const key = message.key ? message.key.toString() : '(no key)';
    try {
      const reading = JSON.parse(message.value.toString());
      console.log(`p${partition} offset ${message.offset}  key=${key}  ${reading.stesen} paras_air=${reading.paras_air}`);
    } catch (error) {
      // Skip a message we cannot read instead of stopping (Topic 15, Try it)
      console.log(`p${partition} offset ${message.offset}  SKIPPED: ${error.message}`);
    }
  },
});
