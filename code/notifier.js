// Topic 20: react to flood alerts. Its own consumer group, so it gets every alert.
//   amaran-banjir → notifier.js → an SMS (here: a line on the screen)
import { kafka, disconnectOnExit } from './kafka.js';

const consumer = kafka.consumer({ groupId: 'notifier' });
await consumer.connect();
await consumer.subscribe({ topic: 'amaran-banjir' });
disconnectOnExit(consumer);
console.log('Waiting for flood alerts... (Ctrl+C to stop)');

await consumer.run({
  eachMessage: async ({ message }) => {
    const alert = JSON.parse(message.value.toString());
    const time = new Date(alert.masa).toLocaleTimeString('en-MY');
    if (alert.kepada === 'AMARAN' || alert.kepada === 'BAHAYA') {
      console.log(`📱 SMS to ${alert.lokasi} district office: ${alert.stesen} is at ${alert.kepada}, ${alert.paras_air} m (${time})`);
    } else {
      console.log(`   ${alert.stesen}: ${alert.dari} → ${alert.kepada}, no SMS needed (${time})`);
    }
  },
});
