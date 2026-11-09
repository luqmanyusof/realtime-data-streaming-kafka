// Topic 14 + 16: send JSON river readings to Kafka, one message per station.
import { kafka, TOPIC } from './kafka.js';

const STATIONS = ['KLT01', 'PHG01', 'SGR01'];
const producer = kafka.producer();
await producer.connect();

for (let round = 1; round <= 5; round++) {
  for (const station of STATIONS) {
    const reading = {
      stesen: station,
      masa: new Date().toISOString(),
      paras_air: Number((18 + Math.random()).toFixed(2)),
      hujan: Number((Math.random() * 5).toFixed(1)),
    };
    const [result] = await producer.send({
      topic: TOPIC,
      messages: [{ key: station, value: JSON.stringify(reading) }],
    });
    console.log(`${station} → partition ${result.partition}, offset ${result.baseOffset}  paras_air=${reading.paras_air}`);
  }
  await new Promise((resolve) => setTimeout(resolve, 1000));
}

await producer.disconnect();
