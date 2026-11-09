// Topic 19: watch every reading and publish a flood alert when a station's status changes.
//   telemetri-sungai → alert.js → amaran-banjir
import { kafka, TOPIC, disconnectOnExit } from './kafka.js';
import { findStation, levelStatus } from './stations.js';

const consumer = kafka.consumer({ groupId: 'alert' });
const producer = kafka.producer();
await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: TOPIC });
disconnectOnExit(consumer, producer);
console.log('Watching river levels... (Ctrl+C to stop)');

const lastStatus = {}; // the last status of each station

// Returns the reading, or null if it is not a usable reading. Never throws.
function readReading(value) {
  try {
    const reading = JSON.parse(value.toString());
    return findStation(reading.stesen) && typeof reading.paras_air === 'number' ? reading : null;
  } catch {
    return null;
  }
}

await consumer.run({
  eachMessage: async ({ message }) => {
    const reading = readReading(message.value);
    if (!reading) {
      console.log(`SKIP    bad reading: ${message.value.toString()}`);
      return;
    }

    const station = findStation(reading.stesen);
    const status = levelStatus(station, reading.paras_air);
    const previous = lastStatus[station.id];
    lastStatus[station.id] = status;

    // Alert only when the status changes, not on every reading
    if (status === previous || (!previous && status === 'NORMAL')) return;

    const alert = { stesen: station.id, lokasi: station.location, dari: previous || 'TIDAK DIKETAHUI', kepada: status, paras_air: reading.paras_air, masa: reading.masa };
    await producer.send({ topic: 'amaran-banjir', messages: [{ key: station.id, value: JSON.stringify(alert) }] });
    console.log(`AMARAN  ${station.id} ${station.location}: ${alert.dari} → ${status} (${reading.paras_air} m)`);
  },
});
