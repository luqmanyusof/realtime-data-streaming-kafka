// Topic 18: simulates the river stations, one reading per station every second.
//   node simulator.js                  normal weather
//   node simulator.js --flood KLT01    heavy rain at KLT01: its water level keeps rising
//   node simulator.js --faulty         also sends one broken reading (a sensor error)
import { kafka, TOPIC, disconnectOnExit } from './kafka.js';
import { STATIONS } from './stations.js';

const args = process.argv.slice(2);
const floodStation = args.includes('--flood') ? args[args.indexOf('--flood') + 1] : null;
const sendFaulty = args.includes('--faulty');

const producer = kafka.producer();
await producer.connect();
disconnectOnExit(producer);

const levels = Object.fromEntries(STATIONS.map((station) => [station.id, station.normal]));
let round = 0;

function newReading(station) {
  const flooding = station.id === floodStation;
  levels[station.id] = flooding
    ? Math.min(levels[station.id] + 0.05, station.danger + 1) // rises 0.05 m per reading
    : station.normal + (Math.random() - 0.5) * 0.2; // wanders a little around normal
  const reading = {
    stesen: station.id,
    masa: new Date().toISOString(),
    paras_air: Number(levels[station.id].toFixed(2)),
    hujan: Number((flooding ? 20 + Math.random() * 40 : Math.random() * 3).toFixed(1)),
  };
  return { key: station.id, value: JSON.stringify(reading) };
}

console.log(`Simulator started: ${STATIONS.length} stations, every second. Ctrl+C to stop.`);
setInterval(async () => {
  round++;
  const messages = STATIONS.map(newReading);
  if (sendFaulty && round === 5) {
    const faulty = { stesen: 'KDH01', masa: new Date().toISOString(), paras_air: 'ERR', hujan: null };
    messages.push({ key: 'KDH01', value: JSON.stringify(faulty) });
    console.log('⚠️  Sent a faulty reading from KDH01:', JSON.stringify(faulty));
  }
  try {
    await producer.send({ topic: TOPIC, messages });
    console.log(`[${new Date().toLocaleTimeString('en-MY')}] sent ${messages.length} readings`);
  } catch (error) {
    console.error('Send failed:', error.message);
  }
}, 1000);
