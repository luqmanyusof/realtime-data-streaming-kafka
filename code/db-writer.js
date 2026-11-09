// Topic 21: save every reading to PostgreSQL, so the history is kept for reports.
//   telemetri-sungai → db-writer.js → table bacaan
import pg from 'pg';
import { kafka, TOPIC, disconnectOnExit } from './kafka.js';
import { findStation } from './stations.js';

// Lab-only connection details, matching docker-compose.yml
const database = new pg.Pool({ host: 'localhost', port: 5433, user: 'kafka', password: 'kafka-lab', database: 'sungai' });
// Without this, a database restart crashes the whole program through an idle connection
database.on('error', (error) => console.warn('Database connection lost:', error.message));

// One row per reading. The primary key makes it impossible to save the same reading twice.
await database.query(`CREATE TABLE IF NOT EXISTS bacaan (
  stesen    TEXT          NOT NULL,
  masa      TIMESTAMPTZ   NOT NULL,
  paras_air NUMERIC(6, 2) NOT NULL,
  hujan     NUMERIC(6, 1),
  PRIMARY KEY (stesen, masa)
)`);
const INSERT = 'INSERT INTO bacaan (stesen, masa, paras_air, hujan) VALUES ($1, $2, $3, $4) ON CONFLICT (stesen, masa) DO NOTHING';

const consumer = kafka.consumer({ groupId: 'db-writer' });
await consumer.connect();
await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
disconnectOnExit(consumer);
console.log('Saving readings to PostgreSQL... (Ctrl+C to stop)');

let saved = 0;
let alreadyThere = 0;

await consumer.run({
  eachMessage: async ({ message, heartbeat }) => {
    let reading;
    try {
      reading = JSON.parse(message.value.toString());
    } catch {
      return; // bad data: skip it, as alert.js does
    }
    if (!findStation(reading.stesen) || typeof reading.paras_air !== 'number') return;

    // A database that is down is a temporary problem: wait and try again, never skip the reading
    for (;;) {
      try {
        const result = await database.query(INSERT, [reading.stesen, reading.masa, reading.paras_air, reading.hujan]);
        if (result.rowCount === 1) saved++;
        else alreadyThere++;
        return;
      } catch (error) {
        console.warn(`Database not reachable (${error.code || error.message}). Trying again in 2 s`);
        await new Promise((resolve) => setTimeout(resolve, 2000));
        await heartbeat(); // tell Kafka this consumer is still alive while it waits
      }
    }
  },
});

setInterval(() => console.log(`Saved ${saved} new rows, ${alreadyThere} already there`), 5000);
