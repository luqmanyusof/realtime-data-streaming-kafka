// One place for the Kafka connection settings. Every lab file uses this.
import { Kafka, logLevel } from 'kafkajs';

// KafkaJS 2 warns that its default partitioner changed. We want the new one, so hide the warning.
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

export const kafka = new Kafka({ clientId: 'kafka-lab', brokers: ['localhost:9092'], logLevel: logLevel.WARN });
export const TOPIC = 'telemetri-sungai';

// On Ctrl+C, leave Kafka cleanly so the consumer group rebalances at once
export function disconnectOnExit(...clients) {
  const stop = async () => {
    for (const client of clients) await client.disconnect();
    process.exit(0);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}
