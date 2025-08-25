import { kafka, registry } from "./kafkaConfig.js";

async function startConsumer() {
  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "nodejs",
  });
  const topic = process.env.KAFKA_TOPIC || "notification-topic";

  try {
    console.log(`🔌 Connecting to Kafka broker: ${process.env.KAFKA_BROKER}`);
    await consumer.connect();

    console.log(`📡 Subscribing to topic: ${topic}`);
    await consumer.subscribe({ topic, fromBeginning: false });

    console.log("✅ Consumer is now listening...");
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // Decode Avro message using Schema Registry
          const decodedValue = await registry.decode(message.value);
          console.log(
            `📥 [${topic} | partition ${partition} | offset ${message.offset}]`,
            decodedValue
          );
        } catch (err) {
          console.error("❌ Failed to decode message:", err);
        }
      },
    });

    // ✅ Graceful shutdown
    const shutdown = async () => {
      console.log("\n🛑 Disconnecting Kafka consumer...");
      await consumer.disconnect();
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  } catch (error) {
    console.error("❌ Error in consumer:", error);
    await consumer.disconnect();
    process.exit(1);
  }
}

startConsumer();
