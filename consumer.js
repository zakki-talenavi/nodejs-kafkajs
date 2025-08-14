import { Kafka, logLevel } from "kafkajs";

async function startConsumer() {
  const kafka = new Kafka({
    clientId: "my-consumer", // Biar log dan metrics lebih jelas
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    logLevel: logLevel.INFO,
  });

  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "nodejs",
  });

  try {
    console.log("🔌 Connecting to Kafka...");
    await consumer.connect();

    console.log("📡 Subscribing to topic: notification-topic");
    await consumer.subscribe({
      topic: "notification-topic",
      fromBeginning: true,
    });

    console.log("✅ Consumer is now listening...");
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `📥 [${topic} | partition ${partition}] ${message.value?.toString()}`
        );
      },
    });

    // Graceful shutdown
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
