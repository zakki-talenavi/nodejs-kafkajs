import { Kafka, logLevel } from "kafkajs";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "my-consumer",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
  logLevel: logLevel.INFO,
  ssl: process.env.KAFKA_USE_SSL === "true", // true jika pakai SSL (port 9093)
  sasl: process.env.KAFKA_SASL_MECHANISM
    ? {
        mechanism: process.env.KAFKA_SASL_MECHANISM, // plain | scram-sha-256 | scram-sha-512
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
      }
    : undefined,
});

export const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL || "http://localhost:8181",
  auth:
    process.env.SCHEMA_REGISTRY_USER && process.env.SCHEMA_REGISTRY_PASS
      ? {
          username: process.env.SCHEMA_REGISTRY_USER,
          password: process.env.SCHEMA_REGISTRY_PASS,
        }
      : undefined,
});
