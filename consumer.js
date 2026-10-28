const { kafka } = require("./client.js");
const group = process.argv[2];
async function init() {
    const consumer = kafka.consumer({ groupId: "group" });
    console.log("Connecting consumer...");
    await consumer.connect();
    console.log("Consumer connected");

    await consumer.subscribe({
        topic: "rider-updates",
        fromBeginning: true,
    });
    console.log("Consumer subscribed to rider-updates");

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(
                `${group}:[${topic}]: PART:${partition}:`,
                message.value.toString()
            );
        },
    });
}

init().catch((error) => {
    console.error("Consumer operation failed:", error);
    process.exitCode = 1;
});