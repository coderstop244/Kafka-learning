const { kafka } = require("./client.js");
const readline = require("readline");

const r1 = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();
    console.log("Connecting producer...");
    await producer.connect();
    console.log("Producer connected");

    r1.setPrompt(">");
    r1.prompt();

    r1.on("line", async (line) => {
        const [riderName, location] = line.trim().split(/\s+/);

        if (!riderName || !location) {
            console.error("Enter a rider name and location, for example: Tony North");
            r1.prompt();
            return;
        }

        try {
            await producer.send({
                topic: "rider-updates",
                messages: [
                    {
                        partition: location.toLowerCase() === "north" ? 0 : 1,
                        key: "location-update",
                        value: JSON.stringify({ name: riderName, location }),
                    },
                ],
            });
            console.log("Message sent successfully");
        } catch (error) {
            console.error("Failed to send message:", error);
        }

        r1.prompt();
    });

    r1.on("close", async () => {
        await producer.disconnect();
        console.log("Producer disconnected");
    });
}

init().catch((error) => {
    console.error("Producer operation failed:", error);
    process.exitCode = 1;
});