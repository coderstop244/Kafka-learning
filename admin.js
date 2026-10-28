const { kafka } = require("./client.js");

async function init(){
    const admin = kafka.admin();
    console.log("Admin connecting...");
    await admin.connect();
    console.log("Admin connected");

    //creating topics 
    console.log("Creating topic [rider-updates]");
    await admin.createTopics({
        topics:[
            {
                topic:"rider-updates",
                numPartitions:2,
            },
        ],
    });
    console.log("Topic Created Success [rider-updates]");
    console.log("Disconnecting admin");
    await admin.disconnect();
}

init().catch((error) => {
    console.error("Admin operation failed:", error);
    process.exitCode = 1;
});