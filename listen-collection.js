const { MongoClient } = require('mongodb');

module.exports = function (RED) {
    function OnChangeStreamNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // MongoDB Configuration from node properties
        const mongoUrl = config.mongoUrl;
        const dbName = config.dbName;
        const collectionName = config.collection;

        let client;
        let changeStream;

        // Connect to MongoDB and start watching the specified collection
        async function startChangeStream() {
            try {
                node.status({ fill: "yellow", shape: "dot", text: "connecting..." });

                // Connect to MongoDB
                client = await MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });
                const db = client.db(dbName);
                const collection = db.collection(collectionName);

                node.status({ fill: "green", shape: "dot", text: "connected" });

                // Start watching the change stream
                changeStream = collection.watch();

                // Listen for change events
                changeStream.on('change', (change) => {
                    // Send the change event as payload
                    node.send({ payload: change });
                });

                node.status({ fill: "green", shape: "dot", text: "watching for changes..." });

            } catch (err) {
                node.error("MongoDB connection error: " + err.message);
                node.status({ fill: "red", shape: "dot", text: "error" });
            }
        }

        // Stop the change stream and close MongoDB connection when the node is closed
        node.on('close', async function (done) {
            if (changeStream) {
                await changeStream.close();
            }
            if (client) {
                await client.close();
            }
            node.status({});
            done();
        });

        // Initialize the change stream when the node is initialized
        startChangeStream();
    }

    RED.nodes.registerType("listen-collection", OnChangeStreamNode);
};
