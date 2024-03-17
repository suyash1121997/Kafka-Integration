const kafka = require('kafka-node');
const MongoClient = require('mongodb').MongoClient;

// Kafka consumer configuration
const kafkaClientOptions = { kafkaHost: 'localhost:9092' }; // Adjust this according to your Kafka setup
const kafkaConsumerOptions = { groupId: 'my-group', autoCommit: true, autoCommitIntervalMs: 5000 };

// MongoDB configuration
const mongoUrl = 'mongodb://localhost:27017'; // Adjust this according to your MongoDB setup
const mongoDbName = 'mydb'; // Adjust this according to your MongoDB database name
const mongoCollectionName = 'messages'; // Adjust this according to your MongoDB collection name

// Create a Kafka consumer
const consumerGroup = new kafka.ConsumerGroup(Object.assign({ groupId: 'my-group2' }, kafkaClientOptions), ['kafka-mongo-tests']);

MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    console.error('Failed to connect to MongoDB:', err);
    return;
  }
  
  console.log('Connected to MongoDB');
  const db = client.db(mongoDbName);
  const collection = db.collection(mongoCollectionName);

  // Consume messages from Kafka
  consumerGroup.on('message', async function (message) {
    try {
      console.log('Received message:', message);
      // Save the message into MongoDB
      await collection.insertOne({ value: message.value });
      console.log('Message saved to MongoDB');
    } catch (error) {
      console.error('Failed to save message to MongoDB:', error);
    }
  });

  consumerGroup.on('error', function (err) {
    console.error('Error with Kafka consumer:', err);
  });

  // Close MongoDB connection when the Node.js process exits
  process.on('SIGINT', () => {
    console.log('Closing MongoDB connection');
    client.close();
    process.exit();
  });
});
