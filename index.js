const debug = require('debug')('my:index');
const kafka = require('kafka-node');
const R = require('ramda');

const { kafkaHost } = require('./config');

const client = new kafka.KafkaClient({ kafkaHost });

const Producer = kafka.Producer;
const KeyedMessage = kafka.KeyedMessage;
const producer = new Producer(client);
const km = new KeyedMessage('key', 'message');
const payloads = [
  { topic: 'test1', messages: 'hi' },
];

const Consumer = kafka.Consumer;
const consumer = new Consumer(
  client,
  [
    { topic: 'test1' },
  ],
  {
    autoCommit: false,
  }
);

client.once('connect', function () {
	client.loadMetadataForTopics([], function (err, results) {
	  if (err) {
	  	return console.error(err);
	  }
	  console.log(R.keys(results[1].metadata));
  });

  producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
      if (err) {
        return console.error(err);
      }
      console.log(data);
    });
  });

  producer.on('err', err => console.error(err));


  consumer.on('message', (message) => {
    console.log(message);
  });

  consumer.on('err', err => console.error(err));
});
