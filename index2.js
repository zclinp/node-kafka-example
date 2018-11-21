const debug = require('debug')('my:index');
const R = require('ramda');
const Kafka = require('node-rdkafka');

const { kafkaHost } = require('./config');

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': kafkaHost,
}, {});

// Flowing mode
consumer.connect();

consumer
  .on('ready', function() {
    console.log('client ready');
    consumer.subscribe([
      'pentium.system.test02',
      'test',
      'topic2',
      'pentium.system.test01',
      'faas-request',
      'test1',
      'fluent.info',
      'test3',
      'fluent.warn',
      'topic1',
      'measure.disk',
      'pentium.system.tony2',
      'measure.cpu',
      '__consumer_offsets',
      'pentium.system.tony1',
      'measure.memory',
      'test2'
    ]);

    consumer.consume();
  })
  .on('data', function(data) {
    // Output the actual message contents
    console.log(data.value.toString());
  })
  .on('log', status => console.log(log))
  .on('status', status => console.log(status))
  .on('error', err => console.error(err));


var producer = new Kafka.Producer({
  'metadata.broker.list': kafkaHost,
  'dr_cb': true
});

producer.connect();

producer.on('ready', function() {
  console.log('producer ready');
  try {
    producer.produce(
      // Topic to send the message to
      'topic1',
      // optionally we can manually specify a partition for the message
      // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
      null,
      // Message to send. Must be a buffer
      new Buffer('Awesome message'),
      // for keyed messages, we also specify the key - note that this field is optional
      'Stormwind',
      // you can send a timestamp here. If your broker version supports it,
      // it will get added. Otherwise, we default to 0
      Date.now(),
      // you can send an opaque token here, which gets passed along
      // to your delivery reports
    );
  } catch (err) {
    console.error('A problem occurred when sending our message');
    console.error(err);
  }
});

// Any errors we encounter, including connection errors
producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
})
