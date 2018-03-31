const Kafka = require('node-rdkafka');

module.exports = async function (topic) {
  const consumer = await createKafkaConsumer('localhost:9092')

  consumer.on('data', (data) => {
    handleMessage(data)
  })

  showLogsOf(consumer)

  consumer.subscribe([topic])
  
  setInterval(function () {
    consumer.consume()
  }, 1000);

  return new Promise((resolve) => {
    setTimeout(() => { resolve(0) }, 200000)
  })
}

const handleMessage = async function (msg) {
  console.log(`received`, msg.value.toString())
}

/**
 * Opens a connection with Kafka as a consumer
 * @param {string} url 
 * @returns {Promise<Kafka.Consumer>}
 */
function createKafkaConsumer(url) {
  const consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': url,
    'enable.auto.commit': false
  });

  consumer.connect();

  return new Promise((resolve, reject) => {
    consumer.on('ready', () => resolve(consumer))
    consumer.on('event.error', (err) => reject(err))
  })
}

/**
 * Register callbacks to the log events of kafka to output then to STDOUT
 * @param {Kafka.Client} kafkaClient 
 */
function showLogsOf(kafkaClient) {
  // logging debug messages, if debug is enabled
  kafkaClient.on('event.log', function (log) {
    console.log(log);
  })

  // logging disconnect
  kafkaClient.on('disconnected', function (arg) {
    console.log('kafka disconnected. ' + JSON.stringify(arg));
  })

  // logging all errors
  kafkaClient.on('event.error', function (err) {
    console.error('Error from producer');
    console.error(err);
  })
}