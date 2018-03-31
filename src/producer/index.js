const Kafka = require('node-rdkafka');

let msgCounter = 1

module.exports = async function (topic) {
  const producer = await createKafkaProducer('localhost:9092')

  if (topic.includes('*') || topic.includes('#')) {
    throw Error('topic cannot contain wildcards (*, #) when producing messages')
  }

  showLogsOf(producer)

  setInterval(() => {
    sendMessage(producer, topic)
  }, 500)
  
  setInterval(() => {
    producer.poll();
  }, 3000)

  return new Promise((resolve) => {
    setTimeout(() => { resolve(0) }, 200000)
  })
}

function sendMessage(producer, topic) {
  const user = {
    _id: msgCounter++,
    data: { foo: 'bar' }
  }
  const msg = Buffer.from(JSON.stringify(user))

  producer.produce(
    topic,
    null,
    msg,
    undefined,
    Date.now()
  )
  console.log(`sent ${topic}:${msg}`);
}

/**
 * Opens a connection with Kafka as a producer
 * @param {string} url 
 * @returns {Promise<Kafka.Producer>}
 */
function createKafkaProducer(url) {
  const producer = new Kafka.Producer({
    'metadata.broker.list': url,
    'dr_cb': true
  });

  producer.connect();

  return new Promise((resolve, reject) => {
    producer.on('ready', () => resolve(producer))
    producer.on('event.error', (err) => reject(err))
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