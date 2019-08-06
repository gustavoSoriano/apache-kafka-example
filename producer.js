const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

//Configuração do kafka
const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: '@soriano'
})


const topic    = 'topic-queue'
const producer = kafka.producer()

async function run() {
  
  for(let x=0; x < 10000; x++)
  {
    const message = {text: `Mensagem de teste [${x}]`}
    await producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [
        { value: JSON.stringify(message) }
      ]
    })
  }

  setTimeout( () => process.exit(), 500)
}

run().catch(console.error)

process.once('SIGINT', async () => await producer.disconnect() )
