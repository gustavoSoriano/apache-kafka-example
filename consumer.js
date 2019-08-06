const { Kafka, logLevel } = require('kafkajs')

//Configuração do kafka
const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: '@soriano'
})



const topic    = 'topic-queue'
const consumer = kafka.consumer({ groupId: 'queue-group' })

const Await = time => new Promise( res => setTimeout( () => res(true), time) )

async function run() {
  await consumer.connect()
  await consumer.subscribe({ topic })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} | ${message.key}#${message.value}`)
      await Await(100)
      console.log( JSON.parse(message.value) )
    }
  })
}

run().catch(console.error)

process.once('SIGINT', async () => await consumer.disconnect() )
