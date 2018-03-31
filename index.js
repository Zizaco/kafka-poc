#!/usr/bin/env node

const role = process.argv[2]
const topic = process.argv[3] || 'kafkapoc.user.update'
const help = `Usage: ./index.js <role> [topic]
role: 'producer' or 'consumer'.
topic: topic of messages to produce or subscribe (default: 'rabbitpoc.user.update')`

if (role !== 'producer' && role !== 'consumer') {
  console.log(help)
  process.exit(0)
}

console.log(`Running as ${role}`)
const program = require(`./src/${role}/`)

program(topic).then((exitCode) => {
  process.exit(exitCode || 0)
}).catch((err) => {
  console.error(err)
  process.exit(1)
})
