'use strict'

const ES_HOST_URL = 'http://127.0.0.1:9200/' // change this
const ES_INDEX_NAME = 'photos' // change this

const { Client } = require('@elastic/elasticsearch')
const fetch = require('node-fetch');


const client = new Client({
  hosts: [ ES_HOST_URL ],
  nodes: [ ES_HOST_URL ]
})




async function run () {
  // Let's start by indexing some data
  await client.indices.create({
    index: ES_INDEX_NAME,
    operations: {
      mappings: {
        properties: {
          id: { type: 'integer' },
          albumId: { type: 'integer' },
          title: { type: 'text' },
          url: { type: 'url.full' },
          thumbnailUrl: { type: 'url.full' }
        }
      }
    }
  }, { ignore: [400] })

  const jsonz = await fetch('https://jsonplaceholder.typicode.com/photos/')
      .then(response => response.json())



  const operations = jsonz.flatMap(doc => [{ index: { _index: ES_INDEX_NAME, _id: doc.id } }, doc] )

  const bulkResponse = await client.bulk({ refresh: true, operations })

  if (bulkResponse.errors) {
    const erroredDocuments = []
    // The items array has the same order of the dataset we just indexed.
    // The presence of the `error` key indicates that the operation
    // that we did for the document has failed.
    bulkResponse.items.forEach((action, i) => {
      const operation = Object.keys(action)[0]
      if (action[operation].error) {
        erroredDocuments.push({
          // If the status is 429 it means that you can retry the document,
          // otherwise it's very likely a mapping error, and you should
          // fix the document before to try it again.
          status: action[operation].status,
          error: action[operation].error,
          operation: body[i * 2],
          document: body[i * 2 + 1]
        })
      }
    })
    console.log(erroredDocuments)
  }

  const count = await client.count({ index: ES_INDEX_NAME })
  console.log(count)

}

run().catch(console.log)