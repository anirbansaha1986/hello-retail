'use strict'

const aws = require('aws-sdk') // eslint-disable-line import/no-unresolved, import/no-extraneous-dependencies

// TODO Make a dynamoDB in a service that holds all of the schema and a schema-getter and validator, instead of listing them out here
const AJV = require('ajv')

const ajv = new AJV()
const makeSchemaId = schema => `${schema.self.vendor}/${schema.self.name}/${schema.self.version}`

const productPurchaseSchema = require('./schemas/product-purchase-schema.json')
const productCreateSchema = require('./schemas/product-create-schema.json')
const userLoginSchema = require('./schemas/user-login-schema.json')
const updatePhoneSchema = require('./schemas/user-update-phone-schema.json')
const addRoleSchema = require('./schemas/user-add-role-schema.json')

const productPurchaseSchemaId = makeSchemaId(productPurchaseSchema)
const productCreateSchemaId = makeSchemaId(productCreateSchema)
const userLoginSchemaId = makeSchemaId(userLoginSchema)
const updatePhoneSchemaId = makeSchemaId(updatePhoneSchema)
const addRoleSchemaId = makeSchemaId(addRoleSchema)
ajv.addSchema(productPurchaseSchema, productPurchaseSchemaId)
ajv.addSchema(productCreateSchema, productCreateSchemaId)
ajv.addSchema(userLoginSchema, userLoginSchemaId)
ajv.addSchema(updatePhoneSchema, updatePhoneSchemaId)
ajv.addSchema(addRoleSchema, addRoleSchemaId)
const schema = [
  {
    methodName: 'product-purchase',
    schemaId: productPurchaseSchemaId, // NB after addSchema can refer to schema by schemaId
  },
  {
    methodName: 'product-create',
    schemaId: productCreateSchemaId,
  },
  {
    methodName: 'user-login',
    schemaId: userLoginSchemaId,
  },
  {
    methodName: 'user-update-phone',
    schemaId: updatePhoneSchemaId,
  },
  {
    methodName: 'user-add-role',
    schemaId: addRoleSchemaId,
  },
]

const constants = {
  INVALID_REQUEST: 'Invalid Request',
  INTEGRATION_ERROR: 'Kinesis Integration Error',
  API_NAME: 'Retail Stream Event Writer',
}

const impl = {
  response: (statusCode, body) => ({
    statusCode,
    headers: {
      'Access-Control-Allow-Origin': '*', // Required for CORS support to work
      'Access-Control-Allow-Credentials': true, // Required for cookies, authorization headers with HTTPS
    },
    body,
  }),

  clientError: (errors, event) => impl.response(400,
    `${constants.API_NAME} ${constants.INVALID_REQUEST} could not validate request to any known event schema. Errors by scheme: '${JSON.stringify(errors)}'.  Found in event: '${JSON.stringify(event)}'`),

  kinesisError: (methodNames, err) => {
    console.log(err)
    return impl.response(500, `${constants.API_NAME} - ${constants.INTEGRATION_ERROR} trying to write an event for '${JSON.stringify(methodNames)}'`)
  },

  success: items => impl.response(200, JSON.stringify(items)),

  checkKnownSchema: (eventData) => {
    const results = {
      errors: [],
      matches: [], // TODO does ajv actually check the content of the schema field vs the eventData schema field or just that it is of the format url?  if latter, then could have multiple matches.
    }

    schema.forEach((scheme) => {
      if (!ajv.validate(scheme.schemaId, eventData)) { // bad request
        results.errors.push({
          methodName: scheme.methodName,
          schemaId: scheme.schemaId,
          message: ajv.errorsText(),
        })
      } else {
        results.matches.push(scheme.methodName)
      }
    })

    return results
  },

  validateAndWriteKinesisEventFromApiEndpoint(event, callback) {
    console.log(JSON.stringify(event))
    const eventData = JSON.parse(event.body)
    console.log(eventData)
    const origin = eventData.origin
    console.log(origin)
    delete eventData.origin

    const validation = impl.checkKnownSchema(eventData)

    if (validation.matches.length === 0) { // bad request with no matching schema
      console.log(validation.errors)
      callback(null, impl.clientError(validation.errors, event))
    } else {
      const kinesis = new aws.Kinesis()
      const newEvent = {
        Data: JSON.stringify({
          schema: 'com.nordstrom/retail-stream-ingress/1-0-0',
          timeOrigin: new Date().toISOString(),
          data: eventData,
          origin,
        }),
        PartitionKey: eventData.id, // TODO if some schema use id field something other than the partition key, the schema need to have a keyName field and here code should be eventData[eventData.keyName]
        StreamName: process.env.STREAM_NAME,
      }

      // TODO what if it matches more than one scheme?  I guess the stream doesn't care, as long as it's valid for something, it's worth putting on.  Consumers can handle it themselves.
      kinesis.putRecord(newEvent, (err, data) => {
        if (err) {
          callback(null, impl.kinesisError(validation.matches, err))
        } else if (data) {
          callback(null, impl.success(`${validation.matches}: ${JSON.stringify(data)}`))
        }
      })
    }
  },
}

const api = {
  /**
   * Send the retail event to the retail stream.  Example events:
   *
   * product-purchase:
   {
     "schema": "com.nordstrom/product/purchase/1-0-0",
     "id": "4579874"
   }
   *
   * product-create:
   {
     "schema": "com.nordstrom/product/create/1-0-0",
     "id": "4579874",
     "brand": "POLO RALPH LAUREN",
     "name": "Polo Ralph Lauren 3-Pack Socks",
     "description": "PAGE:/s/polo-ralph-lauren-3-pack-socks/4579874",
     "category": "Socks for Men"
   }
   *
   * user-login:
   {
     "schema": "com.nordstrom/user-info/create/1-0-0",
     "id": "amzn1.account.AHMNGKVGNQYJUV7BZZZMFH3HP3KQ",
     "name": "Greg Smith"
   }
   *
   * update-phone:
   {
     "schema": "com.nordstrom/user-info/create/1-0-0",
     "id": "amzn1.account.AHMNGKVGNQYJUV7BZZZMFH3HP3KQ",
     "phone": "4255552603"
   }
   *
   * @param event The API Gateway lambda invocation event describing the event to be written to the retail stream.
   * @param context AWS runtime related information, e.g. log group id, timeout, request id, etc.
   * @param callback The callback to inform of completion: (error, result).
   */
  eventWriter: (event, context, callback) => {
    impl.validateAndWriteKinesisEventFromApiEndpoint(event, callback)
  },
}

module.exports = {
  eventWriter: api.eventWriter,
}
