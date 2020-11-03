// Copyright 2018-2020Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

const AWS = require('aws-sdk');

const ddb = new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10', region: process.env.AWS_REGION });

const { TABLE_NAME } = process.env;

exports.handler = async event => {
  let openConnections;

  console.log('Incoming event', event);
  
  try {
    openConnections = await ddb.query({
      TableName: TABLE_NAME,
      IndexName : "openConnectionsIndex",
      KeyConditionExpression: "isOpen = :openValue",
      ExpressionAttributeValues: {
        ":openValue": "true"
      },
    }).promise();
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }
  
  const apigwManagementApi = new AWS.ApiGatewayManagementApi({
    apiVersion: '2018-11-29',
    endpoint: event.requestContext.domainName + '/' + event.requestContext.stage
  });

  const sender = event.requestContext.connectionId;
  const messageBody = JSON.parse(event.body);
  const postData = {
    data: messageBody.data,
    sender,
  };

  const postCalls = openConnections.Items.map(({ connectionId }) => {
    if (connectionId === sender) {
      return;
    }

    try {
      return apigwManagementApi.postToConnection({ ConnectionId: connectionId, Data: JSON.stringify(postData) }).promise();
    } catch (e) {
      if (e.statusCode === 410) {
        console.log(`Found stale connection, deleting ${connectionId}`);
        return ddb.delete({ TableName: TABLE_NAME, Key: { connectionId } }).promise();
      }
      console.log(`Unexpected error while sending the message`, e);
      throw e;
    }
  });
  
  try {
    await Promise.allSettled(postCalls);
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }

  return { statusCode: 200, body: 'Data sent.' };
};
