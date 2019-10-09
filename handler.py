import json

import boto3

CHATCONNECTION_TABLE = 'chatIdTable'

dynamo = boto3.client('dynamodb')

successfullResponse = {
  'statusCode': 200,
  'body': 'everything is alright'
}


def connectionHandler(event, context):
  # print(f'## connectionHandler event={json.dumps(event)}')
  if event['requestContext']['eventType'] == 'CONNECT':
      #handle connection
      try:
          cid = event['requestContext']['connectionId']
          print(f'# addConnetion cid={cid}')
          addConnection(cid)
          return successfullResponse
      except Exception as err:
        print(f'ERROR failed to addConnection: {err}')
        # If I raise this will retry, desired?
        raise RuntimeError(err)
  elif event['requestContext']['eventType'] == 'DISCONNECT':
      ## handle disconnection
      try:
          deleteConnection(event['requestContext']['connectionId'])
          return successfullResponse
      except Exception as err:
          raise RuntimeError(f'ERROR failed to disconnect: {err}')


def defaultHandler(event, context):
  print('defaultHandler was called')
  print(event);
  return {'statusCode': 200, body: 'defaultHandler'}


def sendMessageHandler(event, context):
  try:
      sendMessageToAllConnected(event)
      return successfullResponse
  except Exception as err:
      print(f'ERROR sendMessageHandler: {err}')


def sendMessageToAllConnected(event):
  connectionData = getConnectionIds()
  for connectionId in collectionData['Items']:
      send(event, connectionId.connectionId)


def getConnectionIds():
    return dynamo.scan(TableName=CHATCONNECTION_TABLE,
                       ProjecttionExpression='connectionId')


def send(event, connectionId) :
    print(f'## event={event}')
    body = json.loads(event['body']);
    postData = body['data'];
    # endpoint = event['requestContext']['domainName'] + '/' + event['requestContext']['stage']
    # AWS.ApiGatewayManagementApi(endpoint=endpoint)
    apigwManagementApi = boto3.client('apigatewaymanagementapi')
    return apigwManagementApi.postToConnection(ConnectionId=connectionId,
                                               Data=postData)


def addConnection(connectionId):
  return dynamo.put_item(TableName=CHATCONNECTION_TABLE,
                         Item={'connectionId': {'S': connectionId}})


def deleteConnection(connectionId):
    return dynamo.delete_item(TableName=CHATCONNECTION_TABLE,
                              Key={'connectionId': connectionId})
