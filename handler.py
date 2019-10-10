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
            cid = event['requestContext']['connectionId']
            print(f'## connectionHandler delete id={cid}')
            deleteConnection(cid)
            return successfullResponse
        except Exception as err:
            raise RuntimeError(f'ERROR failed to disconnect: {err}')


def defaultHandler(event, context):
    print(f'### defaultHandler event={json.dumps(event)}')
    body = event['body']
    print(f'### defaultHandler body={json.dumps(body)}')
    return successfullResponse


def sendMessageHandler(event, context):
    try:
        print('# sendMessageHandler sending to all...')
        sendMessageToAllConnected(event)
        print('# sendMessageHandler rturn success...')
        return successfullResponse
    except Exception as err:
        print(f'ERROR sendMessageHandler: {err}')


def sendMessageToAllConnected(event):
    print('# sendMessageToAllConnected...')
    connectionData = getConnectionIds()
    print(f'# sendMessageToAllConnected Items={connectionData["Items"]}')
    for connectionId in connectionData['Items']:
        try:                    # we may have stale connections
            print(f'# sendMessageToAllConnected id={connectionId}')
            send(event, connectionId.connectionId)
            print(f'# sendMessageToAllConnected id={connectionId} ok')
        except Exception as e:
            print(f'# sendMessageToAllConnected id={connectionId} failed, ignoring')


def getConnectionIds():
    print(f'getConnectionIds...')
    return dynamo.scan(TableName=CHATCONNECTION_TABLE,
                       ProjectionExpression='connectionId')


def send(event, connectionId) :
    print(f'## send event={json.dumps(event)}')
    body = json.loads(event['body']);
    print(f'## send body=={json.dumps(body, indent=2)}')
    postData = body['data'];
    print(f'## send postData=={json.dumps(postData, indent=2)}')
    endpoint = f'https://{event["requestContext"]["domainName"]}/{event["requestContext"]["stage"]}'
    apigwManagementApi = boto3.client('apigatewaymanagementapi', endpoint=endpoint)
    print(f'## send apigma=={apigwManagementApi}')
    return apigwManagementApi.post_to_connection(ConnectionId=connectionId,
                                                 Data=postData)


def addConnection(connectionId):
    print(f'addConnection id={connectionId}')
    return dynamo.put_item(TableName=CHATCONNECTION_TABLE,
                           Item={'connectionId': {'S': connectionId}})


def deleteConnection(connectionId):
    print(f'deleteConnection id={connectionId}')
    return dynamo.delete_item(TableName=CHATCONNECTION_TABLE,
                              Key={'connectionId': {'S': connectionId}})
