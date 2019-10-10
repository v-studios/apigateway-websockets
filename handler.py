import json
import os

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
    """If we don't get a route match, it comes here; send back a warning."""
    print(f'### defaultHandler event={json.dumps(event)}')
    cid = event['requestContext']['connectionId']
    endpoint = f'https://{event["requestContext"]["domainName"]}/{event["requestContext"]["stage"]}'
    print(f'### defaultHandler endpoint={endpoint}')
    apigapi = boto3.client('apigatewaymanagementapi', endpoint_url=endpoint)
    ret = apigapi.post_to_connection(ConnectionId=cid, Data='UNMATCHED ROUTE')
    print(f'### defaultHandler ret={json.dumps(ret)}')
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
    connection_ids = connectionData["Items"]
    print(f'# sendMessageToAllConnected ids={connection_ids}')
    for connectionId in connection_ids:
        try:                    # we may have stale connections
            print(f'# sendMessageToAllConnected id={connectionId}')
            body = json.loads(event['body']);
            send(event, connectionId, body)
            print(f'# sendMessageToAllConnected id={connectionId} ok')
        except Exception as e:
            print(f'# sendMessageToAllConnected id={connectionId} failed, ignoring')


def getConnectionIds():
    print(f'getConnectionIds...')
    return dynamo.scan(TableName=CHATCONNECTION_TABLE,
                       ProjectionExpression='connectionId')


def send(event, connectionId, body) :
    print(f'## send event={json.dumps(event)}')
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
