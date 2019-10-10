import json
import os

import boto3

CHATCONNECTION_TABLE = 'chatIdTable'

dynamo = boto3.resource('dynamodb').Table(CHATCONNECTION_TABLE)

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
    apigapi = boto3.client('apigatewaymanagementapi', endpoint_url=endpoint)
    ret = apigapi.post_to_connection(ConnectionId=cid,
                                     Data=f'UNMATCHED ROUTE body={event["body"]}')
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
    """Pull the 'route' off the body and send it out to everyone."""
    print('# sendMessageToAllConnected...')
    body = event['body']        # stringy json '{"route": "sendMessage", "data": "stuff"}'
    print(f'# sendMessageToAllConnected body={body} type={type(body)}')
    try:
        body = json.loads(body)
        body = body['data']
    except Exception as err:
        print(f'# sendMessageToAllConnected ERR could not unpack str to json body={body}')
    items = getConnectionIds()['Items']
    print(f'# sendMessageToAllConnected ids={items}')
    for item in items:
        cid = item['connectionId']
        try:                    # we may have stale connections
            print(f'# sendMessageToAllConnected id={cid}')
            send(event, cid, body)
            print(f'# sendMessageToAllConnected id={cid} ok')
        except Exception as e:
            print(f'# sendMessageToAllConnected id={cid} failed, ignoring')


def getConnectionIds():
    print(f'getConnectionIds...')
    return dynamo.scan(ProjectionExpression='connectionId')


def send(event, connectionId, body) :
    # print(f'## send event={json.dumps(event)}')
    if not isinstance(body, str):
        body = json.dumps(body)
    #print(f'## send body=={body}')
    endpoint = f'https://{event["requestContext"]["domainName"]}/{event["requestContext"]["stage"]}'
    apigapi = boto3.client('apigatewaymanagementapi', endpoint_url=endpoint)
    try:
        ret = apigapi.post_to_connection(ConnectionId=connectionId, Data=body)
        print(f'#### send ret={ret}')
    except Exception as err:
        print(f'### send ERROR err={err}')


def addConnection(connectionId):
    print(f'addConnection id={connectionId}')
    return dynamo.put_item(Item={'connectionId': connectionId})


def deleteConnection(connectionId):
    print(f'deleteConnection id={connectionId}')
    return dynamo.delete_item(Key={'connectionId': connectionId})
