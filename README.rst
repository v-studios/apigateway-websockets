========================
 API Gateway Websockets
========================

I saw Paul Chin Jr.'s highly-entertaining presentation at
ServerlessConf NYC 2019 and was inpired to learn more about
websockets. Since I'm a lot more comfortable with Python than Node, I
rewrote the Lambda handler in Python. I've tweaked the
``serverless.yml`` to configure the websocket route-finding, then
wrote a simple ``index.html`` to implement a chat UI.

There's no CSS or anything else, I'm trying to keep it simple so it's
easy to understand and build upon. I can see lots of applications for
quickly-updating status dashboards and the like.
