// small helper function for selecting element by id
let id = id => document.getElementById(id);

//Establish the WebSocket connection and set up event handlers
//let ws = new WebSocket("ws://" + location.hostname + ":" + location.port + "/chat");
let ws = new WebSocket("wss://u6k9ujsvz7.execute-api.us-east-1.amazonaws.com/dev");

ws.onopen = () => alert("WebSocket opened");
ws.onclose = () => alert("WebSocket connection closed");
ws.onmessage = msg => updateChat(msg);

// Add event listeners to button and input field
id("send").addEventListener("click", () => sendAndClear(id("message").value));
id("message").addEventListener("keypress", function (e) {
    if (e.keyCode === 13) { // Send message if enter is pressed in input field
        sendAndClear(e.target.value);
    }
});

function sendAndClear(message) {
    if (message !== "") {
        ws.send(JSON.stringify({'route': 'sendMessage', 'data': message}));
        id("message").value = "";
    }
}

function updateChat(messageEvent) { // Update chat-panel and list of connected users
    console.log(messageEvent)
    let data = messageEvent.data // don't I have to JSON.parse() it? only if nonstring?
    id("chat").insertAdjacentHTML("afterbegin", data);
}
