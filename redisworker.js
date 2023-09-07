var fs = require("fs");
var path = require("path");
var redis = require("redis");
var { get_conf, get_redis_subscriber } = require("./../frappe/node_utils");

// var conf = get_conf();

// var subscriber = redis.createClient(
//   conf.redis_socketio || conf.redis_async_broker_port
// );
// alternatively one can try:
var subscriber = get_redis_subscriber();

console.log("Redis Subscriber started");

console.log(subscriber);
process.on("SIGINT", () => {
	client.unsubscribe();
	client.quit();
	console.log("Subscriber has been terminated.");
});

while (true) {
	subscriber.on("channel", function (channel, message) {
		console.log("Here####", message, channel);
		message = JSON.parse(message);
		if (message.event == "custom_connector") {
			console.log("Got the Message:", message);
		}
	});
}
