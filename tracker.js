/*
	TODO

	* cleanup after shutdown http://stackoverflow.com/questions/26163800/node-js-pm2-on-exit
	* add winston logger
*/

var hn = require("hashnest");
var market = 'ANTS5/BTC';
var marketId;
var orders = [];

hn.getCurrencyMarkets(function(data) {
		// console.log("Markets:");
		// console.dir(data);
		var m = -1;
		var i = 0;
		while(m < 0 && i < data.length) {
			if(data[i].name == market)
				m = data[i].id;
		}
		if(m >= 0) {
			marketId = m;
			setInterval(pullMarketOrders, 60000);
		}
	});

function pullMarketOrders() {
	hn.getMarketOrderHistory(marketId, 'purchase', function(data) {
		// console.log('ANTS5/BTC Orders:');
		// console.dir(data);
		stackOrders(data);
		processMarketOrders(orders);
	});
}

function stackOrders(lot) {
	while(orders.length && lot.length && compareOrders(orders[orders.length - 1], lot[lot.length - 1]))
		lot.pop();

	for(var i = lot.length - 1; i >= 0; i--)
		orders.push(lot[i]);
}

function compareOrders(o1, o2) {
	return ((o1.created_at > o2.created_at) || (o1.created_at == o2.created_at && o1.total_price == o2.total_price && o1.amount == o2.amount && o1.ppc == o2.ppc));
}
	
function processMarketOrders(orders) {
	if(orders.length <= 1)
		return;
	
	var order = orders.shift();
	processMarketOrder(order, function() {
		processMarketOrders(orders);
	});
}

function processMarketOrder(order, callback) {
	console.log('%d,%d,%d,%s', order.ppc, order.amount, order.total_price, order.created_at);
	if((orders.length - 1) > 0)
		process.nextTick(function() {
			callback();
		});
}
