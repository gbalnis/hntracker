var args = process.argv.slice(2);
var logger = require('winston');
logger.level = (args.length ? args[0] : 'error');
logger.exitOnError = false;
logger.debug('Logger set up');

var hn = require("hashnest");
logger.debug('Hasnhnest client set up');

var interval = 120000;

var market = 'ANTS5/BTC';
var marketId;
var orders = [];

hn.getCurrencyMarkets(function(data) {
		var m = -1;
		var i = 0;
		while(m < 0 && i < data.length) {
			if(data[i].name == market)
				m = data[i].id;
		}
		if(m >= 0) {
			marketId = m;
			setInterval(pullOrders, interval);
			setInterval(processOrders, interval * 2);
		}
	});

process.on('SIGTERM', function() {
	logger.debug('SIGTERM received: Flushing remaining orders');
    logOrders(new Date(0), new Date());
	process.exit();
});

process.on('SIGINT', function() {
	logger.debug('SIGINT received: Flushing remaining orders');
    logOrders(new Date(0), new Date());
	process.exit();
});

function pullOrders() {
	hn.getMarketOrderHistory(marketId, 'purchase', function(data) {
		stackOrders(data);
		processOrders(orders);
	});
}

function stackOrders(lot) {
	logger.debug('%d orders to stack', lot.length);
	for(var i in lot)
		lot[i].created_at = new Date(Date.parse(lot[i].created_at));
	
	while(orders.length && lot.length && (orders[orders.length - 1].created_at.getTime() >= lot[lot.length - 1].created_at.getTime()))
		lot.pop();

	logger.debug('%d new orders to stack', lot.length);

	for(var i = lot.length - 1; i >= 0; i--)
		orders.push(lot[i]);
}

// is o2 older or equal?
function compareOrders(o1, o2) {
	return o1.created_at.getTime() > o2.created_at.getTime();
}

function processOrders() {
	if(orders.length <= 1) {
		logger.debug('No orders available for processing at this time');
		return;
	}
	
	var stopAt = thisMinute(orders[orders.length - 1].created_at);
	
	logger.debug('Processing orders from %s until %s', orders[0].created_at.toISOString(), stopAt.toISOString());
	
	logOrders(orders[0].created_at, stopAt);

	logger.debug('Processing orders done, leaving %d items on stack', orders.length);
}

function logOrders(from, until) {
	logger.debug('Logging orders from %s until %s', from.toISOString(), until.toISOString());
	
	var t = from.getTime();
	while((t < until.getTime()) && orders.length) {
		logger.info('%d,%d,%d,%s', orders[0].ppc, orders[0].amount, orders[0].total_price, orders[0].created_at.toISOString());
		orders.shift();
		if(orders.length)
			t = orders[0].created_at.getTime();
	}
	
	logger.debug('Logging orders done');	
}

function thisMinute(time) {
	var minute = 1000 * 60;	// 1 minute in millis
	var millis = time.getTime();
	return new Date(millis - millis % minute);
}
