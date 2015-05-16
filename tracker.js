var args = process.argv.slice(2);
var logger = require('winston');
logger.level = (args.length ? args[0] : 'error');	// priority: debug, info, warn, error
logger.exitOnError = false;
logger.debug('Logger set up to ' + logger.level);

var hn = require('hashnest');
logger.debug('Hasnhnest client set up');

var fs = require('fs');
var dirRaw = 'raw';

var interval = 60000;

var market = 'ANTS5/BTC';
var marketId;
var orders = [];

fs.exists(dirRaw, function(exists) {
	if(!exists)
		fs.mkdir(dirRaw, function(err) {
			logger.debug('Directory ' + dirRaw + ' created.');
		});
	});
		
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
	logger.debug('Shutdown in 10 secs');
	setTimeout(function() {
		process.exit();
	}, 10000);
});

process.on('SIGINT', function() {
	logger.debug('SIGINT received: Flushing remaining orders');
    logOrders(new Date(0), new Date());
	logger.debug('Shutdown in 10 secs');
	setTimeout(function() {
		process.exit();
	}, 10000);
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
	
	var stopAt = new Date(thisMinute(orders[orders.length - 1].created_at.getTime()));
	
	logger.debug('Processing orders from %s until %s', orders[0].created_at.toISOString(), stopAt.toISOString());
	
	if(orders[0].created_at.getTime() < stopAt.getTime())
		logOrders(orders[0].created_at, stopAt);

	logger.debug('Processing orders done, leaving %d items on stack', orders.length);
}

function logOrders(from, until) {

	logger.debug('Logging orders from %s until %s', from.toISOString(), until.toISOString());

	if(!orders.length) {
		logger.debug('logOrders called with empty buffer, nothing to log.');
		return;
	}

	var rawBuffer = '';
	var t = orders[0].created_at.getTime();
	var day = thisDay(t);

	while((t < until.getTime()) && orders.length) {
		// determine the day of the sample
		var nextMinute = thisMinute(t) + 1000 * 60;	// next minute, zero sec/millis
		
		while((t < nextMinute) && orders.length) {
			logger.debug('Buffering %d,%d,%d,%s', orders[0].ppc, orders[0].amount, orders[0].total_price, orders[0].created_at.toISOString());
			rawBuffer += (orders[0].ppc + ',' + orders[0].amount + ',' + orders[0].total_price + ',' + orders[0].created_at.toISOString() + '\n');
			orders.shift();
			if(orders.length)
				t = orders[0].created_at.getTime();

		// buffer contains all entries from the currently processed minute
		// if day changes, flush the buffer
		if(orders.length && (day != thisDay(t))) {
			logger.debug('Writing buffer on a day change');
			writeRawData(day, rawBuffer);
			rawBuffer = '';
			day = thisDay(t);
			}
		}
	}
	
	writeRawData(day, rawBuffer);

	logger.debug('Logging orders done');	
}

function writeRawData(d, buffer) {

	if(!buffer.length) {
		logger.debug('writeRawData called with empty buffer, nothing to write.');
		return;
	}
	
	var day = formatDay(new Date(d));
	logger.debug('%s:', day);
	logger.debug(buffer);
	
	var options = { encoding : 'utf8', flags : 'a' }
	var fileWriteStream = fs.createWriteStream('./' + dirRaw + '/' + day + '.txt', options);
	fileWriteStream.on("close", function(){
		logger.debug('File ./' + dirRaw + '/' + day + '.txt closed.');
	});
	fileWriteStream.write(buffer);
	fileWriteStream.end();
}

// takes a time in millis and returns time in millis that is set at the beginning of the minute (xx:xx:00.000)
function thisMinute(millis) {
	var minute = 1000 * 60;	// 1 minute in millis
	return millis - millis % minute;
}

// takes a time in millis and returns time in millis that is set at the beginning of the day (00:00:00.000)
function thisDay(millis) {
	var day = 1000 * 60 * 60 * 24;	// 1 day in millis
	return millis - millis % day;
}

function formatDay(t) {
	return t.getUTCFullYear() + addZero(t.getUTCMonth() + 1) + addZero(t.getUTCDate());
}

function addZero(i) {
	if(i < 10)
		i = '0' + i;
	return i;
}
