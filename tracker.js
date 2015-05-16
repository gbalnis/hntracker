var args = process.argv.slice(2);
var logger = require('winston');
logger.level = (args.length ? args[0] : 'error');	// priority: debug, info, warn, error
logger.exitOnError = false;
logger.debug('Logger set up to ' + logger.level);

var hn = require('hashnest');
logger.debug('Hasnhnest client set up');

var fs = require('fs');
var dirRaw = 'raw';
var dirIntra = 'intraday';

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

fs.exists(dirIntra, function(exists) {
	if(!exists)
		fs.mkdir(dirIntra, function(err) {
			logger.debug('Directory ' + dirIntra + ' created.');
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
	var intraBuffer = '';
	var t = orders[0].created_at.getTime();
	var day = thisDay(t);

	while((t < until.getTime()) && orders.length) {
		// determine the day of the sample
		var nextMinute = thisMinute(t) + 1000 * 60;	// next minute, zero sec/millis
		var d = new Date(t);
		var ohlc = {
			ticker : market,
			per : 'I',
			date : formatDay(d),
			time : formatTime(d),
			open : 0,
			high : 0,
			low : -1,
			close : 0,
			vol : 0
		}
		
		while((t < nextMinute) && orders.length) {
			logger.debug('Buffering %d,%d,%d,%s', orders[0].ppc, orders[0].amount, orders[0].total_price, orders[0].created_at.toISOString());

			rawBuffer += (orders[0].ppc + ',' + orders[0].amount + ',' + orders[0].total_price + ',' + orders[0].created_at.toISOString() + '\n');

			ppc = Number(orders[0].ppc);
			amount = Number(orders[0].amount);
			if(ohlc.open == 0)
				ohlc.open = ppc;
			if(ohlc.high < ppc)
				ohlc.high = ppc;
			if((ohlc.low < 0) || (ohlc.low > ppc))
				ohlc.low = ppc;
			ohlc.close = ppc;
			ohlc.vol += amount;
			
			orders.shift();
			if(orders.length)
				t = orders[0].created_at.getTime();
		}
		
		// bump up the values so that they have max 2 decimal points
		ohlc.open *= 1E6;
		ohlc.high *= 1E6;
		ohlc.low *= 1E6;
		ohlc.close *= 1E6;
		intraBuffer += (ohlc.ticker + ',' + ohlc.per + ',' + ohlc.date + ',' + ohlc.time + ',' +
			ohlc.open.toFixed(2) + ',' + ohlc.high.toFixed(2) + ',' + ohlc.low.toFixed(2) + ',' + 
			ohlc.close.toFixed(2) + ',' + ohlc.vol.toFixed(0) + '\n');
			
		// buffers contain all entries from the currently processed minute
		// if day changes, flush the buffer
		if(orders.length && (day != thisDay(t))) {
			logger.debug('Writing buffers out on a day change');
			writeRawData(day, rawBuffer);
			rawBuffer = '';
			writeIntradayData(day, intraBuffer);
			intraBuffer = '';
			day = thisDay(t);
		}
	}
	
	writeRawData(day, rawBuffer);
	writeIntradayData(day, intraBuffer);
	
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

function writeIntradayData(d, buffer) {

	if(!buffer.length) {
		logger.debug('writeIntradayData called with empty buffer, nothing to write.');
		return;
	}
	
	var day = formatDay(new Date(d));
	logger.debug('%s:', day);
	logger.debug(buffer);
	
	var options = { encoding : 'utf8', flags : 'a' }
	var fileWriteStream = fs.createWriteStream('./' + dirIntra + '/' + day + '.txt', options);
	fileWriteStream.on("close", function(){
		logger.debug('File ./' + dirIntra + '/' + day + '.txt closed.');
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

function formatTime(t) {
	return addZero(t.getUTCHours()) + ':' + addZero(t.getUTCMinutes());
}

function addZero(i) {
	if(i < 10)
		i = '0' + i;
	return i;
}
