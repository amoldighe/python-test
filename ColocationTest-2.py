from asyncio import run
import asyncio
import time
import sys
import traceback
import hashlib
import ccxt
import ccxtpro
from ccxtpro.base.exchange import Exchange
import numpy as np


class DeribitPro(Exchange, ccxt.async_support.deribit):
    def describe(self):
        return self.deep_extend(super(DeribitPro, self).describe(), {
            'has': {
                'ws': True,
                'watchOrderBook': True,
            },
            'urls': {
                'api': 'https://www.deribit.com',
                'ws': 'wss://www.deribit.com/ws/api/v2'
            },
            'options': {
            },
        })

    def handle_message(self, client, message):
        if 'result' in message:
            if 'token_type' in message['result']:
                self.handle_authentication(client,message)

        if 'params' in message:
            self.handle_order_book(client,message)

    def handle_deltas(self, bookside, deltas):
        for j in range(0, len(deltas)):
            delta = deltas[j]
            price = float(delta[1])
            amount = float(delta[2])
            bookside.store(price, amount)

    def handle_order_book(self, client, message):
        symbol = message['params']['data']['instrument_name']
        channel = message['params']['channel']

        if(message['params']['data']['type']=='snapshot') :
            self.orderbooks[symbol] = self.order_book({}, 100)

        orderbook = self.orderbooks[symbol]
        ask_delta  = message['params']['data']['asks']
        bid_delta = message['params']['data']['bids']

        self.handle_deltas(orderbook['asks'],ask_delta)
        self.handle_deltas(orderbook['bids'],bid_delta)

        orderbook['timestamp'] = message['params']['data']['timestamp']
        client.resolve(orderbook,channel)

    def handle_authentication(self,client,message):
        print("authentication message : " +str(message))

        future = self.safe_value(client.futures, 'authenticated')
        future.resolve(True)

    async def authenticate(self, params={}):
        url = self.urls['ws']
        nonce = self.milliseconds()
        payload = ''
        auth = str(nonce) + "\n" + str(nonce) + "\n" + payload
        signature = self.hmac(self.encode(auth), self.encode(self.secret), hashlib.sha256)
        msg = {
            "jsonrpc": "2.0",
            "method": 'public/auth',
            "params": {
                "grant_type": "client_signature",
                "client_id": self.apiKey,
                "timestamp": nonce,
                "signature": signature,
                "nonce": str(nonce),
                "data": payload
            }
        }

        return await self.watch(url, "authenticated", msg, "authenticated")

    async def subscribe(self, method, channel, message_hash):
        url = self.urls['ws']
        msg =  {
                 "jsonrpc": "2.0",
                 "method": method,
                 "params": {
                     "channels": [channel]
                 }
        }
        return await self.watch(url, message_hash, msg, channel)

    async def watch_order_book(self, symbol, limit=100, params={}):
        await self.load_markets()
        await self.authenticate()
        channel = "book.{symbolToUse}.raw".format(symbolToUse=symbol)
        orderbook = await self.subscribe("public/subscribe",  channel, channel)
        return orderbook.limit(limit)

ORDER_TEST_TYPE = "order_creation_test"
WEBSOCKET_TEST_TYPE = "websocket_test"

DERIBIT = "deribit"
BITFINEX = "bitfinex"
BINANCE = "binance"
COINBASEPRO = "coinbasepro"

SUPPORTED_EXCHANGES = [
    DERIBIT,
    BITFINEX,
    BINANCE,
#    COINBASEPRO
]

exchange_secrets = {
    DERIBIT: {
        "apiKey": "ytOE2A3o",
        "secret": "edShXB0xYoKYVaE2cDB1wlF1crweJLanZhsfZXJVIf4"
    },
    BITFINEX: {
        "apiKey": "nwKxL9Cyy6bNNM7h6i8GFI8IjEnqtJdTv77THSoSof7",
        "secret": "xK7tpQnhMTo7AYa4q1zcQ8aF2fQKlaNYRjDKu9rohzo",
    },
    BINANCE: {
        "apiKey": "2zowyU44frikR8xWgYolhDQD0LNKhcCeK04EbuTxTRHhZ13I1RG9mMrbMLMawwNJ",
        "secret": "jCxBzuryr3em7puWlMlUvyogilWhneSlQ8OYwAWH49G9iPec0wcU2ynrDQQr3JWs"
    },
    COINBASEPRO: {
        "apiKey": "c573d68abb76133ebeec97bd36cfaf67",
        "secret": "xaqJZOwfAaTpWO6q3IFyM3IiXSpb0szSy6enEASZT01Prx4nZ6+2CcG8vhP1BiogxUBv4f5wKTOvUjb9KG72fA==",
        "password": "4tnf74l9qv",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 OPR/45.0.2552.882"
        }
    }
}


def set_options_on_binance(binance, type):
    binance.options['defaultType'] = type
    return binance


exchange_instance_map = {
    "sync" : {
        "future": {
            DERIBIT: ccxt.deribit(exchange_secrets[DERIBIT]),
            BITFINEX: ccxt.bitfinex(exchange_secrets[BITFINEX]),
            BINANCE: set_options_on_binance(ccxt.binance(exchange_secrets[BINANCE]),
                                                      type="future")
        },
        "spot": {
            BITFINEX: ccxt.bitfinex(exchange_secrets[BITFINEX]),
            COINBASEPRO: ccxt.coinbasepro(exchange_secrets[COINBASEPRO]),
            BINANCE: set_options_on_binance(ccxt.binance(exchange_secrets[BINANCE]),
                                                      type="spot")
        }
    },
    "async" : {
        "future": {
            DERIBIT: ccxt.async_support.deribit(exchange_secrets[DERIBIT]),
            BITFINEX: ccxt.async_support.bitfinex(exchange_secrets[BITFINEX]),
            BINANCE: set_options_on_binance(ccxt.async_support.binance(exchange_secrets[BINANCE]),
                                                      type="future")
        },
        "spot": {
            BITFINEX: ccxt.async_support.bitfinex(exchange_secrets[BITFINEX]),
            COINBASEPRO: ccxt.async_support.coinbasepro(exchange_secrets[COINBASEPRO]),
            BINANCE: set_options_on_binance(ccxt.async_support.binance(exchange_secrets[BINANCE]),
                                                      type="spot")
        }
    },
    "websocket" : {
        "future": {
            BITFINEX: ccxtpro.bitfinex(exchange_secrets[BITFINEX]),
            BINANCE: set_options_on_binance(ccxtpro.binance(exchange_secrets[BINANCE]),
                                                      type="future"),
            DERIBIT: DeribitPro(exchange_secrets[DERIBIT])
        },
        "spot": {
            COINBASEPRO: ccxtpro.coinbasepro(exchange_secrets[COINBASEPRO]),
            BITFINEX: ccxtpro.bitfinex(exchange_secrets[BITFINEX]),
            BINANCE: set_options_on_binance(ccxtpro.binance(exchange_secrets[BINANCE]),
                                                      type="spot")
        }
    }
}


def get_symbol_for_exchange_name(exchange_name):
    if exchange_name == BITFINEX:
        return 'BTCF0/USTF0'

    if exchange_name == BINANCE:
        return 'BTC/USDT'

    if exchange_name == DERIBIT:
        return 'BTC-PERPETUAL'

    if exchange_name == COINBASEPRO:
        return 'BTC-USDT'

    raise Exception('No symbol found for exchange: %s' % exchange_name)


def get_quantity_for_exchange(exchange_name):
    if exchange_name == BITFINEX:
        return 0.001

    if exchange_name == BINANCE:
        return 0.001

    if exchange_name == DERIBIT:
        return 10

    if exchange_name == COINBASEPRO:
        return 0.001

    raise Exception('No symbol found for exchange: %s' % exchange_name)


def get_params_for_exchange(exchange_name):
    if exchange_name == DERIBIT:
        return {}

    if exchange_name == COINBASEPRO:
        return {}

    if exchange_name == BITFINEX:
        return { 'lev': '1', 'type': 'limit' }

    if exchange_name == BINANCE:
        return { }

    raise Exception("Exchange not supported: " + exchange_name)


def print_data_analysis(data_arr, message, unit='ms'):
    if len(data_arr) == 0:
        print('Insufficient data for analytics')
        return

    print('----- ----- ----- ----- ----- ----- ----- ----- ----- -----')
    print(message)

    print('Count: %s' % len(data_arr))
    print('Min (%s): %s' % (unit, min(data_arr)))
    print('Avg (%s): %s' % (unit, np.average(data_arr)))
    print('Max (%s): %s' % (unit, max(data_arr)))
    print()
#    print('p10 (%s): %s' % (unit, np.percentile(data_arr, 10)))
#    print('p25 (%s): %s' % (unit, np.percentile(data_arr, 25)))
#    print('p50 (%s): %s' % (unit, np.percentile(data_arr, 50)))
#    print('p75 (%s): %s' % (unit, np.percentile(data_arr, 75)))
    print('p90 (%s): %s' % (unit, np.percentile(data_arr, 90)))
    print('p95 (%s): %s' % (unit, np.percentile(data_arr, 95)))
    print('p99 (%s): %s' % (unit, np.percentile(data_arr, 99)))
    print('----- ----- ----- ----- ----- ----- ----- ----- ----- -----')


def cancel_order(exchange_name, exchange_instance, order_id, symbol):
    while True:
        try:
            exchange_instance.cancel_order(order_id, symbol=symbol)
            print('Cancelled order on exchange = %s, order id = %s' % (exchange_name, order_id))
            break
        except Exception as ex:
            print('An exception occured while canceling order on exchange: %s, %s' % (exchange_name, order_id))


def run_order_creation_test(exchange_name, n):
    print("Running order creation test for exchange: %s" % exchange_name)
    symbol = get_symbol_for_exchange_name(exchange_name)
    quantity = get_quantity_for_exchange(exchange_name)

    market_type = 'future' if exchange_name != COINBASEPRO else 'spot'
    exchange_instance = exchange_instance_map['sync'][market_type][exchange_name]

    total_rtt_time = 0
    order_creation_rtt_times = []

    print('Creating orders on exchange: %s' % exchange_name)
    created_order_ids = []
    for i in range(0, n):
        try:
            start_time = time.time() * 1000 # in milliseconds
            order = exchange_instance.create_order(symbol, 'limit', 'buy', quantity, price=10000, params=get_params_for_exchange(exchange_name))
            end_time = time.time() * 1000
            order_creation_rtt_times.append((end_time - start_time))
            total_rtt_time = total_rtt_time + (end_time - start_time)
            created_order_ids.append(order['id'])

            cancel_order(exchange_name, exchange_instance, order['id'], symbol)
        except Exception as ex:
            print('An error occured while placing order for exchange: %s' % exchange_name)
            traceback.print_exc()

    print_data_analysis(order_creation_rtt_times, 'Order creation analytics for exchange: %s, order count: %s ' % (exchange_name, n))
    return total_rtt_time / max(len(created_order_ids), 1)


def run_order_creation_tests(exchange_list, n):
    print("Running order creation tests...")
    for exchange_name in exchange_list:
        if exchange_name not in SUPPORTED_EXCHANGES:
            print('Exchange not supported: %s' % exchange_name)
            continue
        average_time = round(run_order_creation_test(exchange_name, n), 3)
#        print('Average order creation time (milliseconds) for exchange %s: %s' % (exchange_name, average_time))


async def run_websocket_test(exchange_name, n):
    print("Running websocket test for exchange: %s" % exchange_name)
    symbol = get_symbol_for_exchange_name(exchange_name)
    market_type = 'future' if exchange_name != COINBASEPRO else 'spot'
    exchange_instance = exchange_instance_map['websocket'][market_type][exchange_name]

    websocket_response_times = []
    total_rtt_time = 0
    num_calls = 0
    final_end_time = time.time() + n # in seconds

    while True:
        try:
            if time.time() > final_end_time:
                break
            start_time = time.time() * 1000 # in milliseconds
            await exchange_instance.watch_order_book(symbol)
            end_time = time.time() * 1000
            websocket_response_times.append((end_time - start_time))
            total_rtt_time = total_rtt_time + (end_time - start_time)
            num_calls = num_calls + 1
        except Exception as ex:
            print('An exception in getting websocket data: %s' % exchange_name)
            traceback.print_exc()

    await exchange_instance.close()

    print_data_analysis(websocket_response_times, 'Websocket analytics for exchange: %s, duration: %s seconds' % (exchange_name, n))
    return total_rtt_time / num_calls


async def run_websocket_tests(exchange_list, n):
    print("Running websocket tests...")
    supported_exchanges = list(filter(lambda exchange_name: exchange_name in SUPPORTED_EXCHANGES, exchange_list))
    await asyncio.wait([run_websocket_test(exchange_name, n) for exchange_name in supported_exchanges], return_when=asyncio.ALL_COMPLETED)


async def run_colocation_test(args):
    print('Running colocation test with args: %s' % args)

    test_type = args[0].lower()
    if test_type not in [ORDER_TEST_TYPE, WEBSOCKET_TEST_TYPE]:
        raise Exception('Test type not supported: %s' % test_type)

    if test_type == ORDER_TEST_TYPE:
        run_order_creation_tests(args[1].split(','), int(args[2]))
        return

    if test_type == WEBSOCKET_TEST_TYPE:
        await run_websocket_tests(args[1].split(','), int(args[2]))


if __name__ == '__main__':
    run(run_colocation_test(sys.argv[1:]))
    # python3 CleonBot/scripts/ColocationTest.py order_creation_test binance,deribit 1
    # python3 CleonBot/scripts/ColocationTest.py websocket_test binance 10
