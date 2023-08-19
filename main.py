import datetime
import json
import websocket
import threading
import pandas as pd
import pandas_ta as pt


class BinanceWSConnector(websocket.WebSocketApp):
    def __init__(self, url, *args, **kwargs):
        self.buffer = []
        super().__init__(url, on_message=self.message_handler, on_error=self.error_handler)

        self.run_forever()

    @staticmethod
    def error_handler(ws, err):
        print("ERROR:", err)

    def message_handler(self, ws, data):
        dict_data = json.loads(data)['k']

        self.buffer.append(float(dict_data['c']))

        if dict_data['x']:
            print("BINANCE INFO ------------------")
            print("Close price:", dict_data['c'])
            df = pd.DataFrame(self.buffer, columns=['cost'])
            if len(df) > 14:
                print("RSI:", pt.rsi(df['cost'], length=14).iloc[-1])
                self.buffer.clear()
            print("-------------------------------")


class BitfinexWBConnector(websocket.WebSocketApp):
    def __init__(self, url, key, *args, **kwargs):
        self.buffer = []
        super().__init__(url, on_message=self.message_handler, on_error=self.error_handler)

        self.on_open = lambda self: self.send(
            '{ "event": "subscribe",  "channel": "candles",  "key": "%s" }' % key)

        self.run_forever()

    @staticmethod
    def error_handler(ws, err):
        print("ERROR:", err)

    def message_handler(self, ws, data):
        data = json.loads(data)
        if isinstance(data, list) and isinstance(data[1], list) and (not isinstance(data[1][0], list)):
            data = [float(elem) for elem in data[1]]
            data.append(datetime.datetime.now())
            if len(self.buffer) == 0 or data[0] >= self.buffer[-1][0]:
                if len(self.buffer) != 0 and data[0] > self.buffer[-1][0]:
                    df = pd.DataFrame(self.buffer,
                                      columns=['mts', 'open', 'close', 'high', 'low', 'volume', 'data_time'])
                    print("Bitfinex -------")
                    df.set_index('data_time', inplace=True)
                    df.sort_values(by='data_time')
                    vwap = pt.vwap(high=df['high'], low=df['low'], close=df['close'],
                                   volume=df['volume'])
                    print("Close price", self.buffer[-1][2])
                    print("VWAP:", vwap[-1])
                    print("----------------")
                    self.buffer.clear()
                self.buffer.append(data)


def run():
    print("Start")

    binance_url = "wss://stream.binance.com:443/ws/btcusdt@kline_5m"
    threading.Thread(target=BinanceWSConnector, args=(binance_url,)).start()

    bitfinex_url = "wss://api-pub.bitfinex.com/ws/2"
    bitfinex_key = "trade:1m:tBTCUSD"
    threading.Thread(target=BitfinexWBConnector, args=(bitfinex_url, bitfinex_key)).start()


if __name__ == '__main__':
    run()
