import datetime
import hashlib
import json
import logging
import threading
import time
import urllib
from datetime import datetime as dt
from time import sleep

import requests
import websocket

logger = logging.getLogger(__name__)


class position:
    prd: str
    exch: str
    instname: str
    symname: str
    exd: int
    optt: str
    strprc: float
    buyqty: int
    sellqty: int
    netqty: int

    def encode(self):
        return self.__dict__


class ProductType:
    Delivery = 'C'
    Intraday = 'I'
    Normal = 'M'
    CF = 'M'


class FeedType:
    TOUCHLINE = 1
    SNAPQUOTE = 2


class PriceType:
    Market = 'MKT'
    Limit = 'LMT'
    StopLossLimit = 'SL-LMT'
    StopLossMarket = 'SL-MKT'


class BuyorSell:
    Buy = 'B'
    Sell = 'S'


def reportmsg(msg):
    # print(msg)
    logger.debug(msg)


def reporterror(msg):
    # print(msg)
    logger.error(msg)


def reportinfo(msg):
    # print(msg)
    logger.info(msg)


class NorenApi:
    __service_config = {
        'host': 'http://wsapihost/',
        'routes': {
            'authorize': '/QuickAuth',
            'logout': '/Logout',
            'forgot_password': '/ForgotPassword',
            'change_password': '/Changepwd',
            'watchlist_names': '/MWList',
            'watchlist': '/MarketWatch',
            'watchlist_add': '/AddMultiScripsToMW',
            'watchlist_delete': '/DeleteMultiMWScrips',
            'placeorder': '/PlaceOrder',
            'modifyorder': '/ModifyOrder',
            'cancelorder': '/CancelOrder',
            'exitorder': '/ExitSNOOrder',
            'product_conversion': '/ProductConversion',
            'orderbook': '/OrderBook',
            'tradebook': '/TradeBook',
            'singleorderhistory': '/SingleOrdHist',
            'searchscrip': '/SearchScrip',
            'TPSeries': '/TPSeries',
            'optionchain': '/GetOptionChain',
            'holdings': '/Holdings',
            'limits': '/Limits',
            'positions': '/PositionBook',
            'scripinfo': '/GetSecurityInfo',
            'getquotes': '/GetQuotes',
            'span_calculator': '/SpanCalc',
            'option_greek': '/GetOptionGreek',
            'get_daily_price_series': '/EODChartData',
        },
        'websocket_endpoint': 'wss://wsendpoint/',
        # 'eoddata_endpoint' : 'http://eodhost/'
    }

    def __init__(self, host, websocket):
        self.__service_config['host'] = host
        self.__service_config['websocket_endpoint'] = websocket
        # self.__service_config['eoddata_endpoint'] = eodhost

        self.__websocket = None
        self.__websocket_connected = False
        self.__ws_mutex = threading.Lock()
        self.__on_error = None
        self.__on_disconnect = None
        self.__on_open = None
        self.__subscribe_callback = None
        self.__order_update_callback = None
        self.__subscribers = {}
        self.__market_status_messages = []
        self.__exchange_messages = []

        self.session = requests.Session()

    def __ws_run_forever(self):

        while self.__stop_event.is_set() == False:
            try:
                self.__websocket.run_forever(ping_interval=3, ping_payload='{"t":"h"}')
            except Exception as e:
                logger.warning(f"websocket run forever ended in exception, {e}")

            sleep(0.1)  # Sleep for 100ms between reconnection.

    def __ws_send(self, *args, **kwargs):
        while self.__websocket_connected == False:
            sleep(0.05)  # sleep for 50ms if websocket is not connected, wait for reconnection
        with self.__ws_mutex:
            ret = self.__websocket.send(*args, **kwargs)
        return ret

    def __on_close_callback(self, wsapp, close_status_code, close_msg):
        reportmsg(close_status_code)
        reportmsg(wsapp)

        self.__websocket_connected = False
        if self.__on_disconnect:
            self.__on_disconnect()

    def __on_open_callback(self, ws=None):
        self.__websocket_connected = True

        # prepare the data
        values = {
            "t": "c",
            "uid": self.__username,
            "actid": self.__username,
            "susertoken": self.__susertoken,
            "source": 'API',
        }
        payload = json.dumps(values)

        reportmsg(payload)
        self.__ws_send(payload)

        # self.__resubscribe()

    def __on_error_callback(self, ws=None, error=None):
        if (
                type(
                    ws) is not websocket.WebSocketApp):  # This workaround is to solve the websocket_client's compatiblity issue of older versions. ie.0.40.0 which is used in upstox. Now this will work in both 0.40.0 & newer version of websocket_client
            error = ws
        if self.__on_error:
            self.__on_error(error)

    def __on_data_callback(self, ws=None, message=None, data_type=None, continue_flag=None):
        # print(ws)
        # print(message)
        # print(data_type)
        # print(continue_flag)

        res = json.loads(message)

        if (self.__subscribe_callback is not None):
            if res['t'] in ['tk', 'tf']:
                self.__subscribe_callback(res)
                return
            if res['t'] in ['dk', 'df']:
                self.__subscribe_callback(res)
                return

        if res['t'] == 'ck' and res['s'] != 'OK' and self.__on_error is not None:
            self.__on_error(res)
            return

        if res['t'] == 'om' and self.__order_update_callback is not None:
            self.__order_update_callback(res)
            return

        if res['t'] == 'ck' and res['s'] == 'OK' and self.__on_open:
            self.__on_open()
            return

    def start_websocket(self, subscribe_callback=None,
                        order_update_callback=None,
                        socket_open_callback=None,
                        socket_close_callback=None,
                        socket_error_callback=None):
        """ Start a websocket connection for getting live data """
        self.__on_open = socket_open_callback
        self.__on_disconnect = socket_close_callback
        self.__on_error = socket_error_callback
        self.__subscribe_callback = subscribe_callback
        self.__order_update_callback = order_update_callback
        self.__stop_event = threading.Event()
        url = self.__service_config['websocket_endpoint'].format(access_token=self.__susertoken)
        reportmsg(f'connecting to {url}')

        self.__websocket = websocket.WebSocketApp(url,
                                                  on_data=self.__on_data_callback,
                                                  on_error=self.__on_error_callback,
                                                  on_close=self.__on_close_callback,
                                                  on_open=self.__on_open_callback)
        # th = threading.Thread(target=self.__send_heartbeat)
        # th.daemon = True
        # th.start()
        # if run_in_background is True:
        self.__ws_thread = threading.Thread(target=self.__ws_run_forever)
        self.__ws_thread.daemon = True
        self.__ws_thread.start()

    def close_websocket(self):
        if self.__websocket_connected == False:
            return
        self.__stop_event.set()
        self.__websocket_connected = False
        self.__websocket.close()
        self.__ws_thread.join()

    def login(self, userid, password, twoFA, vendor_code, api_secret, imei):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['authorize']}"
        reportmsg(url)

        # Convert to SHA 256 for password and app key
        pwd = hashlib.sha256(password.encode('utf-8')).hexdigest()
        u_app_key = '{0}|{1}'.format(userid, api_secret)
        app_key = hashlib.sha256(u_app_key.encode('utf-8')).hexdigest()
        # prepare the data
        values = {
            "source": "API",
            "apkversion": "1.0.0",
            "uid": userid,
            "pwd": pwd,
            "factor2": twoFA,
            "vc": vendor_code,
            "appkey": app_key,
            "imei": imei,
        }
        res_dict = self.send_payload(url, values, is_authorized=False)

        if res_dict:
            self.__username = userid
            self.__accountid = userid
            self.__password = password
            self.__susertoken = res_dict['susertoken']

        return res_dict

    def set_session(self, userid, password, usertoken):

        self.__username = userid
        self.__accountid = userid
        self.__password = password
        self.__susertoken = usertoken

        reportmsg(f'{userid} session set to : {self.__susertoken}')

        return True

    def forgot_password(self, userid, pan, dob):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['forgot_password']}"
        reportmsg(url)

        # prepare the data
        values = {"source": "API", "uid": userid, "pan": pan, "dob": dob}
        return self.send_payload(url, values, is_authorized=False)

    def logout(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['logout']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username}
        payload = f'jData={json.dumps(values)}' + f'&jKey={self.__susertoken}'

        res_dict = self.send_payload(url, values)

        if res_dict:
            self.__username = None
            self.__accountid = None
            self.__password = None
            self.__susertoken = None

        return res_dict

    def subscribe(self, instrument, feed_type=FeedType.TOUCHLINE):
        values = {}

        if feed_type == FeedType.TOUCHLINE:
            values['t'] = 't'
        elif feed_type == FeedType.SNAPQUOTE:
            values['t'] = 'd'
        else:
            values['t'] = str(feed_type)

        values['k'] = '#'.join(instrument) if type(instrument) == list else instrument
        data = json.dumps(values)

        # print(data)
        self.__ws_send(data)

    def unsubscribe(self, instrument, feed_type=FeedType.TOUCHLINE):
        values = {}

        if (feed_type == FeedType.TOUCHLINE):
            values['t'] = 'u'
        elif (feed_type == FeedType.SNAPQUOTE):
            values['t'] = 'ud'

        values['k'] = '#'.join(instrument) if type(instrument) == list else instrument
        data = json.dumps(values)

        # print(data)
        self.__ws_send(data)

    def subscribe_orders(self):
        values = {'t': 'o', 'actid': self.__accountid}
        data = json.dumps(values)

        reportmsg(data)
        self.__ws_send(data)

    def get_watch_list_names(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['watchlist_names']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username}
        return self.send_payload(url, values)

    def get_watch_list(self, wlname):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['watchlist']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "wlname": wlname}
        return self.send_payload(url, values)

    def send_payload(self, url, values, expected_return_type=None, is_authorized=True, headers=None):
        payload = f'jData={json.dumps(values)}'
        if is_authorized:
            payload += f'&jKey={self.__susertoken}'

        reportmsg(payload)
        res = self.session.post(url, data=payload, headers=headers)
        reportmsg(res.text)
        res = json.loads(res.text)

        success = None
        if (
                expected_return_type == 'list'
                and type(res) == list
                or expected_return_type != 'list'
                and res['stat'] == 'Ok'
        ):
            success = True
        return res if success else None

    def add_watch_list_scrip(self, wlname, instrument):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['watchlist_add']}"
        reportmsg(url)
        # prepare the data
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "wlname": wlname,
            'scrips': '#'.join(instrument)
            if type(instrument) == list
            else instrument,
        }
        return self.send_payload(url, values)

    def delete_watch_list_scrip(self, wlname, instrument):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['watchlist_delete']}"
        reportmsg(url)
        # prepare the data
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "wlname": wlname,
            'scrips': '#'.join(instrument)
            if type(instrument) == list
            else instrument,
        }
        return self.send_payload(url, values)

    def place_order(self, buy_or_sell, product_type,
                    exchange, tradingsymbol, quantity, discloseqty,
                    price_type, price=0.0, trigger_price=None,
                    retention='DAY', amo='NO', remarks=None, bookloss_price=0.0, bookprofit_price=0.0, trail_price=0.0):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['placeorder']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "actid": self.__accountid, "trantype": buy_or_sell,
                  "prd": product_type, "exch": exchange, "tsym": urllib.parse.quote_plus(tradingsymbol),
                  "qty": str(quantity), "dscqty": str(discloseqty), "prctyp": price_type, "prc": str(price),
                  "trgprc": str(trigger_price), "ret": retention, "remarks": remarks, "amo": amo}

        # if cover order or high leverage order
        if product_type == 'H':
            values["blprc"] = str(bookloss_price)
            # trailing price
            if trail_price != 0.0:
                values["trailprc"] = str(trail_price)

        # bracket order
        if product_type == 'B':
            values["blprc"] = str(bookloss_price)
            values["bpprc"] = str(bookprofit_price)
            # trailing price
            if trail_price != 0.0:
                values["trailprc"] = str(trail_price)

        return self.send_payload(url, values)

    def modify_order(self, orderno, exchange, tradingsymbol, newquantity,
                     newprice_type, newprice=0.0, newtrigger_price=None, bookloss_price=0.0, bookprofit_price=0.0,
                     trail_price=0.0):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['modifyorder']}"
        print(url)

        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "actid": self.__accountid, "norenordno": str(orderno),
                  "exch": exchange, "tsym": urllib.parse.quote_plus(tradingsymbol), "qty": str(newquantity),
                  "prctyp": newprice_type, "prc": str(newprice)}

        if newprice_type in ['SL-LMT', 'SL-MKT']:
            if newtrigger_price is None:
                reporterror('trigger price is missing')
                return None

            else:
                values["trgprc"] = str(newtrigger_price)
        # if cover order or high leverage order
        if bookloss_price != 0.0:
            values["blprc"] = str(bookloss_price)
        # trailing price
        if trail_price != 0.0:
            values["trailprc"] = str(trail_price)
        # book profit of bracket order
        if bookprofit_price != 0.0:
            values["bpprc"] = str(bookprofit_price)

        return self.send_payload(url, values)

    def cancel_order(self, orderno):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['cancelorder']}"
        print(url)

        # prepare the data
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "norenordno": str(orderno),
        }
        return self.send_payload(url, values)

    def exit_order(self, orderno, product_type):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['exitorder']}"
        print(url)

        # prepare the data
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "norenordno": orderno,
            "prd": product_type,
        }
        return self.send_payload(url, values)

    def position_product_conversion(self, exchange, tradingsymbol, quantity, new_product_type, previous_product_type,
                                    buy_or_sell, day_or_cf):
        '''
        Coverts a day or carryforward position from one product to another. 
        '''
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['product_conversion']}"
        print(url)

        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "actid": self.__accountid, "exch": exchange,
                  "tsym": urllib.parse.quote_plus(tradingsymbol), "qty": str(quantity), "prd": new_product_type,
                  "prevprd": previous_product_type, "trantype": buy_or_sell, "postype": day_or_cf}

        return self.send_payload(url, values)

    def single_order_history(self, orderno):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['singleorderhistory']}"
        print(url)

        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "norenordno": orderno}

        return self.send_payload(url, values, expected_return_type="list")

    def get_order_book(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['orderbook']}"
        reportmsg(url)

        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username}

        return self.send_payload(url, values, expected_return_type="list")

    def get_trade_book(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['tradebook']}"
        reportmsg(url)

        # prepare the data
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "actid": self.__accountid,
        }

        return self.send_payload(url, values, expected_return_type="list")

    def searchscrip(self, exchange, searchtext):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['searchscrip']}"
        reportmsg(url)

        if searchtext is None:
            reporterror('search text cannot be null')
            return None

        values = {"uid": self.__username, "exch": exchange, "stext": urllib.parse.quote_plus(searchtext)}

        return self.send_payload(url, values)

    def get_option_chain(self, exchange, tradingsymbol, strikeprice, count=2):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['optionchain']}"
        reportmsg(url)

        values = {"uid": self.__username, "exch": exchange, "tsym": urllib.parse.quote_plus(tradingsymbol),
                  "strprc": str(strikeprice), "cnt": str(count)}

        return self.send_payload(url, values)

    def get_security_info(self, exchange, token):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['scripinfo']}"
        reportmsg(url)

        values = {"uid": self.__username, "exch": exchange, "token": token}

        return self.send_payload(url, values)

    def get_quotes(self, exchange, token):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['getquotes']}"
        reportmsg(url)

        values = {"uid": self.__username, "exch": exchange, "token": token}

        return self.send_payload(url, values)

    def get_time_price_series(self, exchange, token, starttime=None, endtime=None, interval=None):
        """
        gets the chart data
        interval possible values 1, 3, 5 , 10, 15, 30, 60, 120, 240
        """
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['TPSeries']}"
        reportmsg(url)

        # prepare the data
        if starttime is None:
            timestring = time.strftime('%d-%m-%Y') + ' 00:00:00'
            timeobj = time.strptime(timestring, '%d-%m-%Y %H:%M:%S')
            starttime = time.mktime(timeobj)

        #
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "exch": exchange,
            "token": token,
            "st": str(starttime),
        }
        if endtime is not None:
            values["et"] = str(endtime)
        if interval is not None:
            values["intrv"] = str(interval)

        return self.send_payload(url, values, expected_return_type="list")

    def get_daily_price_series(self, exchange, tradingsymbol, startdate=None, enddate=None):
        config = NorenApi.__service_config

        # prepare the uri
        # url = f"{config['eoddata_endpoint']}"
        url = f"{config['host']}{config['routes']['get_daily_price_series']}"
        reportmsg(url)

        # prepare the data
        if startdate is None:
            week_ago = datetime.date.today() - datetime.timedelta(days=7)
            startdate = dt.combine(week_ago, dt.min.time()).timestamp()

        if enddate is None:
            enddate = dt.now().timestamp()

        values = {"uid": self.__username, "sym": '{0}:{1}'.format(exchange, tradingsymbol), "from": str(startdate),
                  "to": str(enddate)}

        headers = {"Content-Type": "application/json; charset=utf-8"}
        return self.send_payload(url, values, headers=headers)

    def get_holdings(self, product_type=None):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['holdings']}"
        reportmsg(url)

        if product_type is None:
            product_type = ProductType.Delivery

        values = {"uid": self.__username, "actid": self.__accountid, "prd": product_type}

        return self.send_payload(url, values, expected_return_type="list")

    def get_limits(self, product_type=None, segment=None, exchange=None):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['limits']}"
        reportmsg(url)

        values = {"uid": self.__username, "actid": self.__accountid}

        if product_type is not None:
            values["prd"] = product_type

        if segment is not None:
            values["seg"] = segment

        if exchange is not None:
            values["exch"] = exchange

        return self.send_payload(url, values)

    def get_positions(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['positions']}"
        reportmsg(url)

        values = {"uid": self.__username, "actid": self.__accountid}

        return self.send_payload(url, values, expected_return_type="list")

    def span_calculator(self, actid, positions: list):
        config = NorenApi.__service_config
        # prepare the uri
        url = f"{config['host']}{config['routes']['span_calculator']}"
        reportmsg(url)

        senddata = {'actid': self.__accountid, 'pos': positions}
        values = json.dumps(senddata, default=lambda o: o.encode())

        return self.send_payload(url, values)

    def option_greek(self, expiredate, StrikePrice, SpotPrice, InterestRate, Volatility, OptionType):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['option_greek']}"
        reportmsg(url)

        # prepare the data
        values = {"source": "API", "actid": self.__accountid, "exd": expiredate, "strprc": StrikePrice,
                  "sptprc": SpotPrice, "int_rate": InterestRate, "volatility": Volatility, "optt": OptionType}

        return self.send_payload(url, values)
