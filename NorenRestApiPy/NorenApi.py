import asyncio
import datetime
import hashlib
import json
import logging
import time
import urllib
from datetime import datetime as dt

import aiohttp
import websockets

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
        self.__password = None
        self.__accountid = None
        self.__username = None
        self.__on_open = None
        self.__subscribe_callback = None
        self.__order_update_callback = None
        self.__websocket_connected = False  # True -> Connected, False -> Not Connected
        self.__ws = None

        self.__service_config["host"] = host
        self.__service_config["websocket_endpoint"] = websocket

        self.__subscribers = {}
        self.__market_status_messages = []
        self.__exchange_messages = []

        self.__session = aiohttp.ClientSession()
        self.__loop = asyncio.get_event_loop()

        # make susertoken accessible outside the class
        self.susertoken = None

    def __del__(self):
        # Close HTTP connection when this object is destroyed
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.__session.close())
            else:
                loop.run_until_complete(self.__session.close())
        except Exception as e:
            logger.error(e)

    def close_websocket(self):
        if self.__websocket_connected:
            self.__websocket_connected = False

    async def start_websocket(
            self,
            subscribe_callback=None,
            order_update_callback=None,
            socket_open_callback=None,
            socket_close_callback=None,
            socket_error_callback=None,
    ):
        self.__order_update_callback = order_update_callback
        self.__subscribe_callback = subscribe_callback
        self.__on_open = socket_open_callback

        if self.__websocket_connected:
            return

        asyncio.create_task(self.websocket_task_async())

    async def websocket_task_async(self):
        url = self.__service_config["websocket_endpoint"]
        self.__ws = await websockets.connect(url, ping_interval=3)
        self.__websocket_connected = True
        logger.info("Websocket connected!")

        # Call on open callback
        await self.__on_open_callback()

        try:
            while self.__websocket_connected:
                await asyncio.sleep(0.05)
                message = await self.__ws.recv()
                res = json.loads(message)

                if self.__subscribe_callback is not None:
                    if res["t"] in ["tk", "tf"]:
                        await self.__subscribe_callback(res)
                        continue
                    if res["t"] in ["dk", "df"]:
                        await self.__subscribe_callback(res)
                        continue

                if res["t"] == "ck" and res["s"] != "OK":
                    logger.error(res)
                    continue

                if (self.__order_update_callback is not None) and res["t"] == "om":
                    await self.__order_update_callback(res)
                    continue

                if self.__on_open and res["t"] == "ck" and res["s"] == "OK":
                    await self.__on_open()
                    continue
        except Exception as e:
            logger.error(e)
        finally:
            await self.__ws.close()
            self.__websocket_connected = False

    async def __on_open_callback(self):
        # prepare the data
        values = {
            "t": "c",
            "uid": self.__username,
            "actid": self.__username,
            "susertoken": self.susertoken,
            "source": "API",
        }
        payload = json.dumps(values)
        reportmsg(payload)
        await self.__ws.send(payload)

    async def send_payload(self, url, values, is_authorized=True, headers=None):
        payload = f'jData={json.dumps(values)}'
        if is_authorized:
            payload += f'&jKey={self.susertoken}'

        reportmsg(payload)

        async with self.__session.post(url, data=payload, headers=headers) as response:
            response_text = await response.text()
            reportmsg(response_text)
            return json.loads(response_text)

    async def login(self, userid, password, twoFA, vendor_code, api_secret, imei):
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
        res_dict = await self.send_payload(url, values, is_authorized=False)

        if res_dict:
            self.__username = userid
            self.__accountid = userid
            self.__password = password
            self.susertoken = res_dict['susertoken']

        return res_dict

    def set_session(self, userid, password, usertoken):

        self.__username = userid
        self.__accountid = userid
        self.__password = password
        self.susertoken = usertoken

        reportmsg(f'{userid} session set to : {self.susertoken}')

        return True

    async def forgot_password(self, userid, pan, dob):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['forgot_password']}"
        reportmsg(url)

        # prepare the data
        values = {"source": "API", "uid": userid, "pan": pan, "dob": dob}
        return await self.send_payload(url, values, is_authorized=False)

    async def logout(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['logout']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username}
        payload = f'jData={json.dumps(values)}' + f'&jKey={self.susertoken}'

        res_dict = await self.send_payload(url, payload)

        if res_dict:
            self.__username = None
            self.__accountid = None
            self.__password = None
            self.susertoken = None

        return res_dict

    async def get_watch_list_names(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['watchlist_names']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username}
        return await self.send_payload(url, values)

    async def get_watch_list(self, wlname):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['watchlist']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "wlname": wlname}
        return await self.send_payload(url, values)

    async def add_watch_list_scrip(self, wlname, instrument):
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
        return await self.send_payload(url, values)

    async def delete_watch_list_scrip(self, wlname, instrument):
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
        return await self.send_payload(url, values)

    async def place_order(self, buy_or_sell, product_type,
                          exchange, tradingsymbol, quantity, discloseqty,
                          price_type, price=0.0, trigger_price=None,
                          retention='DAY', amo='NO', remarks=None, bookloss_price=0.0, bookprofit_price=0.0,
                          trail_price=0.0):
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

        return await self.send_payload(url, values)

    async def modify_order(self, orderno, exchange, tradingsymbol, newquantity,
                           newprice_type, newprice=0.0, newtrigger_price=None, bookloss_price=0.0, bookprofit_price=0.0,
                           trail_price=0.0):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['modifyorder']}"
        # print(url)

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

        return await self.send_payload(url, values)

    async def cancel_order(self, orderno):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['cancelorder']}"
        # print(url)

        # prepare the data
        values = {
            'ordersource': 'API',
            "uid": self.__username,
            "norenordno": str(orderno),
        }
        return await self.send_payload(url, values)

    async def exit_order(self, orderno, product_type):
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
        return await self.send_payload(url, values)

    async def position_product_conversion(self, exchange, tradingsymbol, quantity, new_product_type,
                                          previous_product_type,
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

        return await self.send_payload(url, values)

    async def single_order_history(self, orderno):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['singleorderhistory']}"
        print(url)

        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username, "norenordno": orderno}

        return await self.send_payload(url, values)

    async def get_order_book(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['orderbook']}"
        reportmsg(url)

        # prepare the data
        values = {'ordersource': 'API', "uid": self.__username}

        return await self.send_payload(url, values)

    async def get_trade_book(self):
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

        return await self.send_payload(url, values)

    async def searchscrip(self, exchange, searchtext):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['searchscrip']}"
        reportmsg(url)

        if searchtext is None:
            reporterror('search text cannot be null')
            return None

        values = {"uid": self.__username, "exch": exchange, "stext": urllib.parse.quote_plus(searchtext)}

        return await self.send_payload(url, values)

    async def get_option_chain(self, exchange, tradingsymbol, strikeprice, count=2):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['optionchain']}"
        reportmsg(url)

        values = {"uid": self.__username, "exch": exchange, "tsym": urllib.parse.quote_plus(tradingsymbol),
                  "strprc": str(strikeprice), "cnt": str(count)}

        return await self.send_payload(url, values)

    async def get_security_info(self, exchange, token):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['scripinfo']}"
        reportmsg(url)

        values = {"uid": self.__username, "exch": exchange, "token": token}

        return await self.send_payload(url, values)

    async def get_quotes(self, exchange, token):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['getquotes']}"
        reportmsg(url)

        values = {"uid": self.__username, "exch": exchange, "token": token}

        return await self.send_payload(url, values)

    async def get_time_price_series(self, exchange, token, starttime=None, endtime=None, interval=None):
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

        return await self.send_payload(url, values)

    async def get_daily_price_series(self, exchange, tradingsymbol, startdate=None, enddate=None):
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
        return await self.send_payload(url, values, headers=headers)

    async def get_holdings(self, product_type=None):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['holdings']}"
        reportmsg(url)

        if product_type is None:
            product_type = ProductType.Delivery

        values = {"uid": self.__username, "actid": self.__accountid, "prd": product_type}

        return await self.send_payload(url, values)

    async def get_limits(self, product_type=None, segment=None, exchange=None):
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

        return await self.send_payload(url, values)

    async def get_positions(self):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['positions']}"
        reportmsg(url)

        values = {"uid": self.__username, "actid": self.__accountid}

        return await self.send_payload(url, values)

    async def span_calculator(self, actid, positions: list):
        config = NorenApi.__service_config
        # prepare the uri
        url = f"{config['host']}{config['routes']['span_calculator']}"
        reportmsg(url)

        senddata = {'actid': self.__accountid, 'pos': positions}
        values = json.dumps(senddata, default=lambda o: o.encode())

        return await self.send_payload(url, values)

    async def option_greek(self, expiredate, StrikePrice, SpotPrice, InterestRate, Volatility, OptionType):
        config = NorenApi.__service_config

        # prepare the uri
        url = f"{config['host']}{config['routes']['option_greek']}"
        reportmsg(url)

        # prepare the data
        values = {"source": "API", "actid": self.__accountid, "exd": expiredate, "strprc": StrikePrice,
                  "sptprc": SpotPrice, "int_rate": InterestRate, "volatility": Volatility, "optt": OptionType}

        return await self.send_payload(url, values)
