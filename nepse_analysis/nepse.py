import functools
import json
import locale
import os
import pickle
from datetime import datetime, timedelta
from time import sleep
from typing import Tuple, Union
from urllib.parse import urljoin

import requests
from tabulate import tabulate
from urllib3.util.retry import Retry

from .utils import TimeoutHTTPAdapter, TokenParser, get_logger

logger = get_logger()

locale.setlocale(locale.LC_ALL, "en_IN")


class NEPSE:
    _base_url = "https://www.nepalstock.com.np"
    _data_dir = "./data"

    def __init__(self) -> None:
        self._id = 0
        self._jwt_tokens = {"accessToken": "", "refreshToken": ""}
        self._securities = {}
        self._sectors = {}
        self._holidays = []
        self._create_session()
        self._fetch_jwt_tokens()
        self._fetch_all_securities()
        self._fetch_sectors()
        self._fetch_holidays()
        self._fetch_id()

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def securities(self) -> dict:
        return self._securities

    @property
    def sectors(self) -> dict:
        return self._sectors

    def _pickle_data(self, name: str, data: dict) -> None:
        with open(os.path.join(self._data_dir, name), "wb") as f:
            pickle.dump(data, f)

    def _unpickle_data(self, name: str) -> Union[dict, None]:
        with open(os.path.join(self._data_dir, name), "rb") as f:
            return pickle.load(f)

    def _check_date_sector(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                date_string = kwargs.get("date") or args[1]
                date = datetime.strptime(date_string, "%Y-%m-%d")
            except (IndexError, ValueError):
                date = None
            try:
                symbol_or_sector = kwargs.get("symbol") or kwargs.get("sector_id") or args[0]
            except IndexError:
                symbol_or_sector = None
            if type(symbol_or_sector) == int:
                if not self._sectors.get(symbol_or_sector):
                    logger.info(f"Sector {symbol_or_sector} does not exist")
                    self.display_sectors()
                    return

            if date and date.weekday() in [4, 5] or date > datetime.today() or date_string in self._holidays:
                logger.info("Floorsheet is not available on Friday and Saturday and Holidays !!!")
            else:
                value = func(self, *args, **kwargs)
                if value:
                    return value

        return wrapper

    def _create_session(self) -> None:
        self._session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[401, 413, 429, 502, 503, 504])
        adapter = TimeoutHTTPAdapter(max_retries=retries)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        self._session.hooks["response"].append(self._check_response)

    def _check_response(self, response, *args, **kwargs) -> requests.Response:
        if response.status_code == 401:
            self._refresh_jwt_tokens()
            response.request.headers["authorization"] = 'Salter %s' % self._jwt_tokens["accessToken"]
            response.history.append(response)
            return response
        return response

    def _get_common_headers(self) -> dict:
        return {
            "authority": self._base_url.replace("https://", ""),
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "sec-gpc": "1",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "accept-language": "en-US,en;q=0.9",
        }

    def _fetch_id(self) -> None:
        url = self._create_url("/api/nots/nepse-data/market-open")
        headers = {
            **self._get_common_headers(),
            "referer": "%s/" % self._base_url,
            'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
        }
        response, error = self._perform_request("GET", url, headers=headers, data={})
        if not error:
            id = response.json()["id"]
            self._id = TokenParser.get_post_id(id)

    def _create_url(self, url) -> str:
        return urljoin(self._base_url, url)

    def _perform_request(self, *args, **kwargs) -> Tuple[Union[requests.Response, None], Union[str, None]]:
        try:
            sleep(0.3)
            response = self._session.request(*args, **kwargs)
            response.raise_for_status()
        except BaseException as error:
            return response.text, error
        else:
            return response, None

    def _fetch_jwt_tokens(self) -> None:
        url = self._create_url("/api/authenticate/prove")

        headers = {**self._get_common_headers(), 'referer': self._base_url}

        response, error = self._perform_request("GET", url, headers=headers, data={})
        if not error:
            self._jwt_tokens = TokenParser.parse(response.json())

    def _refresh_jwt_tokens(self) -> None:
        url = self._create_url("/api/authenticate/refresh-token")

        payload = {"refreshToken": self._jwt_tokens["refreshToken"]}
        headers = {
            **self._get_common_headers(),
            'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
            'content-type': 'application/json',
            'origin': self._base_url,
            'referer': '%s/' % self._base_url,
        }

        response, error = self._perform_request("POST", url, headers=headers, data=json.dumps(payload))
        if not error:
            self._jwt_tokens = TokenParser.parse(response.json())

    def _fetch_holidays(self) -> None:
        year = datetime.today().year
        url = self._create_url("/api/nots/holiday/list?year=%s" % year)
        headers = {
            **self._get_common_headers(),
            'referer': '%s/holiday-listing' % self._base_url,
            'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
        }

        response, error = self._perform_request("GET", url, headers=headers, data={})
        if not error:
            self._holidays = [holiday['holidayDate'] for holiday in response.json()]

    def _fetch_all_securities(self) -> None:
        url = self._create_url("/api/nots/securityDailyTradeStat/58")

        headers = {
            **self._get_common_headers(),
            "referer": self._base_url,
            'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
        }

        response, error = self._perform_request("GET", url, headers=headers, data={})
        if not error:
            self._securities = {security["symbol"]: security for security in response.json()}
        else:
            logger.error(error)

    def _fetch_sectors(self) -> None:
        if os.path.exists(os.path.join(self._data_dir, "sectors.pkl")):
            self._sectors = self._unpickle_data("sectors.pkl")
            return
        url = self._create_url("/api/nots")

        headers = {
            **self._get_common_headers(),
            "referer": self._base_url,
            'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
        }

        response, error = self._perform_request("GET", url, headers=headers, data={})

        if not error:
            self._sectors = {sector["id"]: sector["index"] for sector in response.json()}
            self._pickle_data("sectors.pkl", self._sectors)
        else:
            logger.error(error)

    @staticmethod
    def _get_sorted_list(data: dict, top_n: Union[int, None] = None) -> list:
        sorted_data = sorted(data.items(), key=lambda x: x[1]["quantity"], reverse=True)
        return sorted_data[:top_n] if top_n else sorted_data

    def _display_data(self, symbol: str, top_buy: list, top_sell: list, top_n: int) -> None:
        data = [
            [
                "Top Buyer",
                "Quantity",
                "Percent",
                "↕",
                "Top Seller",
                "Quantity",
                "Percent",
            ],
        ]
        for index in range(0, top_n):
            try:
                buy_item = top_buy[index]
                sell_item = top_sell[index]
                data.append(
                    [
                        buy_item[0],
                        self._format_number(buy_item[1]["quantity"]),
                        buy_item[1]["percent"],
                        "↕",
                        sell_item[0],
                        self._format_number(sell_item[1]["quantity"]),
                        sell_item[1]["percent"],
                    ]
                )
            except (IndexError, KeyError):
                pass
        print(tabulate([symbol], tablefmt="grid"), end="\n")
        print(
            tabulate(data, headers="firstrow", tablefmt="fancy_grid"),
            end="\n\n",
        )

    @staticmethod
    def _format_number(number) -> str:
        return locale.format_string("%d", number, grouping=True)

    @_check_date_sector
    def _get_floorsheet(
        self,
        symbol: str,
        date: Union[str, None] = None,
        top_n: Union[int, None] = 5,
    ) -> Union[Tuple[list, list], Tuple[None, None]]:
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        page_number = 0
        last_page = False
        floorsheet_data = []
        _id = self._securities[symbol]["securityId"]
        url = self._create_url(f"/api/nots/security/floorsheet/{_id}")
        while not last_page:
            params = {
                "size": 2000,
                "businessDate": date,
                "sort": "contractId,asc",
                "page": page_number,
            }

            payload = {"id": self._id}
            headers = {
                **self._get_common_headers(),
                "content-type": "application/json",
                "origin": self._base_url,
                "referer": "%s/company/detail/%s" % (self._base_url, _id),
                'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
            }

            response, error = self._perform_request(
                "POST", url, headers=headers, params=params, data=json.dumps(payload)
            )
            if type(response) == str and response == "Searched Date is not valid.":
                last_page = True
                logger.error(response)
            if not error:
                response_json = response.json()
                total_quantity = response_json["totalQty"]
                floorsheet = response_json["floorsheets"]
                last_page = floorsheet["last"]
                if not floorsheet["empty"]:
                    floorsheet_data.extend(floorsheet["content"])
            else:
                logger.error(error)
            page_number += 1
        top_buy, top_sell = {}, {}
        if len(floorsheet_data) > 0:
            for data in floorsheet_data:
                buyer_id = ("%s - %s") % (
                    data["buyerMemberId"],
                    data["buyerBrokerName"],
                )
                seller_id = ("%s - %s") % (
                    data["sellerMemberId"],
                    data["sellerBrokerName"],
                )
                if top_buy.get(buyer_id) != None:
                    top_buy[buyer_id]["quantity"] += data["contractQuantity"]
                else:
                    top_buy[buyer_id] = {"quantity": 0}
                    top_buy[buyer_id]["quantity"] += data["contractQuantity"]
                if top_sell.get(seller_id) != None:
                    top_sell[seller_id]["quantity"] += data["contractQuantity"]
                else:
                    top_sell[seller_id] = {"quantity": 0}
                    top_sell[seller_id]["quantity"] += data["contractQuantity"]

            for k, v in top_buy.items():
                top_buy[k]["percent"] = round(v["quantity"] * 100 / total_quantity, 2)
            for k, v in top_sell.items():
                top_sell[k]["percent"] = round(v["quantity"] * 100 / total_quantity, 2)
            top_buy = self._get_sorted_list(top_buy, top_n)
            top_sell = self._get_sorted_list(top_sell, top_n)
        return top_buy, top_sell

    @_check_date_sector
    def _get_sector_floorsheet(
        self, sector_id: int, date: Union[str, None] = None, top_n: Union[None, int] = 5
    ) -> Union[dict, None]:
        url = self._create_url("/api/nots/securityDailyTradeStat/%s" % sector_id)
        headers = {
            **self._get_common_headers(),
            "referer": self._base_url,
            'authorization': 'Salter %s' % self._jwt_tokens["accessToken"],
        }
        response, error = self._perform_request("GET", url, headers=headers, data={})
        sector_floorsheet = {}
        if not error:
            securities = response.json()
            for security in securities:
                top_buy, top_sell = self._get_floorsheet(security["symbol"], date=date, top_n=top_n)
                sector_floorsheet[security["symbol"]] = {
                    "top_buy": top_buy,
                    "top_sell": top_sell,
                }
        else:
            logger.error(error)
        return sector_floorsheet

    @staticmethod
    def _get_date_range(start_date: str, end_date: str) -> list:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        return [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    def _get_floorsheet_by_range(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> Union[dict, None]:
        date_range = self._get_date_range(start_date, end_date)
        final_data = {}
        for date in date_range:
            floorsheet = self._get_floorsheet(symbol, date.strftime("%Y-%m-%d"), top_n=None)
            if floorsheet:
                top_buy, top_sell = floorsheet
                for buy in top_buy:
                    try:
                        final_data[buy[0]]["buy"] += buy[1]["quantity"]
                    except KeyError:
                        final_data[buy[0]] = {"buy": 0, "sell": 0}
                        final_data[buy[0]]["buy"] += buy[1]["quantity"]
                for sell in top_sell:
                    try:
                        final_data[sell[0]]["sell"] += sell[1]["quantity"]
                    except KeyError:
                        final_data[sell[0]] = {"buy": 0, "sell": 0}
                        final_data[sell[0]]["sell"] += sell[1]["quantity"]
        return final_data

    def _get_sector_floorsheet_by_range(self, sector_id: int, start_date: str, end_date: str):
        date_range = self._get_date_range(start_date, end_date)
        final_data = {}
        for date in date_range:
            sector_floorsheet = self._get_sector_floorsheet(sector_id, date.strftime("%Y-%m-%d"), top_n=None)
            if sector_floorsheet:
                for symbol, floorsheet in sector_floorsheet.items():
                    top_buy, top_sell = floorsheet["top_buy"], floorsheet["top_sell"]
                    for buy in top_buy:
                        if symbol in final_data:
                            if buy[0] in final_data[symbol]:
                                final_data[symbol][buy[0]]["buy"] += buy[1]["quantity"]
                            else:
                                final_data[symbol][buy[0]] = {"buy": 0, "sell": 0}
                                final_data[symbol][buy[0]]["buy"] += buy[1]["quantity"]
                        else:
                            final_data[symbol] = {buy[0]: {"buy": 0, "sell": 0}}
                            final_data[symbol][buy[0]]["buy"] += buy[1]["quantity"]
                    for sell in top_sell:
                        if symbol in final_data:
                            if sell[0] in final_data[symbol]:
                                final_data[symbol][sell[0]]["sell"] += sell[1]["quantity"]
                            else:
                                final_data[symbol][sell[0]] = {"buy": 0, "sell": 0}
                                final_data[symbol][sell[0]]["sell"] += sell[1]["quantity"]
                        else:
                            final_data[symbol] = {sell[0]: {"buy": 0, "sell": 0}}
                            final_data[symbol][sell[0]]["sell"] += sell[1]["quantity"]
        return final_data

    @_check_date_sector
    def display_security_floorsheet(self, symbol: str, date: Union[str, None] = None, top_n: Union[None, int] = 5):
        top_buy, top_sell = self._get_floorsheet(symbol, date, top_n)
        self._display_data(symbol, top_buy, top_sell, top_n)

    def display_security_combined_floorsheet(self, symbol: str, start_date: str, end_date: str, order_by: str = "buy"):
        if order_by not in ["buy", "sell"]:
            logger.info(f"Cannot order by {order_by}. It can be order by only one of {['buy', 'sell']}")
            return
        buy_sell_data = self._get_floorsheet_by_range(symbol, start_date, end_date)
        buy_sell_data = sorted(buy_sell_data.items(), key=lambda x: x[1][order_by], reverse=True)
        data = [["Broker", "Buy", "Sell"]]
        data.extend(
            [
                [broker, self._format_number(buy_sell["buy"]), self._format_number(buy_sell["sell"])]
                for broker, buy_sell in buy_sell_data
            ]
        )
        print(tabulate([symbol], tablefmt="grid"), end="\n")
        print(tabulate(data, headers="firstrow", tablefmt="fancy_grid"))

    @_check_date_sector
    def display_sector_floorsheet(self, sector_id: int, date: Union[str, None] = None, top_n: int = 5):
        sector_floorsheet = self._get_sector_floorsheet(sector_id, date, top_n)
        for symbol, floorsheet in sector_floorsheet.items():
            top_buy = floorsheet["top_buy"]
            top_sell = floorsheet["top_sell"]
            self._display_data(symbol, top_buy, top_sell, top_n)

    def display_sectors(self):
        data = [["Sector ID", "Sector Name"]]
        for sector_id, sector_name in self._sectors.items():
            data.append([sector_id, sector_name])
        print(tabulate(["SECTORS"], tablefmt="grid"), end="\n")
        print(tabulate(data, headers="firstrow", tablefmt="fancy_grid"))

    def display_securities(self, top_n: Union[int, None] = None, order_by: str = "symbol", asc: bool = True):
        data_mapping = {
            "symbol": "symbol",
            "name": "securityName",
            "open": "openPrice",
            "high": "highPrice",
            "low": "lowPrice",
            "ltp": "lastTradedPrice",
            "prev.close": "previousClose",
            "volume": "totalTradeQuantity",
            "change": "percentageChange",
        }
        if order_by not in data_mapping.keys():
            logger.info(f"Cannot order by {order_by}. It can be order by only one of {list(data_mapping.keys())}")
            return
        data = [
            [
                "Symbol",
                "Name",
                "Open",
                "High",
                "Low",
                "LTP",
                "Prev. Close",
                "Volume",
                "Change(%)",
            ]
        ]

        if data_mapping.get(order_by):
            securities = sorted(
                self._securities.items(),
                key=lambda x: x[1][data_mapping[order_by]],
                reverse=not asc,
            )
        if top_n:
            securities = securities[:top_n]
        data.extend(
            [
                [
                    symbol,
                    security["securityName"],
                    self._format_number(security["openPrice"]),
                    self._format_number(security["highPrice"]),
                    self._format_number(security["lowPrice"]),
                    self._format_number(security["lastTradedPrice"]),
                    self._format_number(security["previousClose"]),
                    self._format_number(security["totalTradeQuantity"]),
                    round(security["percentageChange"], 2),
                ]
                for symbol, security in securities
            ]
        )
        print(tabulate(["SECURITIES"], tablefmt="grid"), end="\n")
        print(tabulate(data, headers="firstrow", tablefmt="fancy_grid"))

    @_check_date_sector
    def display_sector_combined_broker_trade(self, sector_id: int, date: Union[str, None] = None, top_n: int = 5):
        sector_analysis = {"top_buy": {}, "top_sell": {}}
        sector_floorsheet = self._get_sector_floorsheet(sector_id, date, top_n=None)
        for _, floorsheet in sector_floorsheet.items():
            top_buy = floorsheet["top_buy"]
            top_sell = floorsheet["top_sell"]
            top_buy_total = 0
            top_sell_total = 0
            for broker in top_buy:
                try:
                    sector_analysis["top_buy"][broker[0]]["quantity"] += broker[1]["quantity"]
                except KeyError:
                    sector_analysis["top_buy"][broker[0]] = {"quantity": 0}
                    sector_analysis["top_buy"][broker[0]]["quantity"] = broker[1]["quantity"]
                top_buy_total += broker[1]["quantity"]
            for broker in top_sell:
                try:
                    sector_analysis["top_sell"][broker[0]]["quantity"] += broker[1]["quantity"]
                except KeyError:
                    sector_analysis["top_sell"][broker[0]] = {"quantity": 0}
                    sector_analysis["top_sell"][broker[0]]["quantity"] += broker[1]["quantity"]
                top_sell_total += broker[1]["quantity"]
            for broker in sector_analysis["top_buy"]:
                sector_analysis["top_buy"][broker]["percent"] = round(
                    sector_analysis["top_buy"][broker]["quantity"] * 100 / top_buy_total, 2
                )
            for broker in sector_analysis["top_sell"]:
                sector_analysis["top_sell"][broker]["percent"] = round(
                    sector_analysis["top_sell"][broker]["quantity"] * 100 / top_sell_total, 2
                )

        top_buy = self._get_sorted_list(sector_analysis["top_buy"], top_n=top_n)
        top_sell = self._get_sorted_list(sector_analysis["top_sell"], top_n=top_n)
        self._display_data(self._sectors.get(sector_id), top_buy, top_sell, top_n)

    def display_sector_top_trade(self, sector_id: int, start_date: str, end_date: str, order_by: str = "buy"):
        if order_by not in ["buy", "sell"]:
            logger.info(f"Cannot order by {order_by}. It can be order by only one of {['buy', 'sell']}")
            return
        order_by = 2 if order_by == "buy" else 3
        final_data = []
        buy_sell_data = self._get_sector_floorsheet_by_range(sector_id, start_date, end_date)
        for symbol, buy_sell in buy_sell_data.items():
            for data in buy_sell.items():
                final_data.append([symbol, data[0], data[1]["buy"], data[1]["sell"]])
        final_data = sorted(final_data, key=lambda x: x[order_by], reverse=True)
        data = [["Symbol", "Broker", "Buy", "Sell"]]
        data.extend(final_data)
        print(tabulate(data, headers="firstrow", tablefmt="fancy_grid"))
