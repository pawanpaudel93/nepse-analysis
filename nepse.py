import sys
import json
import logging
import requests
from time import sleep
from datetime import datetime
from typing import Tuple, Union
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(levelname)s: %(message)s"
)


class TimeoutHTTPAdapter(HTTPAdapter):
    DEFAULT_TIMEOUT = 5

    def __init__(self, *args, **kwargs):
        self.timeout = self.DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class NEPSE:
    _base_url = "https://newweb.nepalstock.com"
    _securities = {}
    _sectors = {}

    def __init__(self) -> None:
        self._create_session()
        self._get_all_securities()

    def _create_session(self) -> None:
        self._session = requests.Session()
        retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = TimeoutHTTPAdapter(max_retries=retries)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def _get_common_headers(self):
        return {
            "authority": self._base_url.replace("https://", ""),
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
            "sec-gpc": "1",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "accept-language": "en-US,en;q=0.9",
        }

    def _create_url(self, url) -> str:
        return urljoin(self._base_url, url)

    def _perform_request(
        self, *args, **kwargs
    ) -> Tuple[Union[requests.Response, None], Union[str, None]]:
        try:
            sleep(0.5)
            response = self._session.request(*args, **kwargs)
            response.raise_for_status()
        except BaseException as error:
            return None, error
        else:
            return response, None

    def _get_all_securities(self):
        url = self._create_url("/api/nots/security?nonDelisted=true")

        headers = {
            **self._get_common_headers(),
            "referer": self._base_url,
        }

        response, error = self._perform_request("GET", url, headers=headers, data={})
        if not error:
            self._securities = {
                security["symbol"]: security for security in response.json()
            }
        else:
            logging.error(error)

    def _get_sectors(self):
        url = self._create_url("/api/nots")

        payload = {}
        headers = {
            **self._get_common_headers(),
            "referer": "https://newweb.nepalstock.com/",
        }

        response, error = self._perform_request(
            "GET", url, headers=headers, data=payload
        )

        if not error:
            self._sub_indices = {sector["id"]: sector for sector in response.json()}
        else:
            logging.error(error)

    @staticmethod
    def _get_sorted_dict(items: dict, top_n: int) -> dict:
        return {
            k: items[k]
            for i, k in enumerate(sorted(items, key=items.get, reverse=True))
            if i <= top_n
        }

    def _get_floorsheet(
        self, symbol: str, date: Union[str, None] = None, top_n: int = 5
    ):
        page_number = 0
        last_page = False
        floorsheet_data = []
        _id = self._securities[symbol]["id"]
        if not date:
            date = datetime.today().strftime("%Y-%m-%d")
        url = self._create_url(f"/api/nots/security/floorsheet/{_id}")
        while not last_page:
            params = {
                "size": 2000,
                "businessDate": date,
                "sort": "contractId,asc",
                "page": page_number,
            }

            payload = {"id": 232}
            headers = {
                **self._get_common_headers(),
                "content-type": "application/json",
                "origin": self._base_url,
                "referer": "%s/company/detail/%s" % (self._base_url, _id),
            }

            response, error = self._perform_request(
                "POST", url, headers=headers, params=params, data=json.dumps(payload)
            )
            if not error:
                floorsheet = response.json()["floorsheets"]
                last_page = floorsheet["last"]
                if not floorsheet["empty"]:
                    floorsheet_data.extend(floorsheet["content"])
            else:
                logging.error(error)
            page_number += 1
        top_buy = {}
        top_sell = {}
        for data in floorsheet_data:
            buyer_id = ("%s - %s") % (data["buyerMemberId"], data["buyerBrokerName"])
            seller_id = ("%s - %s") % (data["sellerMemberId"], data["sellerBrokerName"])
            if top_buy.get(buyer_id) != None:
                top_buy[buyer_id] += data["contractQuantity"]
            else:
                top_buy[buyer_id] = 0
                top_buy[buyer_id] += data["contractQuantity"]

            if top_sell.get(seller_id) != None:
                top_sell[seller_id] += data["contractQuantity"]
            else:
                top_sell[seller_id] = 0
                top_sell[seller_id] += data["contractQuantity"]
        top_buy = self._get_sorted_dict(top_buy, top_n)
        top_sell = self._get_sorted_dict(top_sell, top_n)
        return top_buy, top_sell

    def _get_sector_floorsheet(
        self, sector_id: int, date: Union[str, None] = None, top_n: int = 5
    ):
        url = self._create_url("/api/nots/securityDailyTradeStat/%s" % sector_id)
        headers = {
            **self._get_common_headers(),
            "referer": self._base_url,
        }
        response, error = self._perform_request("GET", url, headers=headers, data={})
        sector_floorsheet = {}
        if not error:
            securities = response.json()
            for security in securities:
                if not date:
                    date = datetime.today().strftime("%Y-%m-%d")
                top_buy, top_sell = self._get_floorsheet(
                    security["symbol"], date=date, top_n=top_n
                )
                sector_floorsheet[security["symbol"]] = {
                    "top_buy": top_buy,
                    "top_sell": top_sell,
                }
            print(json.dumps(sector_floorsheet, indent=4))
        else:
            logging.error(error)
