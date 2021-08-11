import sys
import json
import logging
import requests
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

    def _get_floorsheet(self, symbol):
        page_number = 0
        last_page = False
        floorsheet_data = []
        _id = self._securities[symbol]["id"]
        url = self._create_url(f"/api/nots/security/floorsheet/{_id}")
        while not last_page:
            params = {
                "size": 2000,
                "businessDate": datetime.today().strftime("%Y-%m-%d"),
                "sort": "contractId,asc",
                "page": page_number,
            }

            payload = {"id": 868}
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
        floorsheet_analyis = {}
        for data in floorsheet_data:
            buyer_id = data["buyerMemberId"]
            seller_id = data["sellerMemberId"]
            if floorsheet_analyis.get(buyer_id):
                floorsheet_analyis[buyer_id]["buy"] += data["contractQuantity"]
            else:
                floorsheet_analyis[buyer_id] = {
                    "buy": 0,
                    "sell": 0,
                }

            if floorsheet_analyis.get(seller_id):
                floorsheet_analyis[seller_id]["sell"] += data["contractQuantity"]
            else:
                floorsheet_analyis[seller_id] = {
                    "buy": 0,
                    "sell": 0,
                }
        print(floorsheet_analyis)
