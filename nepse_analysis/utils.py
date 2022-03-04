import logging
from datetime import date

from requests.adapters import HTTPAdapter

logging.getLogger("seleniumwire").setLevel(logging.CRITICAL)


class TimeoutHTTPAdapter(HTTPAdapter):
    DEFAULT_TIMEOUT = 10

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


def get_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt="%(levelname)s: %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


# https://github.com/basic-bgnr/NepseUnofficialApi/blob/master/NepseLib.py
class TokenParser:
    # fmt: off
    data = [
        0x09, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 
        0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 
        0x02, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 
        0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 
        0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 
        0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 
        0x09, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 
        0x06, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 
        0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 
        0x09, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 
        0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 
        0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 
        0x04
    ]

    dummy_data = [
        147, 117, 239, 143, 157, 312, 161, 612, 512, 804, 411, 527, 170,
        511, 421, 667, 764, 621, 301, 106, 133, 793, 411, 511, 312, 423,
        344, 346, 653, 758, 342, 222, 236, 811, 711, 611, 122, 447, 128,
        199, 183, 135, 489, 703, 800, 745, 152, 863, 134, 211, 142, 564,
        375, 793, 212, 153, 138, 153, 648, 611, 151, 649, 318, 143, 117,
        756, 119, 141, 717, 113, 112, 146, 162, 660, 693, 261, 362, 354,
        251, 641, 157, 178, 631, 192, 734, 445, 192, 883, 187, 122, 591,
        731, 852, 384, 565, 596, 451, 772, 624, 691
    ]

    # fmt: on
    @classmethod
    def rdx(self, w2c_p0: int, w2c_p1: int, w2c_p2: int) -> int:
        w2c_i0 = w2c_p1
        w2c_i1 = 100
        w2c_i0 = w2c_i0 // w2c_i1
        w2c_i1 = 10
        w2c_i0 = w2c_i0 % w2c_i1
        w2c_i1 = w2c_p1
        w2c_i2 = 10
        w2c_i1 = w2c_i1 // w2c_i2
        w2c_p0 = w2c_i1
        w2c_i2 = 10
        w2c_i1 = w2c_i1 % w2c_i2
        w2c_i0 += w2c_i1
        w2c_p2 = w2c_i0
        w2c_i1 = w2c_p2
        w2c_i2 = w2c_p1
        w2c_i3 = w2c_p0
        w2c_i4 = 10
        w2c_i3 *= w2c_i4
        w2c_i2 -= w2c_i3
        w2c_i1 += w2c_i2
        w2c_i2 = 2
        w2c_i1 <<= w2c_i2 & 31

        w2c_i1 = self.data[w2c_i1]
        w2c_i0 += w2c_i1
        w2c_i1 = 22
        w2c_i0 += w2c_i1
        return w2c_i0

    @classmethod
    def cdx(self, w2c_p0: int, w2c_p1: int) -> int:
        w2c_i0 = w2c_p1
        w2c_i1 = 10
        w2c_i0 = w2c_i0 // w2c_i1
        w2c_p0 = w2c_i0
        w2c_i1 = 10
        w2c_i0 = w2c_i0 % w2c_i1
        w2c_i1 = w2c_p1
        w2c_i2 = w2c_p0
        w2c_i3 = 10
        w2c_i2 *= w2c_i3
        w2c_i1 -= w2c_i2
        w2c_i0 += w2c_i1
        w2c_i1 = w2c_p1
        w2c_i2 = 100
        w2c_i1 = w2c_i1 // w2c_i2
        w2c_i2 = 10
        w2c_i1 = w2c_i1 % w2c_i2
        w2c_i0 += w2c_i1
        w2c_i1 = 2
        w2c_i0 <<= w2c_i1 & 31

        w2c_i0 = self.data[w2c_i0]
        w2c_i1 = 22
        w2c_i0 += w2c_i1

        return w2c_i0

    @classmethod
    def parse(self, response) -> dict:
        n = self.cdx(response['salt1'], response['salt2'])
        i = self.cdx(response['salt2'], response['salt1'])
        l = self.rdx(response['salt1'], response['salt2'], response['salt4'])
        r = self.rdx(response['salt2'], response['salt1'], response['salt3'])

        access_token = response['accessToken']
        refresh_token = response['refreshToken']

        parsed_access_token = access_token[0:n] + access_token[n + 1 : l] + access_token[l + 1 :]
        parsed_refresh_token = refresh_token[0:i] + refresh_token[i + 1 : r] + refresh_token[r + 1 :]

        return {'accessToken': parsed_access_token, 'refreshToken': parsed_refresh_token}

    @classmethod
    def get_post_id(self, _id: int) -> int:
        id = self.dummy_data[_id]
        return id + _id + 2 * date.today().day
