import logging
from time import sleep
import seleniumwire.undetected_chromedriver as uc

logging.getLogger("seleniumwire").setLevel(logging.CRITICAL)


class Payload:
    def __init__(self):
        self._id = None
        options = uc.ChromeOptions()
        options.add_argument("--headless")
        self._driver = uc.Chrome(options=options)
        self._driver.request_interceptor = self.interceptor
        self._driver.scopes = [
            ".*/api/nots/security/.*",
        ]

    def interceptor(self, request):
        self._id = eval(request.body.decode("utf-8"))["id"]

    def get_id(self, security_id):
        self._driver.get(
            "https://newweb.nepalstock.com/company/detail/%s" % security_id
        )
        while not self._id:
            sleep(0.1)
        self._driver.quit()
        return self._id


def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt="%(levelname)s: %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
