import logging
import time
import httpx
from typing import Any, Optional, Tuple
from http import HTTPStatus


class ClienteBase:
    DEFAULT_TIMEOUT = 10
    DEFAULT_MAX_RETRIES = 5
    DEFAULT_BACKOFF_SECONDS = 5
    DEFAULT_MAX_BACKOFF_SECONDS = 60
    TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self, base_url: str, headers: Optional[dict] = None) -> None:
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, headers=headers)
        logging.info(
            f"[cliente_base.py] Initialized ClienteBase with base_url: {base_url}"
        )

    def request(
        self, method: str, path: str, **kwargs: Any
    ) -> Tuple[HTTPStatus, Optional[dict | list]]:
        """
        Faz uma requisição HTTP em até DEFAULT_MAX_RETRIES+1 tentativas.

        Args:
            method (str): HTTP Method.
            path (str): URL path.

        Returns:
            Tuple[HTTPStatus, dict]: status e resposta da requisição HTTP.
        """
        kwargs["timeout"] = kwargs.get("timeout", self.DEFAULT_TIMEOUT)
        response = None

        for attempt in range(self.DEFAULT_MAX_RETRIES + 1):
            try:
                logging.info(
                    f"[cliente_base.py] Attempt {attempt + 1} for {method} "
                    f"{self.base_url}{path} with kwargs: {kwargs}"
                )
                response = self.client.request(method, path, **kwargs)
                response.raise_for_status()
                logging.info(
                    f"[cliente_base.py] Request successful with status "
                    f"{response.status_code}"
                )
                return HTTPStatus(response.status_code), response.json()
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status not in self.TRANSIENT_STATUS_CODES:
                    logging.error(
                        "[cliente_base.py] API failed with non-transient status %s: %s",
                        status,
                        str(e),
                    )
                    raise Exception(
                        "API request failed with a non-transient error"
                    ) from e
                if attempt == self.DEFAULT_MAX_RETRIES:
                    logging.error(
                        "[cliente_base.py] API failed after %s retries. Error: %s",
                        self.DEFAULT_MAX_RETRIES,
                        str(e),
                    )
                    raise Exception(
                        "API failed after the maximum number of attempts!"
                    ) from e

                retry_after = e.response.headers.get("Retry-After", "")
                backoff = min(
                    self.DEFAULT_BACKOFF_SECONDS * 2**attempt,
                    self.DEFAULT_MAX_BACKOFF_SECONDS,
                )
                retry_after_segundos = (
                    int(retry_after) if str(retry_after).isdigit() else 0
                )
                espera = min(
                    max(backoff, retry_after_segundos),
                    self.DEFAULT_MAX_BACKOFF_SECONDS,
                )
                logging.warning(
                    "[cliente_base.py] API returned status %s on attempt %s; "
                    "retrying in %ss",
                    status,
                    attempt + 1,
                    espera,
                )
                time.sleep(espera)
            except httpx.TransportError as e:
                if attempt == self.DEFAULT_MAX_RETRIES:
                    logging.error(
                        "[cliente_base.py] API failed after %s retries. Error: %s",
                        self.DEFAULT_MAX_RETRIES,
                        str(e),
                    )
                    raise Exception(
                        "API failed after the maximum number of attempts!"
                    ) from e

                espera = min(
                    self.DEFAULT_BACKOFF_SECONDS * 2**attempt,
                    self.DEFAULT_MAX_BACKOFF_SECONDS,
                )
                logging.warning(
                    "[cliente_base.py] Transport error on attempt %s; retrying in %ss: %s",
                    attempt + 1,
                    espera,
                    str(e),
                )
                time.sleep(espera)

        return HTTPStatus.INTERNAL_SERVER_ERROR, None
