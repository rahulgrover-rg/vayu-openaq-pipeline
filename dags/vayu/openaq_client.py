import logging 
import random 
import time  

import requests 

log = logging.getLogger(__name__)

class OpenAQError(Exception) : 

    def __init__(self, message, http_status=None):
        super().__init__(message)
        self.http_status = http_status

class TransientError(OpenAQError): 
    pass

class PermanentError(OpenAQError): 
    pass

class PoisonError(OpenAQError): 
    pass

class FatalError(OpenAQError): 
    pass


class OpenAQClient: 
    def __init__(self, api_key, base_url, seconds_between_requests=2.0, pause_when_remaining_below=3, max_retries=5, backoff_base_seconds=2, timeout_seconds=30) : 
        if not api_key: 
            raise FatalError("OpenAQ API key is missing.")

        self.base_url = base_url.rstrip("/")
        self.min_interval = seconds_between_requests 
        self.pause_below = pause_when_remaining_below
        self.max_retries = max_retries
        self.backoff_base = backoff_base_seconds
        self.timeout = timeout_seconds 


        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})

        self._last_request_at = 0.0 
        self.requests_made = 0 


    def _wait_for_turn(self): 
        elapsed = time.monotonic() - self._last_request_at 
        if elapsed < self.min_interval: 
            time.sleep(self.min_interval - elapsed)

    def _respect_rate_headers(self, response): 
        remaining = response.headers.get("x-ratelimit-remaining")
        reset = response.headers.get("x-ratelimit-reset") 

        if remaining is None or reset is None: 
            return 

        try: 
            if int(remaining) < self.pause_below : 
                wait = int(reset) + 1 
                log.info("only %s requests left in this window; pausing %ss", remaining, wait)
                time.sleep(wait) 

        except ValueError:
            pass 

    def _backoff_seconds(self, attempt, response=None): 
        if response is not None: 
            reset = response.headers.get("x-ratelimit-reset", "")
            if reset.isdigit():
                return int(reset) + 1

        return self.backoff_base * (2**attempt) + random.uniform(0,1)


    def get(self, path, params=None): 
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_status = None 
        reason = "unknown" 

        for attempt in range(self.max_retries + 1) : 
            self._wait_for_turn() 
            self._last_request_at = time.monotonic() 
            self.requests_made += 1
            response = None 

            try: 
                response = self.session.get(url, params=params, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as e: 
                last_status = None
                reason = f"network error: ({e.__class__.__name__})" 
            else : 
                last_status = response.status_code
                if last_status == 200: 
                    self._respect_rate_headers(response) 
                    return self._parse(response) 
                if last_status == 404: 
                    raise PermanentError(f"Not Found: {url}", 404)
                if last_status in (401, 403):
                    raise FatalError(f"Authentication failed (HTTP {last_status}); check OPENAQ Api Key", last_status)
                if last_status == 429 or last_status >= 500: 
                    reason = f"HTTP {last_status}"
                else : 
                    raise FatalError(f"Unexpected HTTP {last_status} for {url} : {response.text[:200]}", last_status)


            if attempt == self.max_retries: 
                break 

            wait = self._backoff_seconds(attempt, response if last_status == 429 else None)
            log.warning("%s on %s (attempt %d of %d); retrying in %.1fs", reason, path, attempt + 1, self.max_retries + 1 , wait) 
            time.sleep(wait)

        raise TransientError(f"Gave up after {self.max_retries + 1} attempts: {reason}", last_status)

    @staticmethod 
    def _parse(response): 
        try: 
            payload = response.json() 
        except ValueError as e:
            raise PoisonError(f"Response is not valid JSON: {e}", response.status_code)
        if not isinstance(payload, dict) or not isinstance(payload.get("results"), list): 
            raise PoisonError(f"Response JSON has no 'results' list", response.status_code)
        return payload


    def get_all(self, path, params=None, page_limit=1000): 
        params = dict(params or {})
        params["limit"] = page_limit 
        results = []
        page = 1

        while True: 
            params["page"] = page 
            batch = self.get(path, params=params)["results"]
            results.extend(batch)
            if len(batch) < page_limit:
                return results 
            else :
                page += 1 

