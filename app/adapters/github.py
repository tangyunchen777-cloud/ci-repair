from urllib.parse import urlsplit

import httpx

from app.config import Settings
from app.models import FailureEvent


class LogDownloadError(RuntimeError):
    pass


class GitHubLogs:
    def __init__(self, settings: Settings, transport=None):
        settings.require("github_token")
        self.settings, self.transport = settings, transport

    async def download(self, failure: FailureEvent) -> bytes:
        # The public webhook is unsigned for this classroom. Never send a token
        # to a payload-selected host or accept a run URL for a different repository.
        expected = f"https://api.github.com/repos/{failure.repository_full_name}/actions/runs/{failure.run_id}/logs"
        if str(failure.logs_url) != expected:
            raise LogDownloadError("Unexpected logs endpoint")
        timeout = httpx.Timeout(connect=self.settings.github_connect_timeout,
                                read=self.settings.github_read_timeout, write=10, pool=10)
        headers = {
            "Authorization": f"Bearer {self.settings.github_token.get_secret_value()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": self.settings.github_api_version,
        }
        async with httpx.AsyncClient(timeout=timeout, transport=self.transport, follow_redirects=False,
                                     trust_env=False) as client:
            async with client.stream("GET", expected, headers=headers) as response:
                if response.status_code == 200:
                    return await self._bounded(response)
                if response.status_code not in {301, 302, 303, 307, 308}:
                    raise LogDownloadError("GitHub logs request failed")
                location = response.headers.get("Location", "")
            # Download signed blob URL without Authorization, including on redirects.
            for _ in range(4):
                parsed = urlsplit(location)
                host = parsed.hostname or ""
                if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443)
                        or not (host == "github.com" or host.endswith(".github.com")
                                or host.endswith(".githubusercontent.com") or host.endswith(".blob.core.windows.net"))):
                    raise LogDownloadError("Untrusted log redirect")
                async with client.stream("GET", location) as response:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location", "")
                        continue
                    if response.status_code != 200:
                        raise LogDownloadError("Log archive request failed")
                    return await self._bounded(response)
        raise LogDownloadError("Too many log redirects")

    async def _bounded(self, response: httpx.Response) -> bytes:
        if response.headers.get("Content-Encoding", "identity").lower() != "identity":
            raise LogDownloadError("Encoded archive is not supported")
        length = response.headers.get("Content-Length")
        if length is not None and (not length.isdecimal() or int(length) > self.settings.max_log_bytes):
            raise LogDownloadError("Log archive exceeds size limit")
        chunks, total = [], 0
        async for chunk in response.aiter_raw():
            total += len(chunk)
            if total > self.settings.max_log_bytes:
                raise LogDownloadError("Log archive exceeds size limit")
            chunks.append(chunk)
        raw = b"".join(chunks)
        if not raw.startswith((b"PK\x03\x04", b"PK\x05\x06")):
            raise LogDownloadError("Expected a ZIP archive")
        return raw
