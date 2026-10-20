import io
import zipfile

import httpx
import pytest
from pydantic import SecretStr

from app.adapters.github import GitHubLogs, LogDownloadError
from app.config import Settings


def zip_bytes():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("job/log.txt", "raw CI failure\n")
    return output.getvalue()


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


def downloader(handler, **overrides):
    return GitHubLogs(Settings(github_token=SecretStr("do-not-leak"), **overrides), httpx.MockTransport(handler))


async def test_authenticated_api_redirect_has_no_token_on_blob(failure):
    raw = zip_bytes()
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            assert request.headers["Authorization"] == "Bearer do-not-leak"
            assert request.headers["Accept"] == "application/vnd.github+json"
            assert "X-GitHub-Api-Version" in request.headers
            assert request.extensions["timeout"]["connect"] == 10
            assert request.extensions["timeout"]["read"] == 30
            return httpx.Response(302, headers={"Location": "https://archive.blob.core.windows.net/logs/signed.zip?sig=hidden"})
        assert "Authorization" not in request.headers
        return httpx.Response(200, stream=Chunks([raw[:10], raw[10:]]))

    assert await downloader(handler).download(failure) == raw
    assert len(requests) == 2


@pytest.mark.parametrize("length", ["999999999", "not-a-number"])
async def test_content_length_limit(failure, length):
    with pytest.raises(LogDownloadError):
        await downloader(lambda _: httpx.Response(200, headers={"Content-Length": length}), max_log_bytes=32).download(failure)


async def test_streamed_limit_without_length(failure):
    with pytest.raises(LogDownloadError):
        await downloader(lambda _: httpx.Response(200, stream=Chunks([b"PK\x03\x04", b"x" * 33])), max_log_bytes=32).download(failure)


@pytest.mark.parametrize("location", ["http://127.0.0.1/logs", "https://evil.example/logs", "https://api.github.com:444/logs"])
async def test_unsafe_redirect_rejected(failure, location):
    with pytest.raises(LogDownloadError):
        await downloader(lambda _: httpx.Response(302, headers={"Location": location})).download(failure)


async def test_payload_selected_endpoint_rejected_before_request(failure):
    def handler(_):
        pytest.fail("Token must not be sent")
    with pytest.raises(LogDownloadError):
        await downloader(handler).download(failure.model_copy(update={"logs_url": "https://evil.example/logs"}))


@pytest.mark.parametrize("status,body", [(403, b"secret"), (404, b"expired logs"), (200, b"not a zip")])
async def test_non_archive_download_fails(failure, status, body):
    with pytest.raises(LogDownloadError):
        await downloader(lambda _: httpx.Response(status, stream=Chunks([body]))).download(failure)


async def test_network_timeout_propagates_for_retry(failure):
    def handler(request):
        raise httpx.ReadTimeout("private signed URL", request=request)
    with pytest.raises(httpx.ReadTimeout):
        await downloader(handler).download(failure)
