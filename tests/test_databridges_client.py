import requests

from app.shared.databridges import DataBridgesAuth, DataBridgesClient


class FakeResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, *, posts=None, gets=None):
        self.posts = []
        self.gets = []
        self.headers = {}
        self._post_responses = list(posts or [])
        self._get_responses = list(gets or [])

    def post(self, url, **kwargs):
        self.posts.append({"url": url, **kwargs})
        response = self._post_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get(self, url, **kwargs):
        self.gets.append({"url": url, **kwargs})
        response = self._get_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class StubAuth:
    def __init__(self):
        self.scopes = []

    def get_token(self, scopes):
        self.scopes.append(tuple(scopes))
        return "token"


def test_token_exchange_uses_scope_cache():
    session = FakeSession(
        posts=[
            FakeResponse(
                {
                    "access_token": "abc",
                    "expires_in": 3600,
                    "scope": "scope-a scope-b",
                }
            )
        ]
    )
    auth = DataBridgesAuth(
        "key",
        "secret",
        token_url="https://api.wfp.org/token",
        session=session,
    )

    assert auth.get_token(["scope-b", "scope-a"]) == "abc"
    assert auth.get_token(["scope-a", "scope-b"]) == "abc"
    assert len(session.posts) == 1
    assert session.posts[0]["data"]["grant_type"] == "client_credentials"
    assert session.posts[0]["data"]["scope"] == "scope-a scope-b"


def test_pagination_accepts_items_and_items_capitalized():
    auth = StubAuth()
    session = FakeSession(
        gets=[
            FakeResponse({"Items": [{"id": 1}], "totalItems": 2}),
            FakeResponse({"items": [{"id": 2}], "totalItems": 2}),
        ]
    )
    client = DataBridgesClient(
        "key",
        "secret",
        base_url="https://example.test",
        session=session,
        auth_provider=auth,
    )

    rows = client.list_commodities("SSD")

    assert rows == [{"id": 1}, {"id": 2}]
    assert [call["params"]["page"] for call in session.gets] == [1, 2]
    assert auth.scopes == [(client.COMMODITIES_SCOPE,), (client.COMMODITIES_SCOPE,)]


def test_pagination_uses_first_total_when_later_totals_drift():
    auth = StubAuth()
    session = FakeSession(
        gets=[
            FakeResponse({"items": [{"id": 1}], "totalItems": 3}),
            FakeResponse({"items": [{"id": 2}], "totalItems": 2}),
            FakeResponse({"items": [{"id": 3}], "totalItems": 2}),
        ]
    )
    client = DataBridgesClient(
        "key",
        "secret",
        base_url="https://example.test",
        session=session,
        auth_provider=auth,
    )

    rows = client.list_commodities("SSD")

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [call["params"]["page"] for call in session.gets] == [1, 2, 3]


def test_mfi_processed_pagination_stops_on_short_page():
    auth = StubAuth()
    session = FakeSession(
        gets=[
            FakeResponse({"items": [{"row": 1}, {"row": 2}]}),
            FakeResponse({"items": [{"row": 3}]}),
        ]
    )
    client = DataBridgesClient(
        "key",
        "secret",
        base_url="https://example.test",
        session=session,
        auth_provider=auth,
    )

    rows = client.list_mfi_processed_data(123, page_size=2)

    assert rows == [{"row": 1}, {"row": 2}, {"row": 3}]
    assert session.gets[0]["params"]["pageSize"] == 2
    assert [call["params"]["page"] for call in session.gets] == [1, 2]


def test_timeout_and_http_errors_are_actionable():
    timeout_client = DataBridgesClient(
        "key",
        "secret",
        base_url="https://example.test",
        session=FakeSession(gets=[requests.exceptions.Timeout("slow")]),
        auth_provider=StubAuth(),
        timeout=7,
    )
    try:
        timeout_client.list_markets("SSD")
    except TimeoutError as exc:
        assert "after 7s" in str(exc)
    else:
        raise AssertionError("Expected timeout")

    http_client = DataBridgesClient(
        "key",
        "secret",
        base_url="https://example.test",
        session=FakeSession(gets=[FakeResponse({}, status_code=500, text="server failed")]),
        auth_provider=StubAuth(),
    )
    try:
        http_client.list_markets("SSD")
    except RuntimeError as exc:
        assert "HTTP 500" in str(exc)
        assert "server failed" in str(exc)
    else:
        raise AssertionError("Expected HTTP error")
