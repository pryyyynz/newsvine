from newsvine_api.security import create_access_token, decode_token


def test_access_token_has_access_type() -> None:
    token = create_access_token("42")
    payload = decode_token(token)

    assert payload["sub"] == "42"
    assert payload["type"] == "access"
