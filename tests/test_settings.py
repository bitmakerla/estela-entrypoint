from unittest import mock

from scrapy.settings import Settings

from estela_scrapy.settings import (
    load_default_settings,
    populate_settings,
    update_deprecated_classpaths,
)


@mock.patch("estela_scrapy.settings.update_deprecated_classpaths")
@mock.patch("estela_scrapy.settings.load_default_settings")
def test_populate_settings(
    mock_load_default_settings, mock_update_deprecated_classpaths
):
    result = populate_settings()
    assert mock_load_default_settings.called
    assert mock_update_deprecated_classpaths.called
    assert isinstance(result, Settings)


# Test priorities (api, user and defaults) [!] missing


def test_load_default_settings():
    # Test middlewares [!] missing
    test_settings = Settings({"LOG_LEVEL": "DEBUG"})
    load_default_settings(test_settings)
    assert test_settings["LOG_LEVEL"] == "INFO"


def test_update_deprecated_classpaths():
    test_settings = Settings()
    test_settings.get("EXTENSIONS").update(
        {"scrapy.telnet.test": 100}
    )  # deprecated path
    update_deprecated_classpaths(test_settings)
    assert "scrapy.telnet.test" not in test_settings["EXTENSIONS"]
    assert "scrapy.extensions.telnet.test" in test_settings["EXTENSIONS"]
