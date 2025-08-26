import sys

from scrapy.utils.project import get_project_settings

try:
    from scrapy.utils.deprecate import update_classpath
except ImportError:
    update_classpath = lambda x: x


def update_deprecated_classpaths(settings):
    # This method updates settings with dicts as values if they're deprecated
    for setting_key in settings.attributes.keys():
        setting_value = settings[setting_key]
        if hasattr(setting_value, "copy_to_dict"):
            setting_value = setting_value.copy_to_dict()
        if not isinstance(setting_value, dict):
            continue
        for path in setting_value.keys():
            updated_path = update_classpath(path)
            if updated_path != path:
                order = settings[setting_key].pop(path)
                settings[setting_key][updated_path] = order


def load_default_settings(settings):
    downloader_middlewares = {
        "estela_scrapy.middlewares.EstelaProxyMiddleware": 702,
        "estela_scrapy.middlewares.StorageDownloaderMiddleware": 1000,
    }
    spider_middlewares = {}
    extensions = {
        "estela_scrapy.extensions.ItemStorageExtension": 999,
        "estela_scrapy.extensions.RedisStatsCollector": 1000,
    }
    settings.get("DOWNLOADER_MIDDLEWARES_BASE").update(downloader_middlewares)
    settings.get("EXTENSIONS_BASE").update(extensions)
    settings.get("SPIDER_MIDDLEWARES_BASE").update(spider_middlewares)
    settings.setdict({"LOG_LEVEL": "INFO"}, priority="cmdline")
    settings.setdict({"LOG_ENABLED": True}, priority="cmdline")


def populate_settings():
    assert "scrapy.conf" not in sys.modules, "Scrapy settings already loaded"
    settings = get_project_settings().copy()
    update_deprecated_classpaths(settings)
    load_default_settings(settings)
    return settings
