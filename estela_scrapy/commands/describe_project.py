import json
import subprocess

from pkg_resources import parse_version
from scrapy import __version__
from scrapy.commands import ScrapyCommand

"""
For more information about custom commands:
 * https://github.com/scrapy/scrapy/blob/master/scrapy/commands/__init__.py
"""


class Command(ScrapyCommand):
    requires_project = True
    default_settings = {"LOG_ENABLED": False}

    # here we can add more calls
    IMAGE_INFO_CMD = " && ".join(
        [
            "printf 'Linux packages:\n'",
            "dpkg -l",
            "printf '\nPython packages:\n'",
            "pip freeze",
        ]
    )

    def short_desc(self):
        return "Print JSON-encoded project information and image metadata."

    def add_options(self, parser):
        super(Command, self).add_options(parser)
        args = {
            "action": "store_true",
            "help": "List the installed Linux and Python packages " "within the image.",
            "dest": None,
        }
        if parse_version("2.6.0") > parse_version(__version__):
            parser.add_option("--image", **args)
        else:
            parser.add_argument("--image", **args)

    def run(self, args, opts):
        result = {
            "project_type": "scrapy",
            "spiders": sorted(self.crawler_process.spider_loader.list()),
        }
        if opts.image:
            output = subprocess.check_output(
                ["bash", "-c", self.IMAGE_INFO_CMD],
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            result["image"] = output
        print(json.dumps(result))
