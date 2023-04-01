from setuptools import find_packages, setup

setup(
    name="estela-entrypoint",
    version="0.1",
    description="Scrapy entrypoint for Estela job runner",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "Scrapy>=1.0",
        "requests",
        "estela-queue-adapter @ git+https://github.com/bitmakerla/estela-queue-adapter.git"
    ],
    entry_points={
        "console_scripts": [
            "estela-crawl = estela_scrapy.__main__:main",
            "estela-describe-project = estela_scrapy.__main__:describe_project",
        ],
    },
    classifiers=[
        "Framework :: Scrapy",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Utilities",
    ],
)
