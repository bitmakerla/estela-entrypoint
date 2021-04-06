from setuptools import setup, find_packages

setup(
    name='scraping-product-entrypoint',
    version='0.1',
    description='Scrapy entrypoint for Bitmaker job runner',
    packages=find_packages(),
    install_requires=[
        'Scrapy>=1.0',
    ],
    entry_points={
        'console_scripts': [
            'bit-crawl = bit_entrypoint.__main__:crawl',
        ],
    },
)
