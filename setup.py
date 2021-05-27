from setuptools import setup, find_packages

setup(
    name='scraping-product-entrypoint',
    version='0.1',
    description='Scrapy entrypoint for Bitmaker job runner',
    packages=find_packages(),
    install_requires=[
        'Scrapy>=1.0',
        'kafka-python'
    ],
    entry_points={
        'console_scripts': [
            'bm-crawl = bm_scrapy.__main__:main',
            'bm-describe-project = bm_scrapy.__main__:describe_project',
        ],
    },
)
