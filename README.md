# scraping-product-entrypoint

The package implements a wrapper layer to extract job data from environment, prepare the job properly, and execute it using Scrapy.

## Entrypoints
- `bm-crawl`: Process job args and settings to run the job with Scrapy.
- `bm-describe-project`: Print JSON-encoded project information and image metadata.

## Installation
```
python setup.py install
```
