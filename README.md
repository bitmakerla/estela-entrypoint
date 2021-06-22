# scraping-product-entrypoint

The package implements a wrapper layer to extract job data from environment, prepare the job properly, and execute it using Scrapy.

## Entrypoints
- `bm-crawl`: Process job args and settings to run the job with Scrapy.
- `bm-describe-project`: Print JSON-encoded project information and image metadata.

## Installation
```
python setup.py install 
```

## Environment variables
Job specifications are passed through env variables:

- `JOB_INFO`: Dictionary with this fields:
  - [Required] _key_: Job ID.
  - [Required] _spider_: String spider name.
  - [Optional] _args_: Dictionary with job arguments.
- `KAFKA_ADVERTISED_HOST_NAME`: Default value: _localhost_.
- `KAFKA_ADVERTISED_PORT`: Default value: _9092_.
