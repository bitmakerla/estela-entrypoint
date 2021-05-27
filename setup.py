from setuptools import setup, find_packages

setup(
    name='bitmaker-entrypoint',
    version='0.1',
    description='Scrapy entrypoint for Bitmaker job runner',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
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
    classifiers=[
        'Framework :: Scrapy',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Utilities',
    ],
)
