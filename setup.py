from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='eventail',
    version='1.0.1',
    url='https://github.com/allo-media/async-service',
    author='Allo-Media',
    author_email='dev@allo-media.fr',
    description='A base class and utilities for AM service architecture',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Proprietary',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    scripts=["scripts/logger.py", "scripts/monitor.py", "scripts/publish_event.py", "scripts/send_command.py"],
    install_requires=[
        "pika",
        "cbor",
    ],
    extras_require={
        'asyncio':  ["aiormq", "uvloop"],
        'synchronous': ["kombu"]
    },
    python_requires='>=3.7',
)
