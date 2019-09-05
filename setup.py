from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='async_service',
    version='0.4',
    url='https://github.com/allo-media/async-service',
    author='Allo-Media',
    author_email='dev@allo-media.fr',
    description='A base class and utilities for AM service architecture',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='Proprietary',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    scripts=[],
    install_requires=[
        'pika',
        'cbor'
    ],
    python_requires='>=3.6',
)
