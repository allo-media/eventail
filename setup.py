from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='eventail',
    version='1.0.5',
    url='https://github.com/allo-media/async-service',
    author='Allo-Media',
    author_email='dev@allo-media.fr',
    description='A base class and utilities for AM service architecture',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Intended Audience :: Developers",
        "Operating System :: POSIX :: Linux",
        "Topic :: Software Development",
    ],
    license='MIT',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    scripts=["scripts/logger.py", "scripts/monitor.py", "scripts/publish_event.py", "scripts/send_command.py",
             "scripts/inspect_queue.py", "scripts/resurrect.py"],
    install_requires=[
        "pika",
        "cbor",
    ],
    extras_require={
        'asyncio':  ["aiormq", "uvloop"],
        'synchronous': ["kombu"],
        'test': ["tox"]
    },
    python_requires='>=3.7',
)
