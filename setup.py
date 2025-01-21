from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="eventail",
    version="2.3.4",
    url="https://github.com/allo-media/eventail",
    author="Allo-Media",
    author_email="dev@allo-media.fr",
    description="A base class and utilities for AM service architecture",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Intended Audience :: Developers",
        "Operating System :: POSIX :: Linux",
        "Topic :: Software Development",
    ],
    license="MIT",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    scripts=[
        "scripts/inspect_queue.py",
        "scripts/logger.py",
        "scripts/monitor.py",
        "scripts/publish_configuration.py",
        "scripts/publish_event.py",
        "scripts/resurrect.py",
        "scripts/send_command.py",
    ],
    install_requires=["pika>=1.2.0", "cbor2", "redis>=4.5.4"],
    extras_require={
        "synchronous": ["kombu"],
        "test": ["tox"],
    },
    python_requires=">=3.8",
)
