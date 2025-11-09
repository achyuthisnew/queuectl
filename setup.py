from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.0",
    ],
    entry_points={
        "console_scripts": [
            "queuectl=src.cli:cli",
        ],
    },
    author="QueueCTL Team",
    description="Production-grade CLI-based background job queue system",
    python_requires=">=3.10",
)