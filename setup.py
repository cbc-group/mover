"""
Minimal setup.py to simplify project setup.
"""
from setuptools import find_packages, setup

setup(
    name="mover",
    version="0.1",
    description="",
    author="Liu, Yen-Ting",
    author_email="ytliu@gate.sinica.edu.tw",
    license="Apache 2.0",
    packages=find_packages(),
    package_data={"": ["data/*"]},
    install_requires=["click", "coloredlogs", "pystray", "watchdog"],
    zip_safe=True,
    extras_require={},
    entry_points={"console_scripts": ["mover=mover.main:cli"]},
)
