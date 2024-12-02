from setuptools import setup, find_packages

setup(
    name="referral_tracker",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'selenium',
        'sqlalchemy',
        'pandas',
        'python-dotenv'
    ]
)