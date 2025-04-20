from setuptools import setup, find_packages

setup(
    name="flow_analysis",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'requests',
        'psycopg2-binary',
        'python-dotenv',
        'pytz'
    ],
    author="VVV Invest",
    description="Tools for analyzing dark pool and options flow data",
    python_requires=">=3.8",
) 