from setuptools import setup, find_packages

setup(
    name="vvv-invest",
    version="0.1.0",
    packages=find_packages(include=['collectors*', 'flow_analysis*', 'scripts*', 'config*']),
    install_requires=[
        'pandas>=1.5.0',
        'requests>=2.28.0',
        'psycopg2-binary>=2.9.0',
        'python-dotenv>=0.20.0',
        'pytz>=2022.0',
        'sqlalchemy>=1.4.0',
        'celery>=5.2.0',
        'flask>=2.0.0',
        'pytest>=7.0.0',
        'black>=22.0.0',
        'flake8>=4.0.0'
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'black>=22.0.0',
            'flake8>=4.0.0',
            'mypy>=0.900',
        ],
    },
    author="VVV Invest",
    description="Trading data collection and analysis tools",
    python_requires=">=3.8",
    entry_points={
        'console_scripts': [
            'darkpool-collector=collectors.darkpool.collector:main',
            'news-collector=collectors.news.collector:main',
            'options-collector=collectors.options.collector:main',
        ],
    },
) 