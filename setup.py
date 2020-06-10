from setuptools import setup, find_packages
setup(
    name="election_anomaly",
    version="0.1",
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    url="https://github.com/sfsinger19103/results_analysis",
    author="Stephanie Singer",
    author_email="sfsinger@campaignscientific.com", 
    install_requires=['sqlalchemy', 'pandas']
)
