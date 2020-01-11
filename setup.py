from setuptools import setup, find_packages
setup(
    name="ElectionAnomalyDetection",
    version="0.1",
    packages=find_packages(),
    url="https://github.com/sfsinger19103/results_analysis",
    author="Stephanie Singer",
    author_email="sfsinger@campaignscientific.com", install_requires=['sqlalchemy', 'pandas']

)