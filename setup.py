from setuptools import setup, find_packages

setup(
    name="election_data_analysis",
    version="2.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    url="https://github.com/sfsinger19103/results_analysis",
    author="Stephanie Singer",
    author_email="sfsinger@campaignscientific.com",
    install_requires=["sqlalchemy", "pandas"],
)
