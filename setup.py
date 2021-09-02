from setuptools import setup, find_packages

setup(
    name="electiondata",
    version="2.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    url="https://github.com/ElectionDataAnalysis/electiondata",
    author="Stephanie Frank Singer, et al.",
    author_email="sfsinger@campaignscientific.com",
    install_requires=["sqlalchemy", "pandas"],
)
