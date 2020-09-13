from setuptools import setup, find_packages

setup(
    name="election_data_analysis",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    package_data={
        "": [
            "jurisdiction_templates/*.txt",
            "munger_templates/*.txt",
            "munger_templates/*.config",
        ]
    },
    url="https://github.com/sfsinger19103/results_analysis",
    author="Stephanie Singer",
    author_email="sfsinger@campaignscientific.com",
    install_requires=["sqlalchemy", "pandas"],
)
