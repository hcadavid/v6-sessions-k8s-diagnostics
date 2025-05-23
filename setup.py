from os import path
from codecs import open
from setuptools import setup, find_packages

# we're using a README.md, if you do not have this in your folder, simply
# replace this with a string.
here = path.abspath(path.dirname(__file__))
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Here you specify the meta-data of your package. The `name` argument is
# needed in some other steps.
setup(
    name="v6-session-basics",
    version="1.0.0",
    description="Basic data extraction and processing jobs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vantage6/v6-session-basics",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "vantage6-algorithm-tools==5.0.0a16",
        "vantage6-common==5.0.0a16",
        "vantage6_client==5.0.0a16", 
        "pandas",
        "psutil",
        "dnspython",                
        ],
)
