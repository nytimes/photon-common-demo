from pathlib import Path
from setuptools import setup

from setup_common import get_requires, setup_base

DESCRIPTION = "Photon Common Demo"

PACKAGE_FULLNAME = "photon-common-demo"
PACKAGE_NAME = PACKAGE_FULLNAME.replace("photon-", "", 1).replace("-", "_")

filep = Path(__file__)
versionp = filep.with_name("version.txt")
PACKAGE_VERSION = versionp.read_text().strip()

requirementsp = filep.with_name("requirements.txt")
REQUIRES = get_requires(requirementsp)

README = filep.with_name("README.md").read_text()

setup(
    name=PACKAGE_FULLNAME,
    url=f"https://github.com/nytimes/{PACKAGE_FULLNAME}",
    version=PACKAGE_VERSION,
    description=DESCRIPTION,
    long_description=README,
    packages=["photon", "photon.common"],
    package_data={"photon.common": ["py.typed"]},
    install_requires=REQUIRES,
    **setup_base,
)
