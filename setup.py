import pathlib

from setuptools import setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()


def get_version():
    path = HERE / "pype.py"
    py = open(path, "r").readlines()
    version_line = [l.strip() for l in py if l.startswith("__version__")][0]
    version = version_line.split("=")[-1].strip().strip("'\"")
    return version


setup(
    name="pype",
    version=get_version(),
    author="Connor Lane",
    license="MIT",
    url="https://github.com/clane9/pype",
    description="A minimal library for building one-off concurrent data pipelines",
    long_description=README,
    long_description_content_type="text/markdown",
    py_modules=["pype"],
    include_package_data=True,
    python_requires=">=3.7",
)
