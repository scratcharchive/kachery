import setuptools

pkg_name = "kachery"

setuptools.setup(
    name=pkg_name,
    version="0.1.0",
    author="Jeremy Magland",
    author_email="jmagland@flatironinstitute.org",
    description="Content-addressable storage database",
    packages=setuptools.find_packages(),
    scripts=[],
    install_requires=[
        'requests', 'simplejson'
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    )
)
