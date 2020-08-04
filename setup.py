import setuptools

pkg_name = "kachery"

setuptools.setup(
    name=pkg_name,
    version="0.6.5",
    author="Jeremy Magland",
    author_email="jmagland@flatironinstitute.org",
    description="Content-addressable storage database",
    packages=setuptools.find_packages(),
    scripts=[
        'bin/kachery-store',
        'bin/kachery-load',
        'bin/kachery-ls',
        'bin/kachery-cat',
        'bin/kachery-info',
	    'bin/kachery-load-dir',
        'bin/kachery-set-password'
    ],
    install_requires=[
        'requests', 'simplejson'
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    )
)
