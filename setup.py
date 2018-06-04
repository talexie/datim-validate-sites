import io
import os

from setuptools import setup,find_packages

setup(
	name="validate sites",
	version="1.0",
	description="Datim validate sites scripts",
	author="Alex Tumwesigye, Manish Kumar",
	author_email="atumwesigye@gmail.com, manishk@unc.com",
	url="",
	packages=find_packages(exclude=("tests")),
	python_requires=">3.5"
	install_requires=['pyexcel','json','pandas'],
	license='MIT'
)