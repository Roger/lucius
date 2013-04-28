from setuptools import setup, find_packages

setup(
    name = "lucius",
    version = "0.1",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points = {
        'console_scripts': [
            'lucius = lucius.app:start_app',
        ],
    }
)

