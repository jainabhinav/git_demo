from setuptools import setup, find_packages
packages_to_include = find_packages(exclude = ['test.*', 'test', 'test_manual'])
setup(
    name = 'abhinavdemoprophecyio_team_first_project',
    version = '0.0.1',
    packages = packages_to_include,
    description = '',
    install_requires = [],
    data_files = ["resources/extensions.idx"]
)
