from setuptools import setup, find_packages
packages_to_include = find_packages(exclude = ['test.*', 'test', 'test_manual'])
setup(
    name = 'demoteamabhinav_customgemdemosnew',
    version = '0.8',
    packages = packages_to_include,
    description = '',
    install_requires = [
'pyhocon', ],
    data_files = ["resources/extensions.idx"]
)
