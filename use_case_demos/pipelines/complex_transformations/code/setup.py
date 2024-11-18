from setuptools import setup, find_packages
setup(
    name = 'complex_transformations',
    version = '1.0',
    packages = find_packages(include = ('complex_transformations*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'paramiko', 'prophecy-libs==1.9.24'],
    entry_points = {
'console_scripts' : [
'main = complex_transformations.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
