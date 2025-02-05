from setuptools import setup, find_packages
setup(
    name = 'scd2_demo',
    version = '1.0',
    packages = find_packages(include = ('scd2_demo*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'paramiko', 'prophecy-libs==1.9.33'],
    entry_points = {
'console_scripts' : [
'main = scd2_demo.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
