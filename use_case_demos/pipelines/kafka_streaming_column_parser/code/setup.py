from setuptools import setup, find_packages
setup(
    name = 'kafka_streaming_column_parser',
    version = '1.0',
    packages = find_packages(include = ('kafka_streaming_column_parser*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'paramiko', 'prophecy-libs==1.9.28'],
    entry_points = {
'console_scripts' : [
'main = kafka_streaming_column_parser.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
