from setuptools import setup, find_packages
setup(
    name = 'kafka_streaming',
    version = '1.0',
    packages = find_packages(include = ('kafka_streaming*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.9'],
    entry_points = {
'console_scripts' : [
'main = kafka_streaming.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
