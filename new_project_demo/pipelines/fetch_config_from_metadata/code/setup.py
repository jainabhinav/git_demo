from setuptools import setup, find_packages
setup(
    name = 'fetch_config_from_metadata',
    version = '1.0',
    packages = find_packages(include = ('fetch_config_from_metadata*', )) + ['prophecy_config_instances.fetch_config_from_metadata'],
    package_dir = {'prophecy_config_instances.fetch_config_from_metadata' : 'configs/resources/fetch_config_from_metadata'},
    package_data = {'prophecy_config_instances.fetch_config_from_metadata' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.36'],
    entry_points = {
'console_scripts' : [
'main = fetch_config_from_metadata.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
