from setuptools import setup, find_packages
setup(
    name = 'pipeline_across_fabric',
    version = '1.0',
    packages = find_packages(include = ('pipeline_across_fabric*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'paramiko', 'prophecy-libs==1.9.28'],
    entry_points = {
'console_scripts' : [
'main = pipeline_across_fabric.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)