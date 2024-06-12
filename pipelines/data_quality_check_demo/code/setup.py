from setuptools import setup, find_packages
setup(
    name = 'data_quality_check_demo',
    version = '1.0',
    packages = find_packages(include = ('data_quality_check_demo*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'pydeequ', 'prophecy-libs==1.9.5'],
    entry_points = {
'console_scripts' : [
'main = data_quality_check_demo.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
