from setuptools import setup, find_packages
setup(
    name = 'gold_layer_validation',
    version = '1.0',
    packages = find_packages(include = ('gold_layer_validation*', )) + ['prophecy_config_instances.gold_layer_validation'],
    package_dir = {'prophecy_config_instances.gold_layer_validation' : 'configs/resources/gold_layer_validation'},
    package_data = {'prophecy_config_instances.gold_layer_validation' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.36'],
    entry_points = {
'console_scripts' : [
'main = gold_layer_validation.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
