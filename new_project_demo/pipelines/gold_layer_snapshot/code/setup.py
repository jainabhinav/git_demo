from setuptools import setup, find_packages
setup(
    name = 'gold_layer_snapshot',
    version = '1.0',
    packages = find_packages(include = ('gold_layer_snapshot*', )) + ['prophecy_config_instances.gold_layer_snapshot'],
    package_dir = {'prophecy_config_instances.gold_layer_snapshot' : 'configs/resources/gold_layer_snapshot'},
    package_data = {'prophecy_config_instances.gold_layer_snapshot' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.36'],
    entry_points = {
'console_scripts' : [
'main = gold_layer_snapshot.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
