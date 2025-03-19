from setuptools import setup, find_packages
setup(
    name = 'employee_heirarchy',
    version = '1.0',
    packages = find_packages(include = ('employee_heirarchy*', )) + ['prophecy_config_instances.employee_heirarchy'],
    package_dir = {'prophecy_config_instances.employee_heirarchy' : 'configs/resources/employee_heirarchy'},
    package_data = {'prophecy_config_instances.employee_heirarchy' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.36'],
    entry_points = {
'console_scripts' : [
'main = employee_heirarchy.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
