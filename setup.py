import os
import re
from pathlib import Path
from setuptools import setup, find_packages


def get_version():
    """Find the version of the package from version.py"""
    version_file = Path(__file__).parent / 'hivemind_redis_database' / 'version.py'

    if not version_file.exists():
        raise FileNotFoundError(f"Version file not found: {version_file}")

    version_vars = {}
    with open(version_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('VERSION_') and '=' in line:
                key, value = line.split('=', 1)
                # Extract numeric value, handling quotes and comments
                value = re.sub(r'#.*', '', value.strip())  # Remove comments
                value = re.sub(r'["\']', '', value.strip())  # Remove quotes
                try:
                    version_vars[key.strip()] = int(value)
                except ValueError:
                    version_vars[key.strip()] = 0

    major = version_vars.get('VERSION_MAJOR', 0)
    minor = version_vars.get('VERSION_MINOR', 0)
    build = version_vars.get('VERSION_BUILD', 0)
    alpha = version_vars.get('VERSION_ALPHA', 0)

    version = f"{major}.{minor}.{build}"
    if alpha > 0:
        version += f"a{alpha}"
    return version


def get_requirements():
    """Read requirements file and return list of dependencies."""
    requirements_file = Path(__file__).parent / 'requirements.txt'

    if not requirements_file.exists():
        return []

    with open(requirements_file, 'r', encoding='utf-8') as f:
        requirements = []
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Handle loose requirements for development
                if 'MYCROFT_LOOSE_REQUIREMENTS' in os.environ:
                    print('USING LOOSE REQUIREMENTS!')
                    line = line.replace('==', '>=').replace('~=', '>=')
                requirements.append(line)
        return requirements


# Plugin entry point
PLUGIN_ENTRY_POINT = 'hivemind-redis-db-plugin=hivemind_redis_database:RedisDB'
MIGRATION_ENTRY_POINT = 'hivemind-redis-migrate-cluster=hivemind_redis_database.migration:main'

# Get long description from README if it exists
readme_file = Path(__file__).parent / 'README.md'
long_description = ""
if readme_file.exists():
    with open(readme_file, 'r', encoding='utf-8') as f:
        long_description = f.read()

setup(
    name='hivemind-redis-database',
    version=get_version(),
    packages=find_packages(exclude=['tests', 'tests.*']),
    url='https://github.com/JarbasHiveMind/hivemind-redis-database',
    license='Apache-2.0',
    author='jarbasAi',
    author_email='jarbasai@mailfence.com',
    description='Redis database plugin for HiveMind core with RediSearch support',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='redis hivemind database plugin',
    python_requires='>=3.10',
    install_requires=get_requirements(),
    entry_points={
        'hivemind.database': [PLUGIN_ENTRY_POINT],
        'console_scripts': [MIGRATION_ENTRY_POINT],
    },
    include_package_data=True,
    zip_safe=False,
)
