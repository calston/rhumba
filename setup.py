from setuptools import setup, find_packages


setup(
    name="rhumba",
    version='0.1.11',
    url='http://github.com/calston/rhumba',
    license='MIT',
    description="An asynchronous job queue written in Twisted",
    author='Colin Alston',
    author_email='colin.alston@gmail.com',
    packages=find_packages() + [
        "twisted.plugins",
    ],
    package_data={
        'twisted.plugins': ['twisted/plugins/rhumba_plugin.py']
    },
    include_package_data=True,
    install_requires=[
        'Twisted',
        'txredis',
        'PyYaml',
        'redis'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: System :: Distributed Computing',
    ],
)
