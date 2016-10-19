from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='cdmi_client',
      version='0.1',
      description='Interactive command line client for CDMI',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet',
      ],
      keywords='cdmi client qos',
      url='https://github.com/indigo-dc/cdmi_client',
      author='bertl4398',
      author_email='bertl4398@googlemail.com',
      license='BSD',
      packages=['cdmi_cli'],
      install_requires=[
          'requests',
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': ['cdmi-cli=cdmi_cli.command_line:main'],
      },
      include_package_data=True,
      zip_safe=False)

