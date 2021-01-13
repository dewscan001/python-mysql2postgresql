from setuptools import setup, find_packages
def readme():
    with open('README.md', 'r', encoding='utf8', ) as f:
        return f.read()

setup(name='mysql2postgresql',
    packages=find_packages(),
    version='0.4.4dev1',
    description='python-mysql2postgresql',
    author='DewBloodmetal',
    author_email='dewscan001@gmail.com',
    install_requires=['mysql-connector', 'psycopg2', 'tqdm'],
    keywords='python mysql2postgresql python-mysql2postgresql',
    long_description=readme(),
    long_description_content_type='text/markdown',
    classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Cython',
          'Topic :: Database'
      ], 
)