from setuptools import setup


deps = [
    'pandas',
    'sqlalchemy',
    'tshistory',
]


setup(name='tshistory_alias',
      version='0.1.0',
      author='Pythonian',
      author_email='arnaud.campeas@pythonian.fr, aurelien.campeas@pythonian.fr',
      description='Build calculate timeseries from other timeseries',

      packages=['tshistory_alias'],
      install_requires=deps,
)
