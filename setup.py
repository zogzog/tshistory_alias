from setuptools import setup


setup(name='tshistory_alias',
      version='0.6.0',
      author='Pythonian',
      author_email='arnaud.campeas@pythonian.fr, aurelien.campeas@pythonian.fr',
      description='Computed timeseries on top of the `tshistory` package',

      zip_safe=False,
      packages=['tshistory_alias'],
      package_data={'tshistory_alias': [
          'schema.sql'
      ]},
      install_requires=['tshistory'],
      entry_points={'tshistory.subcommands': [
          'register-priorities=tshistory_alias.cli:register_priorities',
          'register-arithmetic=tshistory_alias.cli:register_arithmetic',
          'register-outliers=tshistory_alias.cli:register_outliers',
          'remove-alias=tshistory_alias.cli:remove_alias',
          'reset-aliases=tshistory_alias.cli:reset_aliases',
          'verify-aliases=tshistory_alias.cli:verify_aliases',
          'audit-aliases=tshistory_alias.cli:audit_aliases',
          'export-aliases=tshistory_alias.cli:export_aliases',
          'migrate-alias-0.4-to-0.5=tshistory_alias.cli:migrate_dot_four_to_dot_five',
          'shell=tshistory_alias.cli:shell'
      ]},
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
