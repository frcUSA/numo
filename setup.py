from setuptools import setup

setup(
    name='numo',
    version='0.1.1',
    description='Numo financial data processing software',
    url='https://github.com/frcUSA/numo',
    author='Fair Redwood Conglomerate',
    author_email='frcusaca@gmail.com',
    license='Discriminating MIT Lincense, see LICENSE.txt',
    packages=['numo'],
    install_requires=['ray[default]', 'numpy', 'pandas', 'humanize', 'alpaca-py', ],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Non-professional spectators and investors',
        'License :: Discriminating MIT License',
        'Operating System :: POSIX :: Any',
        'Programming Language :: Python :: 3.11',
    ],
)
