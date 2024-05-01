from setuptools import setup, find_packages

setup(
    name="cashctrl_ledger",
    version="v0.0.1",
    description="Python package that implements the pyledger.LedgerEngine and integrates with cashctrl",
    url='https://github.com/macxred/cashctrl_ledger',
    author="Lukas Elmiger, Oleksandr Stepanenko",
    python_requires='>3.9',
    install_requires=[
        'pandas',
        'pyledger @ https://github.com/macxred/pyledger/tarball/main',
        'cashctrl_api @ https://github.com/macxred/cashctrl_api/tarball/main'
    ],
    packages=find_packages(exclude=('tests', 'examples'))
)