from setuptools import setup

setup(
    name="pyspark_tools",
    description="Tools for testing pyspark functions",
    url="https://github.com/RaoSid01/pyspark_tools",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    author="Siddhant Rao",
    author_email="siddhant.rao@lexisnexisrisk.com",
    license="Proprietary",
    packages=["test_module"],
    package_data={},
    scripts=[],
    zip_safe=False,
)
