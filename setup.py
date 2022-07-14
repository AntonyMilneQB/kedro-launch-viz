from setuptools import setup

setup(
    name="kedro-launch-viz",
    version="0.1",
    packages=["kedro_launch_viz"],
    entry_points={"kedro.line_magic": ["line_magic = kedro_launch_viz:launch_viz"]},
    install_requires=["kedro-viz>=4.7.1"]
)
