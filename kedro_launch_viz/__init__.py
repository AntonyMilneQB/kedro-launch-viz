import logging
from typing import Any, Optional

from kedro_viz.server import run_server
from kedro_viz.launchers.jupyter import _wait_for, _allocate_port, _check_viz_up

logger = logging.getLogger(__name__)


def _get_dbutils() -> Optional[Any]:
    """Get the instance of 'dbutils' or None if the one could not be found."""
    dbutils = globals().get("dbutils")
    if dbutils:
        return dbutils

    try:
        import IPython  # pylint: disable=import-outside-toplevel
    except ImportError:
        return None
    ipython = IPython.get_ipython()
    dbutils = ipython.user_ns.get("dbutils") if ipython else None

    return dbutils


def launch_viz(port: int = None, line=None, local_ns=None) -> None:
    """
    Line magic function to start kedro viz. It calls a kedro viz in a process and displays it in
    the Jupyter notebook environment.

    Args:
        port: TCP port that viz will listen to. Defaults to 4141.
        line: line required by line magic interface.
        local_ns: Local namespace with local variables of the scope where the line magic is invoked.
            For more details, please visit:
            https://ipython.readthedocs.io/en/stable/config/custommagics.html

    """
    port = port or 4141  # Default argument doesn't work in Jupyter line magic.
    port = _allocate_port(start_at=port)
    from kedro.extras.extensions.ipython import default_project_path


    dbutils = _get_dbutils()
    if not dbutils:
        raise Exception("Cannot find dbutils.")

    browser_host_name = get(dbutils, "browserHostName")
    workspace_id = get(dbutils, "workspaceId")
    cluster_id = get(dbutils, "clusterId")

    url = f"https://{browser_host_name}/driver-proxy/o/{workspace_id}/{cluster_id}/{port}/"
    try:
        display_html(f"<a href='{url}'>Launch Kedro-Viz</a>")
    except EnvironmentError:
        print("Launch Kedro-Viz:", url)


    run_server(project_path=default_project_path)

def get(dbutils, thing):
    return getattr(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext(), thing
    )().get()


# https://stackoverflow.com/questions/71474139/how-to-import-displayhtml-in-databricks
def display_html(html: str) -> None:
    """
    Use databricks displayHTML from an external package

    Args:
    - html : html document to display
    """
    import inspect

    for frame in inspect.getouterframes(inspect.currentframe()):
        global_names = set(frame.frame.f_globals)
        # Use multiple functions to reduce risk of mismatch
        if all(v in global_names for v in ["displayHTML", "display", "spark"]):
            return frame.frame.f_globals["displayHTML"](html)
    raise EnvironmentError("Unable to detect displayHTML function")
