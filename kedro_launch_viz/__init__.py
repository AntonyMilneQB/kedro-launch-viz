import logging
import multiprocessing
from functools import partial
from typing import Any, Dict, Optional

from kedro_viz.server import run_server
from kedro_viz.launchers.jupyter import _allocate_port

_VIZ_PROCESSES: Dict[str, int] = {}


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

    if port in _VIZ_PROCESSES and _VIZ_PROCESSES[port].is_alive():
        _VIZ_PROCESSES[port].terminate()

    from kedro.extras.extensions.ipython import default_project_path

    target = partial(run_server, project_path=default_project_path, host="0.0.0.0")

    viz_process = multiprocessing.Process(
        target=target, daemon=True, kwargs={"port": port}
    )

    viz_process.start()
    _VIZ_PROCESSES[port] = viz_process

    env = which_env()
    url = make_url(env, port)

    if env == "db":
        try:
            display_html(f"<a href='{url}'>Launch Kedro-Viz</a>")
        except EnvironmentError:
            print("Launch Kedro-Viz:", url)
    elif env == "jupyter":
        from IPython.display import display, HTML
        display(HTML(f"<a href='{url}'>Launch Kedro-Viz</a>"))
    else:
        print("Viz process launched but can't generate URL")

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


def jupyter_server_proxy():
    return {
        "command": ["kedro", "viz", "--port", "{port}", "--autoreload", "--no-browser"],
        "timeout": 20,
        "launcher_entry": {
            "icon_path": "/Users/antony_milne/Downloads/logo-64x64.svg",
            "title": "Kedro-Viz",
        },
    }

def which_env():
    dbutils = _get_dbutils()
    if dbutils:
        return "db"

    try:
        # Need to check if jupyter server proxy installed
        from notebook import notebookapp
        return "jupyter"
    except:
        return None

def make_url(env, port):

    if env == "db":
        dbutils = _get_dbutils()
        browser_host_name = get(dbutils, "browserHostName")
        workspace_id = get(dbutils, "workspaceId")
        cluster_id = get(dbutils, "clusterId")

        return f"https://{browser_host_name}/driver-proxy/o/{workspace_id}/{cluster_id}/{port}/"

    elif env == "jupyter":
        # N
        # NOTE this won't work in general: https://stackoverflow.com/questions/65475868/get-base-url-of-current-jupyter-server-ipython-is-connected-to
        from notebook import notebookapp
        from urllib.parse import urljoin
        # can we use `/kedro_viz` address rather than port? Should be possible because this route requires jupyter server proxy to work.
        return urljoin(list(notebookapp.list_running_servers())[0]["url"], f"proxy/{port}/")


    return None


