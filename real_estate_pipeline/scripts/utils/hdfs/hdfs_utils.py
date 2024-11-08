import os
import sys
import subprocess

abs_path = os.path.dirname(os.path.abspath(__file__)) + "/../../.."
sys.path.append(abs_path)


def run_bash_cmd(command, message="", is_check_error=True):
    """
    :param command: hdfs client cmd to execute
    :type command: str
    :param message: extra info to print out screen
    :type message: str
    :param is_check_error: whether to throw exception when error executing command or not
    :type is_check_error: bool
    """
    print(message)
    print("RUN:", command)
    res = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=is_check_error
    )
    if res.stdout:
        print("OUTPUT:", res.stdout)
    if res.stderr:
        print("DEBUG:", res.stderr)


