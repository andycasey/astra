import numpy as np

RAND = np.random.randint(0, 1000)
from socket import gethostname
from astra.utils import expand_path
HOSTNAME = gethostname()

def debugger(*foo):
    with open(expand_path(f"$PBS/{HOSTNAME}-{RAND}.log"), "a") as fp:
        fp.write(" ".join(map(str, foo)) + "\n")
