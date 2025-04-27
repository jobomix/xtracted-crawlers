import logging

from colorama import Fore, init

init(autoreset=True)


class ColorFormatter(logging.Formatter):
    # Change this dictionary to suit your coloring needs!
    COLORS = {
        'WARNING': Fore.MAGENTA,
        'ERROR': Fore.RED,
        'DEBUG': Fore.BLUE,
        'INFO': Fore.GREEN,
        'CRITICAL': Fore.RED,
    }

    def format(self, record):  # type: ignore
        color = self.COLORS.get(record.levelname, '')
        if color:
            record.levelname = color + record.levelname + Fore.WHITE
        return logging.Formatter.format(self, record)


class ColorLogger(logging.Logger):
    def __init__(self, name: str) -> None:
        logging.Logger.__init__(self, name, logging.DEBUG)
        root = logging.getLogger('root')
        root.handlers.clear()
        color_formatter = ColorFormatter('%(asctime)s [%(levelname)8s] %(message)s')
        console = logging.StreamHandler()
        console.setFormatter(color_formatter)
        self.addHandler(console)


logging.setLoggerClass(ColorLogger)
