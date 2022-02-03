import logging, os

def get_logger(
    name: str = "sibytesDatabricksUtils3", 
    logging_level: int = logging.INFO
):
    """Get a python canonical logger

    Setups a console handler for the notebook UI.
    Also sets up a App Insights and/or log analytics
    handler if configured in the environment variables.

    """
    logger = logging.getLogger(name)
    logger.setLevel(logging_level)

    # format_string = "%(name)s: %(funcName)s: line %(lineno)d: %(levelname)s: %(message)s"
    # formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")

    if not any(l.get_name() == "console" for l in logger.handlers):
        console = logging.StreamHandler()
        console.setLevel(logging_level)
        # console.setFormatter(formatter)
        console.set_name("console")
        logger.addHandler(console)
        logger.propagate = False

    return logger