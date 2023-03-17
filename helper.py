from hydra import compose, initialize


def load_config():
    """
    config declaration to be used in the pipeline
    :param config:
    :return:
    """
    with initialize(version_base=None, config_path="config"):
        config = compose(config_name="main")
    return config
