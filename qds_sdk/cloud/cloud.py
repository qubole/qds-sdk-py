class Cloud:

    def create_parser(self, argparser):
        return NotImplemented

    def set_cloud_config_from_arguments(self, arguments):
        return NotImplemented

    def set_composition_arguments(self, set_composition_arguments):
        pass

    def get_composition(self, *args, **kwargs):
        pass
